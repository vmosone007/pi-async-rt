//! # 单线程运行时
//!
//! - [SingleTaskPool]\: 单线程任务池
//! - [SingleTaskRunner]\: 单线程异步任务执行器
//! - [SingleTaskRuntime]\: 异步单线程任务运行时
//!
//! [SingleTaskPool]: struct.SingleTaskPool.html
//! [SingleTaskRunner]: struct.SingleTaskRunner.html
//! [SingleTaskRuntime]: struct.SingleTaskRuntime.html
//!
//! # Examples
//!
//! ```
//! use pi_async_rt::prelude::{AsyncRuntimeExt, SingleTaskPool, SingleTaskRunner};
//! let pool = SingleTaskPool::default();
//! let rt = SingleTaskRunner::<(), SingleTaskPool<()>>::new(pool).into_local();
//! let _ = rt.block_on(async move {});
//! ```

use std::thread;
use std::any::Any;
use std::sync::Arc;
use std::vec::IntoIter;
use std::future::Future;
use std::cell::UnsafeCell;
use std::task::{Context, Poll, Waker};
use std::io::{Error, ErrorKind, Result};
use std::collections::vec_deque::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_stream::stream;
use crossbeam_channel::Sender;
use crossbeam_queue::SegQueue;
use flume::bounded as async_bounded;
use futures::{
    future::{BoxFuture, FutureExt},
    stream::{BoxStream, Stream, StreamExt},
    task::waker_ref,
};
use parking_lot::{Condvar, Mutex};
use quanta::Clock;

use wrr::IWRRSelector;

use super::single_task_owner::SingleTaskOwner;

use super::{
    PI_ASYNC_THREAD_LOCAL_ID, DEFAULT_MAX_HIGH_PRIORITY_BOUNDED, DEFAULT_HIGH_PRIORITY_BOUNDED, DEFAULT_MAX_LOW_PRIORITY_BOUNDED, alloc_rt_uid, AsyncMapReduce, AsyncPipelineResult, AsyncRuntime,
    AsyncRuntimeExt, AsyncTask, AsyncTaskPollClaim, AsyncTaskPollGuard, AsyncTaskPool, AsyncTaskPoolExt, AsyncTaskTimer, AsyncWait,
    AsyncWaitAny, AsyncWaitAnyCallback, AsyncWaitTimeout, LocalAsyncRuntime, TaskId, YieldNow,
    requeue_runtime_task
};
use crate::rt::{TaskHandle, AsyncTimingTask};

///
/// 内置单消费者、多生产者任务池。
///
/// 首次 `try_pop/try_pop_all` 或执行器 `run/run_once` 绑定真实 owner 线程；
/// 构造、startup、查询和提交均不绑定。绑定前允许移交，绑定后不支持消费者迁移。
/// 公共提交及外部 Waker 进入并发公共队列；只有本池 owner 可以访问本地 FIFO/栈。
/// get_thread_id 返回当前线程已有展示编号，未初始化为 usize::MAX，不代表池所有权。
///
/// 本池增加一个原子字，任务布局不变；授权 O(1)，无锁/自旋/额外 Clone 或任务分配。
/// 队列操作仍可能分配，执行任务的 poll/析构由调用者决定是否阻塞。非纯对象；
/// 提交和出队不幂等，授权与重复查询幂等。不能用授权突破任务、context 的线程约束。
/// 低层异线程消费在访问容器前 panic；执行器返回 PermissionDenied。
///
/// 自定义任务池不继承本实现的授权机制。详细合同：
/// `docs/SINGLE_TASK_POOL_OWNER_DESIGN.md#owner-design`；
/// 红线与合同测试：`tests/single_task_pool_owner*.rs`。
///
pub struct SingleTaskPool<O: Default + 'static> {
    id:             usize,                                                      //绑定的运行时唯一id
    public:         SegQueue<Arc<AsyncTask<SingleTaskPool<O>, O>>>,             //外部任务队列
    internal:       UnsafeCell<VecDeque<Arc<AsyncTask<SingleTaskPool<O>, O>>>>, //内部任务队列
    stack:          UnsafeCell<Vec<Arc<AsyncTask<SingleTaskPool<O>, O>>>>,      //本地任务栈
    selector:       UnsafeCell<IWRRSelector<2>>,                                //任务池选择器
    owner:          SingleTaskOwner,                                            //首次实际消费者，独立于展示编号
    consume_count:  AtomicUsize,                                                //任务消费计数
    produce_count:  AtomicUsize,                                                //任务生产计数
    thread_waker:   Option<Arc<(AtomicBool, Mutex<()>, Condvar)>>,              //绑定线程的唤醒器
}

// 安全边界：共享提交只操作 SegQueue/原子；私有容器的每个入口必须验证真实线程。
// 这不赋予非 Send 任务或 context 跨线程使用/销毁的能力，调用方仍须遵守其原有约束。
unsafe impl<O: Default + 'static> Send for SingleTaskPool<O> {}
unsafe impl<O: Default + 'static> Sync for SingleTaskPool<O> {}

impl<O: Default + 'static> Default for SingleTaskPool<O> {
    fn default() -> Self {
        SingleTaskPool::new([1, 1])
    }
}

impl<O: Default + 'static> AsyncTaskPool<O> for SingleTaskPool<O> {
    type Pool = SingleTaskPool<O>;

    /// 只读当前线程的 packed ID；未初始化返回 MAX，TLS 销毁期仍按原约定 panic。
    /// O(1)，无分配/锁/副作用；不能据此推导本池 owner，生产者查询不会认领权限。
    #[inline]
    fn get_thread_id(&self) -> usize {
        // 安全：只读当前线程私有 TLS，不取得或保存跨线程引用。
        match PI_ASYNC_THREAD_LOCAL_ID.try_with(|thread_id| unsafe { *thread_id.get() }) {
            Err(e) => {
                //不应该执行到这个分支
                panic!(
                    "Get thread id failed, thread: {:?}, reason: {:?}",
                    thread::current(),
                    e
                );
            }
            Ok(id) => id,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        if let Some(len) = self
            .produce_count
            .load(Ordering::Relaxed)
            .checked_sub(self.consume_count.load(Ordering::Relaxed))
        {
            len
        } else {
            0
        }
    }

    #[inline]
    fn push(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        self.public.push(task);
        self.produce_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// owner 提交入本地 FIFO，其余（包括绑定前）回退公共队列；消费 task 的 Arc。
    /// 不幂等，均摊 O(1)，可能队列扩容；授权无锁无等待，不增加 Clone 或用户回调。
    #[inline]
    fn push_local(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        if self.owner.is_current() && task.owner() == self.id {
            //当前是运行时所在线程
            unsafe {{
                (&mut *self.internal.get()).push_back(task);
            }}
            self.produce_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        } else {
            //当前不是运行时所在线程
            self.push(task)
        }
    }

    /// 优先级 >=10 为 owner 本地栈，5..10 为本地 FIFO，<5 为公共队列。
    /// 任意 usize 均沿原分支处理；非 owner 一律公共回退，不承诺外部优先级抢占。
    /// 入队及空间成本同 push_local；不会主动唤醒线程，通知仍由原上层路径负责。
    #[inline]
    fn push_priority(&self,
                     priority: usize,
                     task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        if priority >= DEFAULT_MAX_HIGH_PRIORITY_BOUNDED {
            //最高优先级
            if self.owner.is_current() && task.owner() == self.id {
                //当前是运行时所在线程
                unsafe {
                    let stack = (&mut *self.stack.get());
                    if stack
                        .capacity()
                        .checked_sub(stack.len())
                        .unwrap_or(0) >= 0 {
                        //本地任务栈有空闲容量，则立即将任务加入本地任务栈
                        (&mut *self.stack.get()).push(task);
                    } else {
                        //本地内部任务队列有空闲容量，则立即将任务加入本地内部任务队列
                        (&mut *self.internal.get()).push_back(task);
                    }
                }

                self.produce_count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            } else {
                //当前不是运行时所在线程
                self.push(task)
            }
        } else if priority >= DEFAULT_HIGH_PRIORITY_BOUNDED {
            //高优先级
            self.push_local(task)
        } else {
            //低优先级
            self.push(task)
        }
    }

    /// 保留原唤醒路由（优先级 5），只入队一次；不负责合并 wake 或轮询任务。
    /// 外部调用不会取得 owner 权限。成本、所有权和线程边界同 push_local。
    #[inline]
    fn push_keep(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        self.push_priority(DEFAULT_HIGH_PRIORITY_BOUNDED, task)
    }

    #[inline]
    fn try_pop(&self) -> Option<Arc<AsyncTask<Self::Pool, O>>> {
        // 必须先授权，再触及任一个 UnsafeCell；出队返回后不持有队列借用。
        self.owner.bind_current(self.id).expect("单线程任务池消费线程错误");
        let task = unsafe { (&mut *self
            .stack
            .get())
            .pop()
        };
        if task.is_some() {
            //指定工作者的任务栈有任务，则立即返回任务
            self.consume_count.fetch_add(1, Ordering::Relaxed);
            return task;
        }

        //从指定工作者的任务队列中弹出任务
        let task = try_pop_by_weight(self);
        if task.is_some() {
            self
                .consume_count
                .fetch_add(1, Ordering::Relaxed);
        }
        task
    }

    /// 首次消费可绑定 owner；异线程调用在分配/访问容器前 panic。
    /// 保持历史批量范围：内部 FIFO 后公共快照，不含本地栈、不调整消费计数；
    /// 因此不能将此 API 当作关闭清空或用随后的 len 推断实际队列空闲。
    /// O(n) 时间/额外空间、可能分配、非幂等、不执行 poll/用户回调，不持借用跨 await。
    #[inline]
    fn try_pop_all(&self) -> IntoIter<Arc<AsyncTask<Self::Pool, O>>> {
        self.owner.bind_current(self.id).expect("单线程任务池批量消费线程错误");
        let mut all = Vec::with_capacity(self.len());

        let internal = unsafe { (&mut *self.internal.get()) };
        for _ in 0..internal.len() {
            if let Some(task) = internal.pop_front() {
                all.push(task);
            }
        }

        let public_len = self.public.len();
        for _ in 0..public_len {
            if let Some(task) = self.public.pop() {
                all.push(task);
            }
        }

        all.into_iter()
    }

    #[inline]
    fn get_thread_waker(&self) -> Option<&Arc<(AtomicBool, Mutex<()>, Condvar)>> {
        self.thread_waker.as_ref()
    }
}

// 仅由已授权的 try_pop 调用；选择器与本地容器都属于同一个真实 owner。
// 保持既有加权选择和空队列回退顺序，不持借用跨任务 poll 或用户回调。
fn try_pop_by_weight<O: Default + 'static>(pool: &SingleTaskPool<O>)
    -> Option<Arc<AsyncTask<SingleTaskPool<O>, O>>> {
    unsafe {
        //根据权重选择从指定的任务队列弹出任务
        match (&mut *pool.selector.get()).select() {
            0 => {
                //弹出外部任务
                let task = try_pop_external(pool);
                if task.is_some() {
                    task
                } else {
                    //当前没有外部任务，则尝试弹出内部任务
                    try_pop_internal(pool)
                }
            },
            _ => {
                //弹出内部任务
                let task = try_pop_internal(pool);
                if task.is_some() {
                    task
                } else {
                    //当前没有内部任务，则尝试弹出外部任务
                    try_pop_external(pool)
                }
            },
        }
    }
}

// 尝试弹出内部任务队列的任务
#[inline]
fn try_pop_internal<O: Default + 'static>(pool: &SingleTaskPool<O>)
    -> Option<Arc<AsyncTask<SingleTaskPool<O>, O>>> {
    unsafe { (&mut *pool.internal.get()).pop_front() }
}

// 尝试弹出外部任务队列的任务
#[inline]
fn try_pop_external<O: Default + 'static>(pool: &SingleTaskPool<O>)
                                          -> Option<Arc<AsyncTask<SingleTaskPool<O>, O>>> {
    pool.public.pop()
}

impl<O: Default + 'static> AsyncTaskPoolExt<O> for SingleTaskPool<O> {
    fn set_thread_waker(&mut self, thread_waker: Arc<(AtomicBool, Mutex<()>, Condvar)>) {
        self.thread_waker = Some(thread_waker);
    }
}

impl<O: Default + 'static> SingleTaskPool<O> {
    /// 构建尚未绑定消费者的池；weights[0]/[1] 分别用于公共/内部队列。
    /// 沿用 IWRRSelector 约束：元素须 <255，且驱动时至少一个非零。
    /// 255 由依赖构造函数 panic；[0,0] 不可驱动（旧选择器会空转），本轮不改此合同。
    /// 初始化 O(1)，分配本地栈和唤醒器；不设置线程身份，无用户回调，不消费任务。
    pub fn new(weights: [u8; 2]) -> Self {
        let id = alloc_rt_uid();
        let public = SegQueue::new();
        let internal = UnsafeCell::new(VecDeque::new());
        let stack = UnsafeCell::new(Vec::with_capacity(1));
        let selector = UnsafeCell::new(IWRRSelector::new(weights));
        let consume_count = AtomicUsize::new(0);
        let produce_count = AtomicUsize::new(0);

        SingleTaskPool {
            id,
            public,
            internal,
            stack,
            selector,
            owner: SingleTaskOwner::new(),
            consume_count,
            produce_count,
            thread_waker: Some(Arc::new((
                AtomicBool::new(false),
                Mutex::new(()),
                Condvar::new(),
            ))),
        }
    }
}

///
/// 异步单线程任务运行时
///
pub struct SingleTaskRuntime<
    O: Default + 'static = (),
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O> = SingleTaskPool<O>,
>(
    Arc<(
        usize,                                  //运行时唯一id
        Arc<P>,                                 //异步任务池
        Sender<(usize, AsyncTimingTask<P, O>)>, //休眠的异步任务生产者
        AsyncTaskTimer<P, O>,                   //本地定时器
        AtomicUsize,                            //定时器任务生产计数
        AtomicUsize,                            //定时器任务消费计数
    )>,
);

unsafe impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Send
    for SingleTaskRuntime<O, P>
{
}
unsafe impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Sync
    for SingleTaskRuntime<O, P>
{
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Clone
    for SingleTaskRuntime<O, P>
{
    fn clone(&self) -> Self {
        SingleTaskRuntime(self.0.clone())
    }
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>> AsyncRuntime<O>
    for SingleTaskRuntime<O, P>
{
    type Pool = P;

    /// 共享运行时内部任务池
    fn shared_pool(&self) -> Arc<Self::Pool> {
        (self.0).1.clone()
    }

    /// 获取当前异步运行时的唯一id
    fn get_id(&self) -> usize {
        (self.0).0
    }

    /// 获取当前异步运行时待处理任务数量
    fn wait_len(&self) -> usize {
        (self.0)
            .4
            .load(Ordering::Relaxed)
            .checked_sub((self.0).5.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// 获取当前异步运行时任务数量
    fn len(&self) -> usize {
        (self.0).1.len()
    }

    /// 分配异步任务的唯一id
    fn alloc<R: 'static>(&self) -> TaskId {
        TaskId(UnsafeCell::new((TaskHandle::<R>::default().into_raw() as u128) << 64 | self.get_id() as u128 & 0xffffffffffffffff))
    }

    /// 派发一个指定的异步任务到异步运行时
    fn spawn<F>(&self, future: F) -> Result<TaskId>
    where
        F: Future<Output = O> + Send + 'static,
    {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_by_id(task_id.clone(), future) {
            return Err(e);
        }

        Ok(task_id)
    }

    /// 派发一个异步任务到本地异步运行时，如果本地没有本异步运行时，则会派发到当前运行时中
    fn spawn_local<F>(&self, future: F) -> Result<TaskId>
        where
            F: Future<Output = O> + Send + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_local_by_id(task_id.clone(), future) {
            return Err(e);
        }

        Ok(task_id)
    }

    /// 派发一个指定优先级的异步任务到异步运行时
    fn spawn_priority<F>(&self, priority: usize, future: F) -> Result<TaskId>
        where
            F: Future<Output = O> + Send + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_priority_by_id(task_id.clone(), priority, future) {
            return Err(e);
        }

        Ok(task_id)
    }

    /// 派发一个异步任务到异步运行时，并立即让出任务的当前运行
    fn spawn_yield<F>(&self, future: F) -> Result<TaskId>
        where
            F: Future<Output = O> + Send + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_yield_by_id(task_id.clone(), future) {
            return Err(e);
        }

        Ok(task_id)
    }

    /// 派发一个在指定时间后执行的异步任务到异步运行时，时间单位ms
    fn spawn_timing<F>(&self, future: F, time: usize) -> Result<TaskId>
    where
        F: Future<Output = O> + Send + 'static,
    {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_timing_by_id(task_id.clone(), future, time) {
            return Err(e);
        }

        Ok(task_id)
    }

    /// 派发一个指定任务唯一id的异步任务到异步运行时
    fn spawn_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where
            F: Future<Output = O> + Send + 'static {
        if let Err(e) = (self.0).1.push(Arc::new(AsyncTask::new(
            task_id,
            (self.0).1.clone(),
            DEFAULT_MAX_LOW_PRIORITY_BOUNDED,
            Some(future.boxed()),
        ))) {
            return Err(Error::new(ErrorKind::Other, e));
        }

        Ok(())
    }

    /// 派发一个指定任务唯一id的异步任务到本地异步运行时，如果本地没有本异步运行时，则会派发到当前运行时中
    fn spawn_local_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where
            F: Future<Output = O> + Send + 'static {
        (self.0).1.push_local(Arc::new(AsyncTask::new(
            task_id,
            (self.0).1.clone(),
            DEFAULT_HIGH_PRIORITY_BOUNDED,
            Some(future.boxed()))))
    }

    /// 派发一个指定任务唯一id和任务优先级的异步任务到异步运行时
    fn spawn_priority_by_id<F>(&self,
                               task_id: TaskId,
                               priority: usize,
                               future: F) -> Result<()>
        where
            F: Future<Output = O> + Send + 'static {
        (self.0).1.push_priority(priority, Arc::new(AsyncTask::new(
            task_id,
            (self.0).1.clone(),
            priority,
            Some(future.boxed()))))
    }

    /// 派发一个指定任务唯一id的异步任务到异步运行时，并立即让出任务的当前运行
    #[inline]
    fn spawn_yield_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where
            F: Future<Output = O> + Send + 'static {
        self.spawn_priority_by_id(task_id,
                                  DEFAULT_HIGH_PRIORITY_BOUNDED,
                                  future)
    }

    /// 派发一个指定任务唯一id和在指定时间后执行的异步任务到异步运行时，时间单位ms
    fn spawn_timing_by_id<F>(&self,
                             task_id: TaskId,
                             future: F,
                             time: usize) -> Result<()>
        where
            F: Future<Output = O> + Send + 'static {
        let rt = self.clone();
        self.spawn_by_id(task_id, async move {
            (rt.0).3.set_timer(
                AsyncTimingTask::WaitRun(Arc::new(AsyncTask::new(
                    rt.alloc::<F::Output>(),
                    (rt.0).1.clone(),
                    DEFAULT_HIGH_PRIORITY_BOUNDED,
                    Some(future.boxed()),
                ))),
                time,
            );

            (rt.0).4.fetch_add(1, Ordering::Relaxed);
            Default::default()
        })
    }

    /// 挂起指定唯一id的异步任务
    fn pending<Output: 'static>(&self, task_id: &TaskId, waker: Waker) -> Poll<Output> {
        task_id.set_waker::<Output>(waker);
        Poll::Pending
    }

    /// 唤醒指定唯一id的异步任务
    fn wakeup<Output: 'static>(&self, task_id: &TaskId) {
        task_id.wakeup::<Output>();
    }

    /// 挂起当前异步运行时的当前任务，并在指定的其它运行时上派发一个指定的异步任务，等待其它运行时上的异步任务完成后，唤醒当前运行时的当前任务，并返回其它运行时上的异步任务的值
    fn wait<V: Send + 'static>(&self) -> AsyncWait<V> {
        AsyncWait(self.wait_any(2))
    }

    /// 挂起当前异步运行时的当前任务，并在多个其它运行时上执行多个其它任务，其中任意一个任务完成，则唤醒当前运行时的当前任务，并返回这个已完成任务的值，而其它未完成的任务的值将被忽略
    fn wait_any<V: Send + 'static>(&self, capacity: usize) -> AsyncWaitAny<V> {
        let (producor, consumer) = async_bounded(capacity);

        AsyncWaitAny {
            capacity,
            producor,
            consumer,
        }
    }

    /// 挂起当前异步运行时的当前任务，并在多个其它运行时上执行多个其它任务，任务返回后需要通过用户指定的检查回调进行检查，其中任意一个任务检查通过，则唤醒当前运行时的当前任务，并返回这个已完成任务的值，而其它未完成或未检查通过的任务的值将被忽略，如果所有任务都未检查通过，则强制唤醒当前运行时的当前任务
    fn wait_any_callback<V: Send + 'static>(&self, capacity: usize) -> AsyncWaitAnyCallback<V> {
        let (producor, consumer) = async_bounded(capacity);

        AsyncWaitAnyCallback {
            capacity,
            producor,
            consumer,
        }
    }

    /// 构建用于派发多个异步任务到指定运行时的映射归并，需要指定映射归并的容量
    fn map_reduce<V: Send + 'static>(&self, capacity: usize) -> AsyncMapReduce<V> {
        let (producor, consumer) = async_bounded(capacity);

        AsyncMapReduce {
            count: 0,
            capacity,
            producor,
            consumer,
        }
    }

    /// 挂起当前异步运行时的当前任务，等待指定的时间后唤醒当前任务
    fn timeout(&self, timeout: usize) -> BoxFuture<'static, ()> {
        let rt = self.clone();
        let producor = (self.0).2.clone();

        AsyncWaitTimeout::new(rt, producor, timeout).boxed()
    }

    /// 立即让出当前任务的执行
    fn yield_now(&self) -> BoxFuture<'static, ()> {
        async move {
            YieldNow(false).await;
        }.boxed()
    }

    /// 生成一个异步管道，输入指定流，输入流的每个值通过过滤器生成输出流的值
    fn pipeline<S, SO, F, FO>(&self, input: S, mut filter: F) -> BoxStream<'static, FO>
    where
        S: Stream<Item = SO> + Send + 'static,
        SO: Send + 'static,
        F: FnMut(SO) -> AsyncPipelineResult<FO> + Send + 'static,
        FO: Send + 'static,
    {
        let output = stream! {
            for await value in input {
                match filter(value) {
                    AsyncPipelineResult::Disconnect => {
                        //立即中止管道
                        break;
                    },
                    AsyncPipelineResult::Filtered(result) => {
                        yield result;
                    },
                }
            }
        };

        output.boxed()
    }

    /// 关闭异步运行时，返回请求关闭是否成功
    fn close(&self) -> bool {
        false
    }
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>> AsyncRuntimeExt<O>
    for SingleTaskRuntime<O, P>
{
    fn spawn_with_context<F, C>(&self, task_id: TaskId, future: F, context: C) -> Result<()>
    where
        F: Future<Output = O> + Send + 'static,
        C: 'static,
    {
        if let Err(e) = (self.0).1.push(Arc::new(AsyncTask::with_context(
            task_id,
            (self.0).1.clone(),
            DEFAULT_MAX_LOW_PRIORITY_BOUNDED,
            Some(future.boxed()),
            context,
        ))) {
            return Err(Error::new(ErrorKind::Other, e));
        }

        Ok(())
    }

    fn spawn_timing_with_context<F, C>(
        &self,
        task_id: TaskId,
        future: F,
        context: C,
        time: usize,
    ) -> Result<()>
    where
        F: Future<Output = O> + Send + 'static,
        C: Send + 'static,
    {
        let rt = self.clone();
        self.spawn_by_id(task_id, async move {
            (rt.0).3.set_timer(
                AsyncTimingTask::WaitRun(Arc::new(AsyncTask::with_context(
                    rt.alloc::<F::Output>(),
                    (rt.0).1.clone(),
                    DEFAULT_MAX_HIGH_PRIORITY_BOUNDED,
                    Some(future.boxed()),
                    context,
                ))),
                time,
            );

            (rt.0).4.fetch_add(1, Ordering::Relaxed);
            Default::default()
        })
    }

    /// 同步驱动本运行时直到 future 完成；不是异步等待原语，会占用调用线程。
    /// 内置池须由原 owner 或尚未绑定时的首次消费者调用；异线程返回 PermissionDenied。
    /// 授权在入队前完成，失败不遗留捕获本函数栈地址的任务；future 在调用线程释放。
    /// 自定义池仍服从自身线程约束。执行时间取决于任务，不能从其它任务的 panic
    /// 推导出取消/恢复保证；本轮只增加前置授权，不改既有执行及异常传播流程。
    fn block_on<F>(&self, future: F) -> Result<F::Output>
    where
        F: Future + Send + 'static,
        <F as Future>::Output: Default + Send + 'static,
    {
        self.bind_builtin_owner()?;
        let runner = SingleTaskRunner {
            is_running: AtomicBool::new(true),
            runtime: self.clone(),
            clock: Clock::new(),
        };
        let mut result: Option<<F as Future>::Output> = None;
        let result_raw = (&mut result) as *mut Option<<F as Future>::Output> as usize;

        self.spawn(async move {
            //在指定运行时中执行，并返回结果
            let r = future.await;
            unsafe {
                *(result_raw as *mut Option<<F as Future>::Output>) = Some(r);
            }

            Default::default()
        });

        loop {
            //执行异步任务
            while runner.run()? > 0 {}

            //尝试获取异步任务的执行结果
            if let Some(result) = result.take() {
                //异步任务已完成，则立即返回执行结果
                return Ok(result);
            }
        }
    }
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>
    SingleTaskRuntime<O, P>
{
    /// 仅识别本模块内置池，避免给第三方 AsyncTaskPool 增加隐式必需 hook。
    /// Any 是安全类型核验；不分配、不克隆、不转换裸指针。自定义 P 返回成功但不
    /// 为其授予任何线程安全能力。必须在 timer 访问及 block_on 入队之前调用。
    /// O(1)，同步非阻塞；成功后不保留 guard 或内部引用，副作用见 owner::bind_current。
    #[inline]
    fn bind_builtin_owner(&self) -> Result<()> {
        if let Some(pool) = ((self.0).1.as_ref() as &dyn Any).downcast_ref::<SingleTaskPool<O>>() {
            pool.owner.bind_current(pool.id)?;
        }
        Ok(())
    }

    /// 获取当前单线程异步运行时的本地异步运行时
    pub fn to_local_runtime(&self) -> LocalAsyncRuntime<O> {
        LocalAsyncRuntime {
            inner: self.as_raw(),
            get_id_func: SingleTaskRuntime::<O, P>::get_id_raw,
            spawn_func: SingleTaskRuntime::<O, P>::spawn_raw,
            spawn_local_func: SingleTaskRuntime::<O, P>::spawn_local_raw,
            spawn_timing_func: SingleTaskRuntime::<O, P>::spawn_timing_raw,
            timeout_func: SingleTaskRuntime::<O, P>::timeout_raw,
        }
    }

    // 获取当前单线程异步运行时的指针
    #[inline]
    pub(crate) fn as_raw(&self) -> *const () {
        Arc::into_raw(self.0.clone()) as *const ()
    }

    // 获取指定指针的单线程异步运行时
    #[inline]
    pub(crate) fn from_raw(raw: *const ()) -> Self {
        let inner = unsafe {
            Arc::from_raw(
                raw as *const (
                    usize,
                    Arc<P>,
                    Sender<(usize, AsyncTimingTask<P, O>)>,
                    AsyncTaskTimer<P, O>,
                    AtomicUsize,
                    AtomicUsize,
                ),
            )
        };
        SingleTaskRuntime(inner)
    }

    // 获取当前异步运行时的唯一id
    pub(crate) fn get_id_raw(raw: *const ()) -> usize {
        let rt = SingleTaskRuntime::<O, P>::from_raw(raw);
        let id = rt.get_id();
        Arc::into_raw(rt.0); //避免提前释放
        id
    }

    // 派发一个指定的异步任务到异步运行时
    pub(crate) fn spawn_raw(raw: *const (), future: BoxFuture<'static, O>) -> Result<()> {
        let rt = SingleTaskRuntime::<O, P>::from_raw(raw);
        let result = rt.spawn_by_id(rt.alloc::<O>(), future);
        Arc::into_raw(rt.0); //避免提前释放
        result
    }

    // 派发一个指定的异步任务到本地异步运行时
    pub(crate) fn spawn_local_raw(raw: *const (), future: BoxFuture<'static, O>) -> Result<()> {
        let rt = SingleTaskRuntime::<O, P>::from_raw(raw);
        let result = rt.spawn_local_by_id(rt.alloc::<O>(), future);
        Arc::into_raw(rt.0); //避免提前释放
        result
    }

    // 定时派发一个指定的异步任务到异步运行时
    pub(crate) fn spawn_timing_raw(
        raw: *const (),
        future: BoxFuture<'static, O>,
        timeout: usize,
    ) -> Result<()> {
        let rt = SingleTaskRuntime::<O, P>::from_raw(raw);
        let result = rt.spawn_timing_by_id(rt.alloc::<O>(), future, timeout);
        Arc::into_raw(rt.0); //避免提前释放
        result
    }

    // 挂起当前异步运行时的当前任务，等待指定的时间后唤醒当前任务
    pub(crate) fn timeout_raw(raw: *const (), timeout: usize) -> BoxFuture<'static, ()> {
        let rt = SingleTaskRuntime::<O, P>::from_raw(raw);
        let boxed = rt.timeout(timeout);
        Arc::into_raw(rt.0); //避免提前释放
        boxed
    }
}

///
/// 单线程异步任务执行器
///
pub struct SingleTaskRunner<
    O: Default + 'static,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O> = SingleTaskPool<O>,
> {
    is_running: AtomicBool,                 //是否开始运行
    runtime:    SingleTaskRuntime<O, P>,    //异步单线程任务运行时
    clock:      Clock,                      //执行器的时钟
}

unsafe impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Send
    for SingleTaskRunner<O, P>
{
}
unsafe impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Sync
    for SingleTaskRunner<O, P>
{
}

impl<O: Default + 'static> Default for SingleTaskRunner<O> {
    fn default() -> Self {
        SingleTaskRunner::new(SingleTaskPool::default())
    }
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>
    SingleTaskRunner<O, P>
{
    /// 用指定池构建运行时与私有 timer，不启动、不消费、不绑定构造线程。
    /// 内置池的 runtime ID 取自该池；自定义 P 保留 get_thread_id() >>32 的历史协议。
    /// 在 A 构造后可交给 B 首次驱动；首次驱动后 owner 固定，不支持跨线程迁移。
    /// 初始化 O(1)，分配 Arc/timer/通道；非纯、不幂等，不执行用户 Future。
    ///
    /// ```
    /// use pi_async_rt::rt::{AsyncRuntime, single_thread::SingleTaskRunner};
    /// let runner = SingleTaskRunner::<()>::default();
    /// let runtime = runner.startup().unwrap();
    /// runtime.spawn(async {}).unwrap();
    /// std::thread::spawn(move || runner.run_once().unwrap()).join().unwrap();
    /// ```
    pub fn new(pool: P) -> Self {
        let rt_uid = if let Some(builtin) = (&pool as &dyn Any).downcast_ref::<SingleTaskPool<O>>() {
            builtin.id
        } else {
            pool.get_thread_id() >> 32
        };
        let pool = Arc::new(pool);

        //构建本地定时器和定时异步任务生产者
        let timer = AsyncTaskTimer::new();
        let producor = timer.producor.clone();
        let timer_producor_count = AtomicUsize::new(0);
        let timer_consume_count = AtomicUsize::new(0);

        //构建单线程任务运行时
        let runtime = SingleTaskRuntime(Arc::new((rt_uid,
                                                  pool,
                                                  producor,
                                                  timer,
                                                  timer_producor_count,
                                                  timer_consume_count)));

        SingleTaskRunner {
            is_running: AtomicBool::new(false),
            runtime,
            clock: Clock::new(),
        }
    }

    /// 获取单线程异步任务执行器的线程唤醒器
    pub fn get_thread_waker(&self) -> Option<Arc<(AtomicBool, Mutex<()>, Condvar)>> {
        (self.runtime.0).1.get_thread_waker().cloned()
    }

    /// 启动单线程异步任务执行器
    /// 仅推进启动标志并返回共享句柄；首次 Some，重复 None，不认领消费者线程。
    /// O(1)，成功时克隆一次既有 Arc；不驱动 timer、队列或用户 Future。
    pub fn startup(&self) -> Option<SingleTaskRuntime<O, P>> {
        if cfg!(target_arch = "aarch64") {
            match self
                .is_running
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(false) => {
                    //未启动，则启动，并返回单线程异步运行时
                    Some(self.runtime.clone())
                }
                _ => {
                    //已启动，则忽略
                    None
                }
            }
        } else {
            match self.is_running.compare_exchange(
                false,
                true,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(false) => {
                    //未启动，则启动，并返回单线程异步运行时
                    Some(self.runtime.clone())
                }
                _ => {
                    //已启动，则忽略
                    None
                }
            }
        }
    }

    /// 推进到期 timer 及一次普通出队，返回原有剩余可运行任务计数（不是等待任务总数）。
    /// 未 startup 返回 Other；内置池首次调用绑定 owner，异线程返回 PermissionDenied。
    /// 校验先于 timer/队列，拒绝时无消费副作用。授权 O(1)，总成本取决于到期项及
    /// Future::poll/析构；不增加锁、忙等或异步挂起，也不保证用户任务不会阻塞。
    pub fn run_once(&self) -> Result<usize> {
        if !self.is_running.load(Ordering::Relaxed) {
            //未启动，则返回错误原因
            return Err(Error::new(
                ErrorKind::Other,
                "Single thread runtime not running",
            ));
        }

        self.runtime.bind_builtin_owner()?;
        //设置新的定时任务，并唤醒已过期的定时任务
        let mut pop_len = 0;
        (self.runtime.0)
            .4
            .fetch_add((self.runtime.0).3.consume(),
                       Ordering::Relaxed);
        loop {
            let current_time = (self.runtime.0).3.is_require_pop();
            if let Some(current_time) = current_time {
                //当前有到期的定时异步任务，则只处理到期的一个定时异步任务
                let timed_out = (self.runtime.0).3.pop(current_time);
                if let Some((handle, timing_task)) = timed_out {
                    match timing_task {
                        AsyncTimingTask::Pended(expired) => {
                            //唤醒休眠的异步任务，并立即执行
                            self.runtime.wakeup::<O>(&expired);
                            if let Some(task) = (self.runtime.0).1.try_pop() {
                                run_task(task);
                            }
                        }
                        AsyncTimingTask::WaitRun(expired) => {
                            //立即执行到期的定时异步任务，并立即执行
                            (self.runtime.0).1.push_priority(handle, expired);
                            if let Some(task) = (self.runtime.0).1.try_pop() {
                                run_task(task);
                            }
                        }
                        AsyncTimingTask::TimeoutWake(waiter) => {
                            //唤醒等待timeout到期的任务
                            waiter.fire();
                            if let Some(task) = (self.runtime.0).1.try_pop() {
                                run_task(task);
                            }
                        }
                    }
                    pop_len += 1;
                }
            } else {
                //当前没有到期的定时异步任务，则退出本次定时异步任务处理
                break;
            }
        }
        (self.runtime.0)
            .5
            .fetch_add(pop_len,
                       Ordering::Relaxed);

        //继续执行当前任务池中的一个异步任务
        match (self.runtime.0).1.try_pop() {
            None => {
                //当前没有异步任务，则立即返回
                return Ok(0);
            }
            Some(task) => {
                run_task(task);
            }
        }

        Ok((self.runtime.0).1.len())
    }

    /// 沿原时间片和队列流程驱动，空闲返回原剩余计数，不等待所有 Pending Future。
    /// 启动/owner/错误合同同 run_once；授权在外层循环前完成，不改变 timer 或任务次序。
    /// 同线程可再次调用；不持容器借用跨用户 poll，但不为用户递归驱动或 panic 提供
    /// 额外取消保证。持续就绪负载下可能不返回，非纯且非幂等，无新增等待/锁。
    pub fn run(&self) -> Result<usize> {
        if !self.is_running.load(Ordering::Relaxed) {
            //未启动，则返回错误原因
            return Err(Error::new(
                ErrorKind::Other,
                "Single thread runtime not running",
            ));
        }

        self.runtime.bind_builtin_owner()?;
        loop {
            //设置新的定时任务，并唤醒已过期的定时任务
            let mut pop_len = 0;
            let mut start_run_millis = self.clock.recent(); //重置开运行时长
            (self.runtime.0)
                .4
                .fetch_add((self.runtime.0).3.consume(),
                           Ordering::Relaxed);
            loop {
                let current_time = (self.runtime.0).3.is_require_pop();
                if let Some(current_time) = current_time {
                    //当前有到期的定时异步任务，则只处理到期的一个定时异步任务
                    let timed_out = (self.runtime.0).3.pop(current_time);
                    if let Some((handle, timing_task)) = timed_out {
                        match timing_task {
                            AsyncTimingTask::Pended(expired) => {
                                //唤醒休眠的异步任务，并立即执行
                                self.runtime.wakeup::<O>(&expired);
                                if let Some(task) = (self.runtime.0).1.try_pop() {
                                    run_task(task);
                                }
                            }
                            AsyncTimingTask::WaitRun(expired) => {
                                //立即执行到期的定时异步任务，并立即执行
                                (self.runtime.0).1.push_priority(handle, expired);
                                if let Some(task) = (self.runtime.0).1.try_pop() {
                                    run_task(task);
                                }
                            }
                            AsyncTimingTask::TimeoutWake(waiter) => {
                                //唤醒等待timeout到期的任务
                                waiter.fire();
                                if let Some(task) = (self.runtime.0).1.try_pop() {
                                    run_task(task);
                                }
                            }
                        }
                        pop_len += 1;
                    }
                } else {
                    //当前没有到期的定时异步任务，则退出本次定时异步任务处理
                    break;
                }
            }
            (self.runtime.0)
                .5
                .fetch_add(pop_len,
                           Ordering::Relaxed);

            //继续执行当前任务池中的一个异步任务
            while self
                .clock
                .recent()
                .duration_since(start_run_millis)
                .as_millis() < 1 {
                match (self.runtime.0).1.try_pop() {
                    None => {
                        //当前没有异步任务，则立即返回
                        return Ok((self.runtime.0).1.len());
                    }
                    Some(task) => {
                        run_task(task);
                    }
                }
            }
        }
    }

    /// 转换为本地异步单线程任务运行时
    pub fn into_local(self) -> SingleTaskRuntime<O, P> {
        self.runtime
    }
}

/// 对单线程执行器弹出的一个任务执行轮询。
///
/// 托管生命周期与多线程驱动共用：认领一个已调度义务，丢弃重复/已完成队列项，
/// 轮询期间的唤醒延期处理，并在 `Pending` 时先恢复 Future、再准确重排一次。尽管
/// 本执行器只有一个消费者，合并仍可防止有限的集中唤醒和已完成任务的迟到唤醒
/// 队列项扩大队列。
///
/// 公开手工驱动任务保持兼容手工模式，并使用原取出/轮询/恢复路径。每次托管轮询的成本为
/// O(1)，另加用户轮询和可选的一次队列入队。状态处理自身不新增分配；可选入队继续服从
/// 任务池既有的容量和扩容行为。Future 互斥锁不跨用户代码、任务池访问或工作线程通知，
/// 也不引入新的阻塞或锁顺序。函数消费一个物理队列 `Arc`、返回 `()`，且可能轮询/
/// 析构、重排一次并通知执行器，因此非纯且非幂等。Future/context 仍在既有执行器线程
/// 析构，V8 任务池也保持这一边界。
#[inline]
fn run_task<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>(
    task: Arc<AsyncTask<P, O>>,
) {
    match task.try_begin_runtime_poll() {
        AsyncTaskPollClaim::Discard => return,
        AsyncTaskPollClaim::Legacy => {
            let waker = waker_ref(&task);
            let mut context = Context::from_waker(&*waker);
            if let Some(mut future) = task.get_inner() {
                if let Poll::Pending = future.as_mut().poll(&mut context) {
                    task.set_inner(Some(future));
                }
            }
            return;
        },
        AsyncTaskPollClaim::Managed => (),
    }

    // 守卫必须先于局部 Future 声明：栈展开时先析构 Future，再由守卫把析构期间
    // 发出的唤醒吸收到终态。
    let guard = AsyncTaskPollGuard::new(&task);
    let waker = waker_ref(&task);
    let mut context = Context::from_waker(&*waker);
    let mut future = match task.take_inner_for_runtime_poll() {
        Some(future) => future,
        None => {
            guard.finish_ready();
            return;
        },
    };

    match future.as_mut().poll(&mut context) {
        Poll::Pending => {
            task.restore_inner_after_runtime_poll(future);
            if guard.finish_pending() {
                requeue_runtime_task(task.get_pool(), &task);
            }
        },
        Poll::Ready(_) => guard.finish_ready(),
    }
}
