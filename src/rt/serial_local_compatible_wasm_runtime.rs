//! 兼容wasm的本地单线程异步运行时
//!
//! - [LocalTaskRunner]\: 本地异步任务执行器
//! - [LocalTaskRuntime]\: 本地异步任务运行时
//!
//! [LocalTaskRunner]: struct.LocalTaskRunner.html
//! [LocalTaskRuntime]: struct.LocalTaskRuntime.html
//!
//! # Examples
//!
//! ```
//! use pi_async::rt::{AsyncRuntime, AsyncRuntimeExt, serial_local_compatible_wasm_thread::{LocalTaskRunner, LocalTaskRuntime}};
//! let rt = LocalTaskRunner::<()>::new().into_local();
//! let _ = rt.block_on(async move {});
//! ```

use std::thread;
use std::vec::IntoIter;
use std::future::Future;
use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::task::{Context, Poll, Waker};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::io::{Error, Result, ErrorKind};

use parking_lot::{Condvar, Mutex};
use async_stream::stream;
use crossbeam_queue::SegQueue;
use flume::bounded as async_bounded;
use futures::{
    future::{FutureExt, LocalBoxFuture},
    stream::{LocalBoxStream, Stream, StreamExt},
    task::{waker_ref, ArcWake},
};

use crate::{rt::{DEFAULT_MAX_HIGH_PRIORITY_BOUNDED, TaskId, AsyncPipelineResult, YieldNow, alloc_rt_uid,
                 serial::{AsyncRuntime, AsyncRuntimeExt, AsyncTaskPool, AsyncTaskPoolExt, AsyncTask, AsyncMapReduce, AsyncWait, AsyncWaitAny, AsyncWaitAnyCallback}}};

///
/// 兼容wasm的本地单线程异步任务池
///
pub struct LocalTaskPool<O: Default + 'static> {
    external:   SegQueue<Arc<AsyncTask<Self, O>>>,                //外部任务队列
    inner:      UnsafeCell<VecDeque<Arc<AsyncTask<Self, O>>>>,    //内部任务队列
}

unsafe impl<O: Default + 'static> Sync for LocalTaskPool<O> {}

impl<O: Default + 'static> Default for LocalTaskPool<O> {
    fn default() -> Self {
        LocalTaskPool {
            external: SegQueue::default(),
            inner: UnsafeCell::new(VecDeque::default()),
        }
    }
}

impl<O: Default + 'static> AsyncTaskPool<O> for LocalTaskPool<O> {
    type Pool = LocalTaskPool<O>;

    #[inline]
    fn get_thread_id(&self) -> usize {
        0
    }

    #[inline]
    fn len(&self) -> usize {
        unsafe {
            self.external.len() + (&*self.inner.get()).len()
        }
    }

    #[inline]
    fn push(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        self.external.push(task);
        Ok(())
    }

    #[inline]
    fn push_local(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        unsafe {
            (&mut *self.inner.get()).push_back(task);
            Ok(())
        }
    }

    #[inline]
    fn push_priority(&self,
                     _priority: usize,
                     task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        self.push_local(task)
    }

    #[inline]
    fn push_keep(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        self.push_local(task)
    }

    #[inline]
    fn try_pop(&self) -> Option<Arc<AsyncTask<Self::Pool, O>>> {
        unsafe {
            (&mut *self.inner.get()).pop_front()
        }
    }

    #[inline]
    fn try_pop_all(&self) -> IntoIter<Arc<AsyncTask<Self::Pool, O>>> {
        let mut all = Vec::with_capacity(self.len());

        let internal = unsafe { (&mut *self.inner.get()) };
        for _ in 0..internal.len() {
            if let Some(task) = internal.pop_front() {
                all.push(task);
            }
        }

        let public_len = self.external.len();
        for _ in 0..public_len {
            if let Some(task) = self.external.pop() {
                all.push(task);
            }
        }

        all.into_iter()
    }

    #[inline]
    fn get_thread_waker(&self) -> Option<&Arc<(AtomicBool, Mutex<()>, Condvar)>> {
        None
    }
}

impl<O: Default + 'static> AsyncTaskPoolExt<O> for LocalTaskPool<O> {
    fn worker_len(&self) -> usize {
        1
    }
}

impl<O: Default + 'static> LocalTaskPool<O> {
    /// 构建一个兼容wasm的本地单线程异步任务池
    pub fn new() -> Self {
        Self::default()
    }

    /// 获取当前内部任务的数量
    #[inline]
    pub(crate) fn internal_len(&self) -> usize {
        unsafe {
            (&*self.inner.get()).len()
        }
    }

    /// 将要唤醒指定的任务
    #[inline]
    pub(crate) fn will_wakeup(&self, task: Arc<AsyncTask<Self, O>>) {
        unsafe {
            (&mut *self.inner.get()).push_back(task);
        }
    }

    /// 从外部任务池中弹出一个任务
    #[inline]
    pub(crate) fn pop(&self) -> Option<Arc<AsyncTask<Self, O>>> {
        self.external.pop()
    }
}

///
/// 兼容wasm的本地单线程异步运行时
///
pub struct LocalTaskRuntime<O: Default + 'static = ()>(Arc<InnerLocalTaskRuntime<O>>);

struct InnerLocalTaskRuntime<O: Default + 'static = ()> {
    uid:        usize,                  //运行时唯一id
    running:    Arc<AtomicBool>,        //运行状态
    pool:       Arc<LocalTaskPool<O>>,  //任务池
}

impl<O: Default + 'static> Clone for LocalTaskRuntime<O> {
    fn clone(&self) -> Self {
        LocalTaskRuntime(self.0.clone())
    }
}

impl<O: Default + 'static> AsyncRuntime<O> for LocalTaskRuntime<O> {
    type Pool = LocalTaskPool<O>;

    fn shared_pool(&self) -> Arc<Self::Pool> {
        self.0.pool.clone()
    }

    fn get_id(&self) -> usize {
        self.0.uid
    }

    fn wait_len(&self) -> usize {
        self.0.pool.len()
    }

    fn len(&self) -> usize {
        self.wait_len()
    }

    fn alloc<R: 'static>(&self) -> TaskId {
        TaskId(UnsafeCell::new(0))
    }

    fn spawn<F>(&self, future: F) -> Result<TaskId>
        where
            F: Future<Output = O> + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_by_id(task_id.clone(), future) {
            return Err(e);
        }

        Ok(task_id)
    }

    fn spawn_local<F>(&self, future: F) -> Result<TaskId>
        where
            F: Future<Output = O> + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_local_by_id(task_id.clone(), future) {
            return Err(e);
        }

        Ok(task_id)
    }

    fn spawn_priority<F>(&self, priority: usize, future: F) -> Result<TaskId>
        where
            F: Future<Output = O> + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_priority_by_id(task_id.clone(), priority, future) {
            return Err(e);
        }

        Ok(task_id)
    }

    fn spawn_yield<F>(&self, future: F) -> Result<TaskId>
        where
            F: Future<Output = O> + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_yield_by_id(task_id.clone(), future) {
            return Err(e);
        }

        Ok(task_id)
    }

    fn spawn_timing<F>(&self, future: F, time: usize) -> Result<TaskId>
        where
            F: Future<Output = O> + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_timing_by_id(task_id.clone(), future, time) {
            return Err(e);
        }

        Ok(task_id)
    }

    fn spawn_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where
            F: Future<Output = O> + 'static {
        self.spawn_local_by_id(task_id, future)
    }

    fn spawn_local_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where
            F: Future<Output = O> + 'static {
        if let Err(e) = self.0.pool.push_local(Arc::new(AsyncTask::new(
            task_id,
            self.0.pool.clone(),
            DEFAULT_MAX_HIGH_PRIORITY_BOUNDED,
            Some(future.boxed_local()),
        ))) {
            return Err(Error::new(ErrorKind::Other, e));
        }

        Ok(())
    }

    fn spawn_priority_by_id<F>(&self,
                               task_id: TaskId,
                               _priority: usize,
                               future: F) -> Result<()>
        where
            F: Future<Output = O> + 'static {
        if let Err(e) = self.0.pool.push_priority(DEFAULT_MAX_HIGH_PRIORITY_BOUNDED,
                                                  Arc::new(AsyncTask::new(
                                                      task_id,
                                                      self.0.pool.clone(),
                                                      DEFAULT_MAX_HIGH_PRIORITY_BOUNDED,
                                                      Some(future.boxed_local()),
                                                  ))) {
            return Err(Error::new(ErrorKind::Other, e));
        }

        Ok(())
    }

    /// 派发一个指定任务唯一id的异步任务到异步运行时，并立即让出任务的当前运行
    #[inline]
    fn spawn_yield_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where
            F: Future<Output = O> + 'static {
        self.spawn_priority_by_id(task_id,
                                  DEFAULT_MAX_HIGH_PRIORITY_BOUNDED,
                                  future)
    }

    /// 派发一个指定任务唯一id和在指定时间后执行的异步任务到异步运行时，时间单位ms
    fn spawn_timing_by_id<F>(&self,
                             _task_id: TaskId,
                             _future: F,
                             _time: usize) -> Result<()>
        where
            F: Future<Output = O> + 'static {
        Err(Error::new(ErrorKind::Other, "unimplemented"))
    }

    /// 挂起指定唯一id的异步任务
    fn pending<Output: 'static>(&self, task_id: &TaskId, waker: Waker) -> Poll<Output> {
        unimplemented!()
    }

    /// 唤醒指定唯一id的异步任务
    fn wakeup<Output: 'static>(&self, task_id: &TaskId) {
        unimplemented!()
    }

    /// 挂起当前异步运行时的当前任务，并在指定的其它运行时上派发一个指定的异步任务，等待其它运行时上的异步任务完成后，唤醒当前运行时的当前任务，并返回其它运行时上的异步任务的值
    fn wait<V: 'static>(&self) -> AsyncWait<V> {
        AsyncWait::new(self.wait_any(2))
    }

    /// 挂起当前异步运行时的当前任务，并在多个其它运行时上执行多个其它任务，其中任意一个任务完成，则唤醒当前运行时的当前任务，并返回这个已完成任务的值，而其它未完成的任务的值将被忽略
    fn wait_any<V: 'static>(&self, capacity: usize) -> AsyncWaitAny<V> {
        let (producor, consumer) = async_bounded(capacity);

        AsyncWaitAny::new(capacity, producor, consumer)
    }

    /// 挂起当前异步运行时的当前任务，并在多个其它运行时上执行多个其它任务，任务返回后需要通过用户指定的检查回调进行检查，其中任意一个任务检查通过，则唤醒当前运行时的当前任务，并返回这个已完成任务的值，而其它未完成或未检查通过的任务的值将被忽略，如果所有任务都未检查通过，则强制唤醒当前运行时的当前任务
    fn wait_any_callback<V: 'static>(&self, capacity: usize) -> AsyncWaitAnyCallback<V> {
        let (producor, consumer) = async_bounded(capacity);

        AsyncWaitAnyCallback::new(capacity, producor, consumer)
    }

    /// 构建用于派发多个异步任务到指定运行时的映射归并，需要指定映射归并的容量
    fn map_reduce<V: 'static>(&self, capacity: usize) -> AsyncMapReduce<V> {
        let (producor, consumer) = async_bounded(capacity);

        AsyncMapReduce::new(0, capacity, producor, consumer)
    }

    /// 挂起当前异步运行时的当前任务，等待指定的时间后唤醒当前任务
    fn timeout(&self, _timeout: usize) -> LocalBoxFuture<'static, ()> {
        unimplemented!()
    }

    /// 立即让出当前任务的执行
    fn yield_now(&self) -> LocalBoxFuture<'static, ()> {
        async move {
            YieldNow(false).await;
        }.boxed_local()
    }

    /// 生成一个异步管道，输入指定流，输入流的每个值通过过滤器生成输出流的值
    fn pipeline<S, SO, F, FO>(&self, input: S, mut filter: F) -> LocalBoxStream<'static, FO>
        where
            S: Stream<Item = SO> + 'static,
            SO: 'static,
            F: FnMut(SO) -> AsyncPipelineResult<FO> + 'static,
            FO: 'static,
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

        output.boxed_local()
    }

    /// 关闭异步运行时，返回请求关闭是否成功
    fn close(&self) -> bool {
        if cfg!(target_arch = "aarch64") {
            if let Ok(true) =
                self
                    .0
                    .running
                    .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            {
                //设置运行状态成功
                true
            } else {
                false
            }
        } else {
            if let Ok(true) =
                self
                    .0
                    .running
                    .compare_exchange_weak(true, false, Ordering::SeqCst, Ordering::SeqCst)
            {
                //设置运行状态成功
                true
            } else {
                false
            }
        }
    }
}

impl<O: Default + 'static> AsyncRuntimeExt<O> for LocalTaskRuntime<O> {
    fn spawn_with_context<F, C>(&self,
                                _task_id: TaskId,
                                _future: F,
                                _context: C) -> Result<()>
        where F: Future<Output = O> + 'static,
              C: 'static {
        Err(Error::new(ErrorKind::Other, "unimplemented"))
    }

    /// 派发一个在指定时间后执行的异步任务到异步运行时，并指定异步任务的初始化上下文，时间单位ms
    fn spawn_timing_with_context<F, C>(&self,
                                       task_id: TaskId,
                                       future: F,
                                       context: C,
                                       time: usize) -> Result<()>
        where F: Future<Output = O> + 'static,
              C: 'static {
        Err(Error::new(ErrorKind::Other, "unimplemented"))
    }

    /// 立即创建一个指定任务池的异步运行时，并执行指定的异步任务，阻塞当前线程，等待异步任务完成后返回
    fn block_on<F>(&self, future: F) -> Result<F::Output>
        where F: Future + 'static,
              <F as Future>::Output: Default + 'static {
        let runner = LocalTaskRunner(self.clone());
        let mut result: Option<<F as Future>::Output> = None;
        let result_ptr = (&mut result) as *mut Option<<F as Future>::Output>;

        self.spawn_local(async move {
            //在指定运行时中执行，并返回结果
            let r = future.await;
            unsafe {
                *result_ptr = Some(r);
            }

            Default::default()
        });

        loop {
            //执行异步任务
            while self.internal_len() > 0 {
                runner.run_once();
            }

            //尝试获取异步任务的执行结果
            if let Some(result) = result.take() {
                //异步任务已完成，则立即返回执行结果
                return Ok(result);
            }
        }
    }
}

impl<O: Default + 'static> LocalTaskRuntime<O> {
    /// 判断当前本地异步任务运行时是否正在运行
    #[inline]
    pub fn is_running(&self) -> bool {
        self
            .0
            .running
            .load(Ordering::Relaxed)
    }

    /// 获取当前内部任务的数量
    #[inline]
    pub fn internal_len(&self) -> usize {
       self.0.pool.internal_len()
    }

    /// 将要唤醒指定的任务
    #[inline]
    pub(crate) fn will_wakeup(&self, task: Arc<AsyncTask<<Self as AsyncRuntime<O>>::Pool, O>>) {
        self.0.pool.will_wakeup(task);
    }

    /// 线程安全的发送一个异步任务到异步运行时
    pub fn send<F>(&self, future: F)
        where
            F: Future<Output = O> + 'static,
    {
        let task_id = self.alloc::<F::Output>();
        self.0.pool.push(Arc::new(AsyncTask::new(
            task_id,
            self.0.pool.clone(),
            DEFAULT_MAX_HIGH_PRIORITY_BOUNDED,
            Some(future.boxed_local()),
        )));
    }
}

///
/// 本地异步任务执行器
///
pub struct LocalTaskRunner<O: Default + 'static = ()>(LocalTaskRuntime<O>);

unsafe impl<O: Default + 'static> Send for LocalTaskRunner<O> {}
impl<O: Default + 'static> !Sync for LocalTaskRunner<O> {}

impl<O: Default + 'static> LocalTaskRunner<O> {
    /// 构建本地异步任务执行器
    pub fn new() -> Self {
        let inner = InnerLocalTaskRuntime {
            uid: alloc_rt_uid(),
            running: Arc::new(AtomicBool::new(false)),
            pool: Arc::new(LocalTaskPool::new()),
        };

        LocalTaskRunner(LocalTaskRuntime(Arc::new(inner)))
    }

    /// 获取当前本地异步任务执行器的运行时
    pub fn get_runtime(&self) -> LocalTaskRuntime<O> {
        self.0.clone()
    }

    /// 启动工作者异步任务执行器
    pub fn startup(self,
                   thread_name: &str,
                   thread_stack_size: usize) -> LocalTaskRuntime<O> {
        let rt = self.get_runtime();
        let rt_copy = rt.clone();
        let _ = thread::Builder::new()
            .name(thread_name.to_string())
            .stack_size(thread_stack_size)
            .spawn(move || {
                rt_copy
                    .0
                    .running
                    .store(true, Ordering::Relaxed);

                while rt_copy.is_running() {
                    self.poll();
                    self.run_once();
                }
            });

        rt
    }

    /// 将外部任务队列中的任务移动到内部任务队列
    #[inline]
    pub fn poll(&self) {
        while let Some(task) = (self.0).0.pool.pop() {
            (self.0)
                .0
                .pool
                .push_local(task);
        }
    }

    // 运行一次本地异步任务执行器
    #[inline]
    pub fn run_once(&self) {
        unsafe {
            if let Some(task) = (self.0).0.pool.try_pop() {
                let waker = waker_ref(&task);
                let mut context = Context::from_waker(&*waker);
                if let Some(mut future) = task.get_inner() {
                    if let Poll::Pending = future.as_mut().poll(&mut context) {
                        //当前未准备好，则恢复本地异步任务，以保证本地异步任务不被提前释放
                        task.set_inner(Some(future));
                    }
                }
            }
        }
    }

    /// 转换为本地异步任务运行时
    pub fn into_local(self) -> LocalTaskRuntime<O> {
        self.0
    }
}

#[test]
fn test_local_compatible_wasm_runtime_block_on() {
    use crate::tests::test_lib::AtomicCounter;

    let rt = LocalTaskRunner::<()>::new().into_local();

    let counter = Arc::new(AtomicCounter::new(10000000));
    for _ in 0..10000000 {
        let counter_copy = counter.clone();
        let _ = rt.block_on(async move { counter_copy.fetch_add(1) });
    }
}








