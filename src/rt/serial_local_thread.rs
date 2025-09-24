//! 本地单线程异步运行时
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
//! use pi_async::rt::{AsyncRuntime, AsyncRuntimeExt, serial_local_thread::{LocalTaskRunner, LocalTaskRuntime}};
//! let rt = LocalTaskRunner::<()>::new().into_local();
//! let _ = rt.block_on(async move {});
//! ```

use std::thread;
use std::task::Context;
use std::future::Future;
use std::cell::UnsafeCell;
use std::task::Poll::Pending;
use std::collections::VecDeque;
use std::io::Result as IOResult;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::sync::atomic::AtomicUsize;
use std::time::{Duration, Instant};

use async_stream::stream;
use crossbeam_queue::SegQueue;
use flume::bounded as async_bounded;
use futures::{
    future::{FutureExt, LocalBoxFuture},
    stream::{LocalBoxStream, Stream, StreamExt},
    task::{waker_ref, ArcWake},
};
use polling::{Events, Poller};

use crate::{
    rt::{
        alloc_rt_uid,
        serial::{AsyncMapReduce, AsyncWait, AsyncWaitAny, AsyncWaitAnyCallback},
        AsyncPipelineResult, YieldNow
    },
};

// 本地异步任务
pub(crate) struct LocalTask<O: Default + 'static = ()> {
    inner: UnsafeCell<Option<LocalBoxFuture<'static, O>>>, //内部本地异步任务
    runtime: LocalTaskRuntime<O>,                          //本地异步任务运行时
}

unsafe impl<O: Default + 'static> Send for LocalTask<O> {}
unsafe impl<O: Default + 'static> Sync for LocalTask<O> {}

impl<O: Default + 'static> ArcWake for LocalTask<O> {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.runtime.will_wakeup(arc_self.clone());
    }
}

impl<O: Default + 'static> LocalTask<O> {
    // 获取内部本地异步任务
    pub fn get_inner(&self) -> Option<LocalBoxFuture<'static, O>> {
        unsafe { (&mut *self.inner.get()).take() }
    }

    // 设置内部本地异步任务
    pub fn set_inner(&self, inner: Option<LocalBoxFuture<'static, O>>) {
        unsafe {
            *self.inner.get() = inner;
        }
    }
}

///
/// 本地异步任务运行时
///
pub struct LocalTaskRuntime<O: Default + 'static = ()>(
    Arc<(
        usize,                                      //运行时唯一id
        Arc<AtomicBool>,                            //运行状态
        SegQueue<Arc<LocalTask<O>>>,                //外部任务队列
        UnsafeCell<VecDeque<Arc<LocalTask<O>>>>,    //内部任务队列
        Option<AtomicBool>,                         //合并唤醒标志
        Option<Arc<Poller>>,                        //用于阻塞等待和跨线程唤醒
    )>,
);

unsafe impl<O: Default + 'static> Send for LocalTaskRuntime<O> {}
impl<O: Default + 'static> !Sync for LocalTaskRuntime<O> {}

impl<O: Default + 'static> Clone for LocalTaskRuntime<O> {
    fn clone(&self) -> Self {
        LocalTaskRuntime(self.0.clone())
    }
}

impl<O: Default + 'static> LocalTaskRuntime<O> {
    /// 判断当前本地异步任务运行时是否正在运行
    #[inline]
    pub fn is_running(&self) -> bool {
        self.0.1.load(Ordering::Relaxed)
    }

    /// 获取当前异步运行时的唯一id
    pub fn get_id(&self) -> usize {
        self.0.0
    }

    /// 获取当前异步运行时任务数量
    pub fn len(&self) -> usize {
        unsafe {
            (self.0).2.len() + self.internal_len()
        }
    }

    /// 获取当前内部任务的数量
    #[inline]
    pub(crate) fn internal_len(&self) -> usize {
        unsafe {
            (&*self.0.3.get()).len()
        }
    }

    /// 派发一个指定的异步任务到异步运行时
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = O> + 'static,
    {
        unsafe {
            (&mut *(self.0).3.get()).push_back(Arc::new(LocalTask {
                inner: UnsafeCell::new(Some(future.boxed_local())),
                runtime: self.clone(),
            }));
        }
    }

    /// 将要唤醒指定的任务
    #[inline]
    pub(crate) fn will_wakeup(&self, task: Arc<LocalTask<O>>) {
        self.0.2.push(task);
    }

    /// 线程安全的发送一个异步任务到异步运行时
    pub fn send<F>(&self, future: F)
    where
        F: Future<Output = O> + 'static,
    {
        self.0.2.push(Arc::new(LocalTask {
            inner: UnsafeCell::new(Some(future.boxed_local())),
            runtime: self.clone(),
        }));

        if let Some(sleeping) = &self.0.4 {
            if sleeping.compare_exchange(
                false,
                true,
                Ordering::AcqRel,
                Ordering::Relaxed).is_ok()
            {
                //需要唤醒
                let _ = self
                    .0
                    .5
                    .as_ref()
                    .unwrap()
                    .notify();
            }
        }
    }

    /// 将外部任务队列中的任务移动到内部任务队列
    #[inline]
    pub fn poll(&self) {
        let internal = unsafe { &mut * (self.0).3.get() };
        while let Some(task) = (self.0).2.pop() {
            internal.push_back(task);
        }
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

    /// 立即让出当前任务的执行
    fn yield_now(&self) -> LocalBoxFuture<'static, ()> {
        async move {
            YieldNow(false).await;
        }.boxed_local()
    }

    /// 生成一个异步管道，输入指定流，输入流的每个值通过过滤器生成输出流的值
    pub fn pipeline<S, SO, F, FO>(&self, input: S, mut filter: F) -> LocalBoxStream<'static, FO>
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

    /// 阻塞当前线程，并在当前线程内执行指定的异步任务，返回指定异步任务执行后的结果
    pub fn block_on<F>(&self, future: F) -> IOResult<F::Output>
        where
            F: Future + 'static,
            <F as Future>::Output: Default + 'static,
    {
        let runner = LocalTaskRunner(self.clone());
        let mut result: Option<<F as Future>::Output> = None;
        let result_ptr = (&mut result) as *mut Option<<F as Future>::Output>;

        self.spawn(async move {
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

    /// 关闭异步运行时，返回请求关闭是否成功
    pub fn close(self) -> bool {
        if cfg!(target_arch = "aarch64") {
            if let Ok(true) =
                (self.0)
                    .1
                    .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            {
                //设置运行状态成功
                true
            } else {
                false
            }
        } else {
            if let Ok(true) =
                (self.0)
                    .1
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

///
/// 本地异步任务执行器
///
pub struct LocalTaskRunner<O: Default + 'static = ()>(LocalTaskRuntime<O>);

unsafe impl<O: Default + 'static> Send for LocalTaskRunner<O> {}
impl<O: Default + 'static> !Sync for LocalTaskRunner<O> {}

impl<O: Default + 'static> LocalTaskRunner<O> {
    /// 构建本地异步任务执行器
    pub fn new() -> Self {
        let inner = (
            alloc_rt_uid(),
            Arc::new(AtomicBool::new(false)),
            SegQueue::new(),
            UnsafeCell::new(VecDeque::new()),
            None,
            None,
        );

        LocalTaskRunner(LocalTaskRuntime(Arc::new(inner)))
    }

    /// 构建一个指定了Poller的本地异步任务执行器
    /// 注意当异步任务执行器在运行时，外部不允许使用wait方法
    pub fn with_poll(poller: Arc<Poller>) -> Self {
        let inner = (
            alloc_rt_uid(),
            Arc::new(AtomicBool::new(false)),
            SegQueue::new(),
            UnsafeCell::new(VecDeque::new()),
            Some(AtomicBool::new(false)),
            Some(poller),
        );

        LocalTaskRunner(LocalTaskRuntime(Arc::new(inner)))
    }

    /// 判断是指定使用Poll
    #[inline(always)]
    pub fn is_with_polling(&self) -> bool {
        self.0.0.4.is_some()
            && self.0.0.5.is_some()
    }

    /// 获取当前本地异步任务执行器的运行时
    pub fn get_runtime(&self) -> LocalTaskRuntime<O> {
        self.0.clone()
    }

    /// 启动工作者异步任务执行器
    pub fn startup(
        self,
        thread_name: &str,
        thread_stack_size: usize
    ) -> LocalTaskRuntime<O> {
        let rt = self.get_runtime();
        let rt_copy = rt.clone();
        let _ = thread::Builder::new()
            .name(thread_name.to_string())
            .stack_size(thread_stack_size)
            .spawn(move || {
                (rt_copy.0).1.store(true, Ordering::Relaxed);

                while rt_copy.is_running() {
                    self.poll();
                    self.run_once();
                }
            });

        rt
    }

    /// 启动设置了Poller的工作者异步任务执行器
    pub fn startup_with_poll(
        self,
        thread_name: &str,
        thread_stack_size: usize,
        try_count: usize,
        timeout: Option<Duration>,
    ) -> LocalTaskRuntime<O> {
        let rt = self.get_runtime();
        let rt_copy = rt.clone();
        let _ = thread::Builder::new()
            .name(thread_name.to_string())
            .stack_size(thread_stack_size)
            .spawn(move || {
                (rt_copy.0).1.store(true, Ordering::Relaxed);

                let mut count = try_count;
                while rt_copy.is_running() {
                    self.poll();
                    self.run_once();
                    match self.try_sleep(count, timeout) {
                        Err(e) => {
                            rt_copy.0.1.store(false, Ordering::Release);
                            panic!("Run runtime failed, reason: {:?}", e);
                        },
                        Ok(Some(new_count)) => {
                            count = new_count;
                            continue;
                        },
                        Ok(None) => {
                            count = try_count;
                            continue;
                        },
                    }
                }
            });

        rt
    }

    /// 将外部任务队列中的任务移动到内部任务队列
    /// 注意应该在执行run_once之前调用
    #[inline]
    pub fn poll(&self) {
        while let Some(task) = ((self.0).0).2.pop() {
            unsafe {
               (&mut *((self.0).0).3.get()).push_back(task);
            }
        }
    }

    /// 运行一次本地异步任务执行器
    #[inline]
    pub fn run_once(&self) {
        unsafe {
            if let Some(task) = (&mut *(&(self.0).0).3.get()).pop_front() {
                let waker = waker_ref(&task);
                let mut context = Context::from_waker(&*waker);
                if let Some(mut future) = task.get_inner() {
                    if let Pending = future.as_mut().poll(&mut context) {
                        //当前未准备好，则恢复本地异步任务，以保证本地异步任务不被提前释放
                        task.set_inner(Some(future));
                    }
                }
            }
        }
    }

    /// 尝试休眠当前推动运行时的线程，并在派发新任务或休眠超时后唤醒
    /// 返回Some表示还需要至少多少次尝试以后才可能休眠
    /// 注意休眠只在设置了Poller后有效，且在执行run_once后调用
    #[inline]
    pub fn try_sleep(
        &self,
        try_count: usize,
        timeout: Option<Duration>
    ) -> IOResult<Option<usize>> {
        if !self.is_with_polling() {
            return Ok(Some(try_count));
        }

        if self.0.len() != 0 {
            //还有任务需要处理，则立即返回
            return Ok(Some(try_count));
        }

        if try_count != 0 {
            //尝试计数不为0，则立即返回
            return Ok(Some(try_count - 1));
        }

        let mut events = Events::with_capacity(std::num::NonZeroUsize::new(1).unwrap());
        let _ = self
            .0
            .0
            .5
            .as_ref()
            .unwrap()
            .wait(&mut events, timeout)?;
        self
            .0
            .0
            .4
            .as_ref()
            .unwrap()
            .store(false, Ordering::Release);

        return Ok(None);
    }

    /// 转换为本地异步任务运行时
    pub fn into_local(self) -> LocalTaskRuntime<O> {
        self.0
    }
}

#[test]
fn test_local_runtime_block_on() {
    struct AtomicCounter(AtomicUsize, Instant);
    impl Drop for AtomicCounter {
        fn drop(&mut self) {
            {
                println!(
                    "!!!!!!drop counter, count: {:?}, time: {:?}",
                    self.0.load(Ordering::Relaxed),
                    Instant::now() - self.1
                );
            }
        }
    }

    let rt = LocalTaskRunner::<()>::new().into_local();

    let counter = Arc::new(AtomicCounter(AtomicUsize::new(0), Instant::now()));
    for _ in 0..10000000 {
        let counter_copy = counter.clone();
        let _ = rt.block_on(async move { counter_copy.0.fetch_add(1, Ordering::Relaxed) });
    }
}
