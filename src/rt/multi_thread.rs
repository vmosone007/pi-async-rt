//! # 多线程运行时
//!
//! - [ComputationalTaskPool]\: 计算型的多线程任务池，适合用于Cpu密集型的应用，
//!   不支持运行时伸缩
//! - [StealableTaskPool]\:
//!   可窃取的多线程任务池，适合用于block较多的应用，支持运行时伸缩
//! - [MultiTaskRuntime]\: 异步多线程任务运行时，支持运行时线程伸缩
//! - [MultiTaskRuntimeBuilder]\: 异步多线程任务运行时构建器
//!
//! [ComputationalTaskPool]: struct.ComputationalTaskPool.html
//! [StealableTaskPool]: struct.StealableTaskPool.html
//! [MultiTaskRuntime]: struct.MultiTaskRuntime.html
//! [MultiTaskRuntimeBuilder]: struct.MultiTaskRuntimeBuilder.html
//!
//! # Examples
//!
//! ```
//! use pi_async_rt::rt::{AsyncRuntime, AsyncRuntimeExt};
//! use pi_async_rt::rt::multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder, StealableTaskPool};
//!
//! let pool = StealableTaskPool::with(4,100000,[1, 254],3000);
//! let builer = MultiTaskRuntimeBuilder::new(pool)
//!     .set_timer_interval(1)
//!     .init_worker_size(4)
//!     .set_worker_limit(4, 4);
//! let rt = builer.build();
//! let _ = rt.spawn(async move {});
//! ```
//!
//! # Concurrency and worker ownership
//!
//! The runtime shares one task pool between all worker threads. Pool-wide state must therefore use
//! thread-safe containers, while each worker-local stack, queue selector, and deque worker may only
//! be accessed by the OS thread bound to that exact pool and worker slot. The implementation binds
//! a private `{thread_id, pool pointer}` context when a worker starts and validates it before every
//! owner-only access. A wrong-pool owner operation panics before touching worker-local state;
//! cross-runtime `spawn_local` remains a supported public-queue fallback.
//!
//! This validation is O(1), performs one thread-local read and pointer comparison, does not allocate,
//! clone, lock, spin, block, poll user code, or call into V8/FFI. See the standard regression target
//! `tests/stealable_task_pool_concurrency.rs`.

use std::sync::Arc;
use std::vec::IntoIter;
use std::time::Duration;
use std::future::Future;
use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::thread::{self, Builder};

use async_stream::stream;
use crossbeam_channel::{bounded, Sender};
use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_queue::{ArrayQueue, SegQueue};
use crossbeam_utils::atomic::AtomicCell;
use st3::{StealError,
          fifo::{Worker as FIFOWorker, Stealer as FIFOStealer}};
use flume::bounded as async_bounded;
use futures::{
    future::{BoxFuture, FutureExt},
    stream::{BoxStream, Stream, StreamExt},
    task::waker_ref,
    TryFuture,
};
use parking_lot::{Condvar, Mutex};
use rand::{Rng, thread_rng};
use num_cpus;
use wrr::IWRRSelector;
use quanta::{Clock, Instant as QInstant};
use log::warn;

use super::{
    PI_ASYNC_LOCAL_THREAD_ASYNC_RUNTIME, PI_ASYNC_THREAD_LOCAL_ID, DEFAULT_MAX_HIGH_PRIORITY_BOUNDED, DEFAULT_HIGH_PRIORITY_BOUNDED, DEFAULT_MAX_LOW_PRIORITY_BOUNDED, alloc_rt_uid, local_async_runtime, AsyncMapReduce, AsyncPipelineResult, AsyncRuntime,
    AsyncRuntimeExt, AsyncTask, AsyncTaskPollClaim, AsyncTaskPollGuard, AsyncTaskPool, AsyncTaskPoolExt, AsyncTaskTimerByNotCancel, AsyncTimingTask,
    AsyncWait, AsyncWaitAny, AsyncWaitAnyCallback, AsyncWaitTimeout, LocalAsyncWaitTimeout, LocalAsyncRuntime, TaskId, TaskHandle, YieldNow, prune_stale_waiting_workers, register_waiting_worker, wake_waiting_worker,
    requeue_runtime_task
};

//计算型任务池的既有槽位下限，不随运行时默认启动线程数变化。
#[cfg(not(target_arch = "wasm32"))]
const MIN_COMPUTATIONAL_WORKER_SIZE: usize = 2;
#[cfg(target_arch = "wasm32")]
const MIN_COMPUTATIONAL_WORKER_SIZE: usize = 1;

//默认启动物理核数加一个工作者；无法探测物理核时沿用依赖库的逻辑核数回退。
//仅在构建或零值配置时查询系统，不进入任务调度热路径；wasm32 保持单工作者。
fn default_init_worker_size() -> usize {
    #[cfg(not(target_arch = "wasm32"))]
    {
        num_cpus::get_physical().saturating_add(1)
    }
    #[cfg(target_arch = "wasm32")]
    {
        1
    }
}

/*
* 默认的工作者线程名称前缀
*/
const DEFAULT_WORKER_THREAD_PREFIX: &str = "Default-Multi-RT";

/*
* 默认的线程栈大小
*/
const DEFAULT_THREAD_STACK_SIZE: usize = 1024 * 1024;

/*
* 默认的工作者线程空闲休眠时长，单位ms
*/
const DEFAULT_WORKER_THREAD_SLEEP_TIME: u64 = 10;

/*
* 默认的运行时空闲休眠时长，单位ms，运行时空闲是指绑定当前运行时的队列为空，且定时器内未到期的任务为空
*/
const DEFAULT_RUNTIME_SLEEP_TIME: u64 = 1000;

/*
* 默认的最大权重
*/
const DEFAULT_MAX_WEIGHT: u8 = 254;

/*
* 默认的最小权重
*/
const DEFAULT_MIN_WEIGHT: u8 = 1;

/*
* multi-thread worker id中worker index的掩码
*/
const MULTI_THREAD_WORKER_ID_MASK: usize = 0xffffffff;

/// 当前 OS 线程绑定的 multi-thread runtime worker 上下文。
///
/// 说明：
/// - `thread_id` 保持既有 packed runtime-id/worker-index 表示；未绑定线程为 `usize::MAX`。
/// - `pool` 是当前 worker 持有的 `Arc<P>` 数据地址经类型擦除后的 raw pointer。
/// - raw pointer 只做地址相等比较，永不解引用、释放或转换回引用，也不拥有 pool。
///
/// 业务边界：
/// - 只用于本模块两个内置 multi-thread pool 的 worker-local owner 校验。
/// - 不替代全局 `PI_ASYNC_THREAD_LOCAL_ID`，后者仍服务既有 runtime 公共逻辑。
/// - 不允许据此从错误 runtime/pool 访问 worker-local stack、deque、selector 或 waker。
///
/// 性能与副作用：
/// - Copy-only 固定大小状态；读取、worker id 提取和地址比较均为 O(1)，每线程空间 O(1)。
/// - 上下文读取是纯操作；绑定不是纯操作，会修改当前线程 TLS，且对同一绑定值幂等。
/// - 不分配、不 clone、不加锁、不自旋、不阻塞、不执行 I/O、future、回调或唤醒。
///
/// 安全性：
/// - TLS `Cell` 只被所属 OS 线程访问，因此无需跨线程同步。
/// - worker 闭包在整个工作循环持有 runtime `Arc`，比较期间 pool 数据地址有效。
/// - 内存安全和线程安全不依赖 raw pointer 解引用；错误地址只会导致 fail-fast panic。
/// - 不触达 V8/FFI，不跨 await 保存 guard，异步安全。
#[derive(Clone, Copy)]
struct MultiThreadWorkerContext {
    thread_id: usize,
    pool: *const (),
}

impl MultiThreadWorkerContext {
    const UNBOUND: Self = MultiThreadWorkerContext {
        thread_id: usize::MAX,
        pool: std::ptr::null(),
    };

    /// 返回当前上下文所属 runtime id。
    ///
    /// 只对 packed id 做位移，O(1)、纯函数、幂等、无副作用且不阻塞。调用方必须先使用该值
    /// 判断 task 是否属于当前 runtime，才能决定是 local owner 路径还是 public fallback。
    #[inline]
    const fn runtime_id(self) -> usize {
        self.thread_id >> 32
    }

    /// 校验当前线程属于 `pool`，并返回 owner worker index。
    ///
    /// 参数：
    /// - `pool`：即将访问 owner-only worker 状态的具体 pool；只借用，不保存、不 clone。
    ///
    /// 返回：
    /// - pool 地址匹配时返回 packed id 的低 32 位 worker index。
    ///
    /// Panic：
    /// - 当前线程未绑定 multi-thread worker，或绑定到另一个 pool 时，在访问任何 owner-only
    ///   状态之前 panic。该 panic 表示调用方违反 `try_pop`/local worker owner 前置条件。
    ///
    /// 性能和安全：
    /// - O(1) 时间、O(1) 空间、纯只读、幂等；正常路径仅一次 raw pointer 比较和位与。
    /// - 无锁、无分配、无 clone、无阻塞；raw pointer 永不解引用，因此比较本身内存安全。
    #[inline]
    fn owner_worker_id<P>(self, pool: &P) -> usize {
        let expected = pool as *const P as *const ();
        if self.pool != expected {
            panic!(
                "Multi-thread task pool owner mismatch: owner-only worker state requires the worker bound to this pool"
            );
        }

        self.thread_id & MULTI_THREAD_WORKER_ID_MASK
    }
}

thread_local! {
    /// 当前 OS 线程的 multi-thread worker/pool 绑定。
    ///
    /// 默认未绑定；只由 `bind_multi_thread_worker_context` 在 worker 启动时写入。`Cell` 不会
    /// 跨线程共享，无锁、无分配，也不拥有 raw pool pointer 指向的对象。
    static PI_ASYNC_MULTI_THREAD_WORKER_CONTEXT: Cell<MultiThreadWorkerContext>
        = Cell::new(MultiThreadWorkerContext::UNBOUND);
}

/// 读取当前 multi-thread worker 上下文。
///
/// 返回 Copy snapshot，O(1)、无分配、无锁、无阻塞。正常线程生命周期中该操作是纯只读且
/// 幂等；仅在线程 TLS 已销毁的非法调用阶段 panic。它是 owner helper 的内部 fail-fast
/// 边界，不替代或改变公开 `get_thread_id` 使用的既有 TLS。
#[inline]
fn current_multi_thread_worker_context() -> MultiThreadWorkerContext {
    match PI_ASYNC_MULTI_THREAD_WORKER_CONTEXT.try_with(|context| context.get()) {
        Ok(context) => context,
        Err(e) => {
            panic!(
                "Get multi-thread worker context failed, thread: {:?}, reason: {:?}",
                thread::current(),
                e
            );
        },
    }
}

///
/// 计算型的工作者任务队列
///
struct ComputationalTaskQueue<O: Default + 'static> {
    stack: Worker<Arc<AsyncTask<ComputationalTaskPool<O>, O>>>,     //工作者任务栈
    queue: SegQueue<Arc<AsyncTask<ComputationalTaskPool<O>, O>>>,   //工作者任务队列
    thread_waker: Arc<(AtomicBool, Mutex<()>, Condvar)>,            //工作者线程的唤醒器
}

impl<O: Default + 'static> ComputationalTaskQueue<O> {
    //构建计算型的工作者任务队列
    pub fn new(thread_waker: Arc<(AtomicBool, Mutex<()>, Condvar)>) -> Self {
        let stack = Worker::new_lifo();
        let queue = SegQueue::new();

        ComputationalTaskQueue {
            stack,
            queue,
            thread_waker,
        }
    }

    //获取计算型的工作者任务队列的任务数量
    pub fn len(&self) -> usize {
        self.stack.len() + self.queue.len()
    }
}

/// 计算型多线程任务池，适合 CPU 密集型应用，不支持运行时伸缩。
///
/// # 使用方式
///
/// 通过 [`ComputationalTaskPool::new`] 创建与 runtime worker slot 数匹配的 pool，再交给
/// [`MultiTaskRuntimeBuilder`]。外部线程可以使用 runtime 的 `spawn`/`spawn_local`；直接
/// `try_pop`、`try_pop_all` 或取得当前 worker waker 只允许在拥有该 pool 的 worker 上执行。
///
/// ```
/// use pi_async_rt::rt::{AsyncRuntime, multi_thread::{ComputationalTaskPool, MultiTaskRuntimeBuilder}};
///
/// let pool = ComputationalTaskPool::new(2);
/// let runtime = MultiTaskRuntimeBuilder::new(pool)
///     .init_worker_size(2)
///     .set_worker_limit(2, 2)
///     .build();
/// runtime.spawn(async {}).unwrap();
/// ```
///
/// # 业务与错误边界
///
/// - `push` 允许任意线程调用；`push_local` 在非所属 runtime 线程上保持公共队列 fallback。
/// - owner-only pop/waker 操作在未绑定线程或其它 pool 的 worker 上调用会 panic，并保证在访问
///   worker-local `crossbeam_deque::Worker` 前失败。
/// - pool 不定义任务结果、取消、timeout 或 runtime 关闭语义。
///
/// # 性能与安全
///
/// - push/pop 快路径为 O(1)；总长度为原子计数近似值，空间为 O(W + N)。
/// - owner 校验一次 TLS 读取和一次指针比较，无分配、clone、锁、自旋或阻塞。
/// - 类型不是纯值容器：push/pop 会修改队列和统计，非幂等；不会在内部 poll 用户 future。
/// - pool 可在线程间共享；worker-local stack 只由通过 pool identity 校验的 owner worker 访问。
/// - 不持有跨 await guard，不触达 V8/FFI。专项入口：
///   `tests/stealable_task_pool_concurrency.rs` 的 owner/cross-runtime 用例。
pub struct ComputationalTaskPool<O: Default + 'static> {
    workers: Vec<ComputationalTaskQueue<O>>, //工作者的任务队列列表
    waits: Option<Arc<ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>>>, //待唤醒的工作者唤醒器队列
    consume_count: Arc<AtomicUsize>,                                       //任务消费计数
    produce_count: Arc<AtomicUsize>,                                       //任务生产计数
}

// SAFETY: shared queue/counters/waits provide their own synchronization. The non-Sync local
// crossbeam `Worker` is accessed only after `MultiThreadWorkerContext::owner_worker_id` proves the
// caller is the worker bound to this exact pool; other threads use the `SegQueue` path. The worker
// closure retains the runtime Arc for the full access lifetime. The invariant is covered by
// `tests/stealable_task_pool_concurrency.rs`.
unsafe impl<O: Default + 'static> Send for ComputationalTaskPool<O> {}
// SAFETY: see the field-by-field owner and lifetime proof above. No method exposes a reference to
// the local `Worker`, and wrong-pool owner operations panic before indexing or touching it.
unsafe impl<O: Default + 'static> Sync for ComputationalTaskPool<O> {}

impl<O: Default + 'static> Default for ComputationalTaskPool<O> {
    fn default() -> Self {
        let core_len = default_init_worker_size(); //默认槽位数与运行时默认工作者数量保持一致
        ComputationalTaskPool::new(core_len)
    }
}

impl<O: Default + 'static> AsyncTaskPool<O> for ComputationalTaskPool<O> {
    type Pool = ComputationalTaskPool<O>;

    /// 返回当前线程既有的 packed runtime/worker id。
    ///
    /// worker 线程返回 runtime id 与 worker index 的组合值，未绑定线程保持既有
    /// `usize::MAX`。本方法不校验 pool owner，调用目的、返回语义和 panic 边界均未改变。
    /// O(1)、纯只读、幂等、无分配/锁/阻塞/唤醒；只读取当前线程 TLS，线程和内存安全。
    #[inline]
    fn get_thread_id(&self) -> usize {
        match PI_ASYNC_THREAD_LOCAL_ID.try_with(move |thread_id| unsafe {
            // SAFETY: this reads only the current OS thread's TLS UnsafeCell and returns a Copy id.
            *thread_id.get()
        }) {
            Err(e) => {
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
        let index = self.produce_count.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        self.workers[index].queue.push(task);
        Ok(())
    }

    /// 将任务优先提交给所属 worker 的本地 queue，否则保持公共 queue fallback。
    ///
    /// `task` 的 `Arc` 所有权被 queue 接收；本实现当前成功返回 `Ok(())`。当 task owner 与当前
    /// runtime 不同（含外部线程）时不触碰 owner-only 状态；owner 相同但 pool 身份错误时在
    /// queue 访问前 panic。O(1)、零额外分配/clone/锁/阻塞，不 poll 用户 future。该操作非纯、
    /// 非幂等；合法调用域内线程安全、内存安全和异步安全，不触达 V8/FFI。
    #[inline]
    fn push_local(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        let context = current_multi_thread_worker_context();
        let rt_uid = task.owner();
        if context.runtime_id() == rt_uid {
            //当前是运行时所在线程
            let worker = &self.workers[context.owner_worker_id(self)];
            worker.queue.push(task);

            self.produce_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        } else {
            //当前不是运行时所在线程
            self.push(task)
        }
    }

    /// 按既有优先级语义提交任务，并保护最高优先级 owner-only stack。
    ///
    /// `priority` 的阈值和 fallback 顺序不变；`task` 被成功转移后返回 `Ok(())`。只有最高优先级
    /// 且 task 属于当前 runtime 时执行 exact-pool owner 校验，错误 pool 在 stack 访问前 panic。
    /// O(1)、无新增分配/clone/锁/阻塞；非纯、非幂等，不执行任务或用户回调，线程/异步/内存
    /// 安全边界与 [`ComputationalTaskPool::push_local`] 相同。
    #[inline]
    fn push_priority(&self,
                     priority: usize,
                     task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        if priority >= DEFAULT_MAX_HIGH_PRIORITY_BOUNDED {
            //最高优先级
            let context = current_multi_thread_worker_context();
            let rt_uid = task.owner();
            if context.runtime_id() == rt_uid {
                let worker = &self.workers[context.owner_worker_id(self)];
                worker.stack.push(task);

                self.produce_count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            } else {
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

    #[inline]
    fn push_keep(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        self.push_priority(DEFAULT_HIGH_PRIORITY_BOUNDED, task)
    }

    /// 从当前 exact pool 的 owner worker slot 尝试取一个任务。
    ///
    /// 返回任务的 `Arc` 或空队列时的 `None`。只能由该 pool 绑定的 worker 调用；外部线程或
    /// wrong-pool worker 会在索引及 local stack 访问前 panic。快路径 O(1)、零分配、零 clone、
    /// 无锁/自旋/阻塞；消费队列使其非纯、非幂等。本方法不 poll/drop 任务，不触达 V8/FFI，
    /// 在 owner 契约内线程、内存与异步安全。
    #[inline]
    fn try_pop(&self) -> Option<Arc<AsyncTask<Self::Pool, O>>> {
        let id = current_multi_thread_worker_context().owner_worker_id(self);
        let worker = &self.workers[id];
        let task = worker.stack.pop();
        if task.is_some() {
            //指定工作者的任务栈有任务，则立即返回任务
            self.consume_count.fetch_add(1, Ordering::Relaxed);
            return task;
        }

        let task = worker.queue.pop();
        if task.is_some() {
            self.consume_count.fetch_add(1, Ordering::Relaxed);
        }

        task
    }

    /// 反复执行 owner-checked `try_pop`，返回当前可取得任务的 owned iterator。
    ///
    /// 空池返回空 iterator；wrong-pool 调用在首次 pop 前 panic。时间 O(N)、临时空间 O(N)，会
    /// 按既有实现分配 `Vec`，其预分配容量来自近似 `len()`。非纯、非幂等；不阻塞、不 poll
    /// 用户 future，owner 契约内线程/内存/异步安全。该既有批量分配行为不属于本轮修改。
    #[inline]
    fn try_pop_all(&self) -> IntoIter<Arc<AsyncTask<Self::Pool, O>>> {
        let mut tasks = Vec::with_capacity(self.len());
        while let Some(task) = self.try_pop() {
            tasks.push(task);
        }

        tasks.into_iter()
    }

    #[inline]
    fn get_thread_waker(&self) -> Option<&Arc<(AtomicBool, Mutex<()>, Condvar)>> {
        //多线程任务运行时不支持此方法
        None
    }
}

impl<O: Default + 'static> AsyncTaskPoolExt<O> for ComputationalTaskPool<O> {
    #[inline]
    fn set_waits(&mut self, waits: Arc<ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>>) {
        self.waits = Some(waits);
    }

    #[inline]
    fn get_waits(&self) -> Option<&Arc<ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>>> {
        self.waits.as_ref()
    }

    #[inline]
    fn worker_len(&self) -> usize {
        self.workers.len()
    }

    /// clone 当前 exact pool/worker 的休眠唤醒器。
    ///
    /// 合法 owner 返回 `Some(Arc<...>)`；wrong-pool 或未绑定线程在索引前 panic。O(1)，会按
    /// 既有语义执行一次 `Arc::clone`，但 owner 校验自身不 clone/分配/加锁/阻塞。该只读查询
    /// 不唤醒线程，本身幂等但增加引用计数；返回 Arc 的释放由调用方负责，线程和内存安全。
    #[inline]
    fn clone_thread_waker(&self) -> Option<Arc<(AtomicBool, Mutex<()>, Condvar)>> {
        let worker = &self.workers[current_multi_thread_worker_context().owner_worker_id(self)];
        Some(worker.thread_waker.clone())
    }
}

impl<O: Default + 'static> ComputationalTaskPool<O> {
    /// 构建指定 worker slot 数量的计算型多线程任务池。
    ///
    /// # 参数与返回
    ///
    /// - `size`：请求的 worker slot 数；保留非 wasm32 最少 2 个、wasm32 最少 1 个的旧下限。
    /// - 返回独立 pool；尚未绑定 runtime/waits，也不会启动线程或执行 future。
    ///
    /// # 性能与副作用
    ///
    /// 时间和空间复杂度均为 O(W)，W 为实际 slot 数。该函数会分配 worker/queue/waker，但不
    /// 阻塞、不执行 I/O、不创建线程。它不是纯函数；每次调用创建不同 pool，因此非幂等。
    ///
    /// # 安全与边界
    ///
    /// 返回值可安全移动并由 builder 在线程间共享。调用方必须让 builder 的最大 worker 数
    /// 不超过 slot 数；builder 会再次收敛该边界。owner-only API 的线程/pool 限制见类型文档。
    pub fn new(mut size: usize) -> Self {
        if size < MIN_COMPUTATIONAL_WORKER_SIZE {
            //槽位不足时只提升到原固定下限，不扩大显式请求的小任务池。
            size = MIN_COMPUTATIONAL_WORKER_SIZE;
        }

        let mut workers = Vec::with_capacity(size);
        for _ in 0..size {
            let thread_waker = Arc::new((AtomicBool::new(false), Mutex::new(()), Condvar::new()));
            let worker = ComputationalTaskQueue::new(thread_waker);
            workers.push(worker);
        }
        let consume_count = Arc::new(AtomicUsize::new(0));
        let produce_count = Arc::new(AtomicUsize::new(0));

        ComputationalTaskPool {
            workers,
            waits: None,
            consume_count,
            produce_count,
        }
    }
}

///
/// 可窃取的混合任务队列
///
struct StealableTaskQueue<O: Default + 'static> {
    stack:          UnsafeCell<Option<Arc<AsyncTask<StealableTaskPool<O>, O>>>>,    //工作者任务栈
    internal:       FIFOWorker<Arc<AsyncTask<StealableTaskPool<O>, O>>>,            //工作者本地内部任务队列，可窃取
    external:       Worker<Arc<AsyncTask<StealableTaskPool<O>, O>>>,                //工作者本地外部任务队列，可窃取
    selector:       UnsafeCell<IWRRSelector<2>>,                                    //工作者任务队列选择器
    thread_waker:   Arc<(AtomicBool, Mutex<()>, Condvar)>,                          //工作者线程的唤醒器
}

impl<O: Default + 'static> StealableTaskQueue<O> {
    // 构建可窃取的混合任务队列，允许设置初始的栈和队列的初始容量，并自动设置栈和队列的容量
    // 栈和队列的容量是初始容量的最小二次方，例如初始容量为0，则容量为1
    pub fn new(
        init_queue_capacity: usize,
        thread_waker: Arc<(AtomicBool, Mutex<()>, Condvar)>,
    ) -> (Self,
          FIFOStealer<Arc<AsyncTask<StealableTaskPool<O>, O>>>,
          Stealer<Arc<AsyncTask<StealableTaskPool<O>, O>>>) {
        let stack = UnsafeCell::new(None);
        let internal = FIFOWorker::new(init_queue_capacity);
        let external = Worker::new_fifo();
        let internal_stealer = internal.stealer();
        let external_stealer = external.stealer();
        let selector = UnsafeCell::new(IWRRSelector::new([2, 1]));

        (
            StealableTaskQueue {
                stack,
                internal,
                external,
                selector,
                thread_waker,
            },
            internal_stealer,
            external_stealer
        )
    }

    // 获取栈容量
    pub const fn stack_capacity(&self) -> usize {
        1
    }

    // 获取本地内部任务队列容量
    pub fn internal_capacity(&self) -> usize {
        self.internal.capacity()
    }

    // 获取剩余的本地内部任务队列容量，不准确
    pub fn remaining_internal_capacity(&self) -> usize {
        self.internal.spare_capacity()
    }

    /// 返回当前 worker single-item stack 的近似精确长度（0 或 1）。
    ///
    /// 该私有 helper 的强前置条件是调用方已经通过 exact pool owner 校验；它随后只读取当前
    /// owner 的 `UnsafeCell<Option<_>>`。O(1)、零分配/clone/锁/阻塞，纯只读且幂等，不执行
    /// future 或回调。若绕过 owner 前置条件并发调用会破坏 `unsafe impl Sync` 的安全不变量，
    /// 因而所有生产入口必须从 `push_priority` 的已校验 local 分支到达。
    #[inline]
    pub fn stack_len(&self) -> usize {
        unsafe {
            // SAFETY: callers reach this private queue only through the exact pool's owner-checked
            // local branch. No other thread reads or writes this worker slot's single-item stack.
            if (&*self.stack.get()).is_some() {
                1
            } else {
                0
            }
        }
    }

    // 获取本地内部任务队列长度
    pub fn internal_len(&self) -> usize {
        self
            .internal_capacity()
            .checked_sub(self.remaining_internal_capacity())
            .unwrap_or(0)
    }

    // 获取本地外部任务队列长度
    pub fn external_len(&self) -> usize {
        self.external.len()
    }
}

/// 可窃取的混合多线程任务池。
///
/// # 使用方式
///
/// pool 将 external/public 任务和 runtime worker 内产生的 internal/local 任务分开保存，并由
/// 每个 worker 的 `IWRRSelector` 按既有 best-effort 权重选择顺序。典型用法：
///
/// ```
/// use pi_async_rt::rt::{AsyncRuntime, multi_thread::{MultiTaskRuntimeBuilder, StealableTaskPool}};
///
/// let pool = StealableTaskPool::with(4, 4096, [1, 1], 3000);
/// let runtime = MultiTaskRuntimeBuilder::new(pool)
///     .init_worker_size(4)
///     .set_worker_limit(4, 4)
///     .build();
/// runtime.spawn(async {}).unwrap();
/// ```
///
/// # 调度与业务边界
///
/// - `push` 可从任意线程进入 public injector；所属 worker 的 `push_local` 优先进入 internal
///   queue，跨 runtime 调用保持 public fallback。
/// - `try_pop`/`try_pop_all` 和 worker waker 获取是 owner-only 操作，只允许拥有该 exact pool
///   的 worker 调用；错误 pool/外部线程会在访问 worker-local 状态前 panic。
/// - pool-level 流量计数和刷新时间只用于近似调度启发式，不发布任务对象，也不承诺所有 worker
///   selector 同时或最终应用同一权重。
/// - `weights` 保留既有 API/存储语义；本版本不改变其历史行为，不在此处定义新的公平性保证。
/// - timeout、取消、任务结果、worker sleep/wake 和 runtime 关闭由其它层负责。
///
/// # 性能、纯度和副作用
///
/// - local/public pop 快路径为 O(1)；尝试其它 W-1 个 stealer 的最坏时间为 O(W)。空间
///   O(W + N)，N 为排队任务数。
/// - 每次 weighted pop 读取一次 `AtomicCell<QInstant>`；到刷新窗口时写入一次。x86_64 验收
///   目标要求该类型 lock-free；其它目标保证正确性但不承诺无内部锁。
/// - owner 校验 O(1)，一次 TLS Copy 读取和 raw pointer 比较；不分配、不 clone、不锁、不阻塞。
/// - push/pop/刷新均非纯且非幂等，会修改队列、统计或当前 worker selector；不会在 pool 内
///   poll 用户 future、执行回调、I/O 或 V8/FFI。
///
/// # 线程、内存与异步安全
///
/// pool-wide 状态使用并发容器/原子；`stack`、deque worker 和 selector 只由 owner worker
/// 访问。raw pool pointer 仅比较不解引用，worker 闭包持有 runtime Arc 保证生命周期。没有锁
/// guard 跨 await，也不引入引用环。专项和 TSan 入口：
/// `tests/stealable_task_pool_concurrency.rs`。
pub struct StealableTaskPool<O: Default + 'static> {
    public:                         Injector<Arc<AsyncTask<StealableTaskPool<O>, O>>>,          //公共的任务池
    workers:                        Vec<StealableTaskQueue<O>>,                                 //工作者的任务队列列表
    internal_stealers:              Vec<FIFOStealer<Arc<AsyncTask<StealableTaskPool<O>, O>>>>,  //工作者任务队列的本地内部任务窃取者
    external_stealers:              Vec<Stealer<Arc<AsyncTask<StealableTaskPool<O>, O>>>>,      //工作者任务队列的本地外部任务窃取者
    internal_consume:               AtomicUsize,                                                //内部任务消费计数
    internal_produce:               AtomicUsize,                                                //内部任务生产计数
    internal_traffic_statistics:    AtomicUsize,                                                //内部任务流量统计
    external_consume:               AtomicUsize,                                                //外部任务消费计数
    external_produce:               AtomicUsize,                                                //外部任务生产计数
    external_traffic_statistics:    AtomicUsize,                                                //外部任务流量统计
    weights:                        [u8; 2],                                                    //工作者任务队列的权重
    clock:                          Clock,                                                      //任务池的时钟
    interval:                       usize,                                                      //整理的间隔时长，单位ms
    last_time:                      AtomicCell<QInstant>,                                       //线程安全的上一次整理时间
    waits:                          Option<Arc<ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>>>, //待唤醒的工作者唤醒器队列
}

// SAFETY: public injector, stealers, atomic counters, AtomicCell timestamp, clock and waits are safe
// to share. Each queue's stack, local workers and selector remain owner-only: every production path
// that reads or mutates them first validates the current TLS pool pointer and worker index. Stealers
// are the only cross-worker view of local queues. The worker closure retains the runtime Arc for the
// entire loop. This invariant is exercised by the owner, exact-once and TSan standard tests.
unsafe impl<O: Default + 'static> Send for StealableTaskPool<O> {}
// SAFETY: see the field-by-field synchronization and owner proof above. Wrong-pool safe API calls
// panic before worker lookup or UnsafeCell access; no owner-only reference escapes from the pool.
unsafe impl<O: Default + 'static> Sync for StealableTaskPool<O> {}

impl<O: Default + 'static> Default for StealableTaskPool<O> {
    fn default() -> Self {
        StealableTaskPool::new()
    }
}

impl<O: Default + 'static> AsyncTaskPool<O> for StealableTaskPool<O> {
    type Pool = StealableTaskPool<O>;

    /// 返回当前线程既有的 packed runtime/worker id。
    ///
    /// worker 线程返回 runtime id 与 worker index 的组合值，未绑定线程保持既有
    /// `usize::MAX`。本方法不执行新增 owner 校验；O(1)、纯只读、幂等、无分配/锁/阻塞，
    /// 只读取当前线程 TLS，调用目的、可见语义及线程/内存安全边界均保持不变。
    #[inline]
    fn get_thread_id(&self) -> usize {
        match PI_ASYNC_THREAD_LOCAL_ID.try_with(move |thread_id| unsafe {
            // SAFETY: this reads only the current OS thread's TLS UnsafeCell and returns a Copy id.
            *thread_id.get()
        }) {
            Err(e) => {
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
        self.internal_produce
            .load(Ordering::Relaxed)
            .checked_sub(self.internal_consume.load(Ordering::Relaxed))
            .unwrap_or(0)
            +
            self.external_produce
                .load(Ordering::Relaxed)
                .checked_sub(self.external_consume.load(Ordering::Relaxed))
                .unwrap_or(0)
    }

    #[inline]
    fn push(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        self.public.push(task);

        self
            .external_produce
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// 将任务提交到所属 worker 的 internal queue，无法使用本地路径时进入 public injector。
    ///
    /// `task` 所有权被成功转移并返回 `Ok(())`。外部线程或跨 runtime 调用保持 public fallback；
    /// task owner 与当前 runtime 相同但 pool 身份错误时，在读取 internal queue 前 panic。正常
    /// local/public 路径分摊 O(1)、零新增分配/clone/锁/阻塞；非纯、非幂等，不 poll 用户
    /// future。合法调用域内线程安全、内存安全、异步安全，不触达 V8/FFI。
    #[inline]
    fn push_local(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        let context = current_multi_thread_worker_context();
        let rt_uid = task.owner();
        if context.runtime_id() == rt_uid {
            //当前是运行时所在线程
            let worker = &self.workers[context.owner_worker_id(self)];
            if worker.remaining_internal_capacity() > 0 {
                //本地内部任务队列有空闲容量，则立即将任务加入本地内部任务队列
                let _ = worker.internal.push(task);

                self
                    .internal_produce
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            } else {
                //本地内部任务队列没有空闲容量，则立即将任务加入公共任务池
                self.push(task)
            }
        } else {
            //当前不是运行时所在线程
            self.push(task)
        }
    }

    /// 按既有优先级规则提交任务，并保护最高优先级 owner-only single-item stack。
    ///
    /// `priority` 的阈值、internal/public fallback 和返回值保持不变。最高优先级本地分支先
    /// 校验 exact pool，错误上下文在 stack/queue 访问前 panic；其它 runtime 继续 public
    /// fallback。分摊 O(1)，无新增分配/clone/锁/阻塞；非纯、非幂等，不执行任务、回调、
    /// I/O 或 V8/FFI，owner 契约内线程/内存/异步安全。
    #[inline]
    fn push_priority(&self,
                     priority: usize,
                     task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        if priority >= DEFAULT_MAX_HIGH_PRIORITY_BOUNDED {
            //最高优先级
            let context = current_multi_thread_worker_context();
            let rt_uid = task.owner();
            if context.runtime_id() == rt_uid {
                //当前是运行时所在线程
                let worker = &self.workers[context.owner_worker_id(self)];
                if worker.stack_len() < 1 {
                    //本地任务栈有空闲容量，则立即将任务加入本地任务栈
                    unsafe {
                        // SAFETY: context.owner_worker_id(self) above proved this exact pool and
                        // worker slot are owned by the current OS thread. The stack never escapes.
                        *worker.stack.get() = Some(task);
                    }
                } else if worker.remaining_internal_capacity() > 0 {
                    //本地内部任务队列有空闲容量，则立即将任务加入本地内部任务队列
                    let _ = worker.internal.push(task);
                } else {
                    //本地任务栈和本地内部任务队列都没有空闲容量，则立即将任务加入公共任务池
                    return self.push(task);
                }

                self
                    .internal_produce
                    .fetch_add(1, Ordering::Relaxed);
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

    #[inline]
    fn push_keep(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()> {
        self.push_priority(DEFAULT_HIGH_PRIORITY_BOUNDED, task)
    }

    /// 从当前 exact pool 的 owner worker slot 按既有权重和 steal 顺序尝试取一个任务。
    ///
    /// 返回任务 `Arc` 或所有候选队列均空时的 `None`。只能由所属 worker 调用；wrong-pool 或
    /// 外部线程在索引、stack、selector 及 local deque 访问前 panic。local 快路径 O(1)，尝试
    /// W-1 个 stealer 最坏 O(W)；原实现的空队列 steal 路径可能构造 O(W) 临时 `Vec`，本轮不
    /// 改该行为。新增 owner guard 零分配/clone/锁/阻塞。消费和统计更新使本方法非纯、非幂等；
    /// 不 poll 用户 future，owner 契约内线程/内存/异步安全。
    #[inline]
    fn try_pop(&self) -> Option<Arc<AsyncTask<Self::Pool, O>>> {
        let id = current_multi_thread_worker_context().owner_worker_id(self);
        let worker = &self.workers[id];
        let task = unsafe {
            // SAFETY: owner_worker_id validated the exact pool before indexing. Only this worker
            // accesses its stack, so taking the Option through UnsafeCell is exclusive.
            (&mut *worker
                .stack
                .get())
                .take()
        };
        if task.is_some() {
            //指定工作者的任务栈有任务，则立即返回任务
            return task;
        }

        //从指定工作者的任务队列中弹出任务
        try_pop_by_weight(self, worker, id)
    }

    /// 反复执行 owner-checked `try_pop`，返回当前可取得任务的 owned iterator。
    ///
    /// 空池返回空 iterator；wrong-pool 调用在首次 pop 前 panic。除每次 pop 的既有复杂度外，
    /// 汇总 N 个任务需 O(N) 时间和 O(N) `Vec` 空间；会分配但不阻塞、不执行任务。该操作非纯、
    /// 非幂等，owner 契约内线程/内存/异步安全；批量收集语义和分配行为未被本轮改变。
    #[inline]
    fn try_pop_all(&self) -> IntoIter<Arc<AsyncTask<Self::Pool, O>>> {
        let mut tasks = Vec::with_capacity(self.len());
        while let Some(task) = self.try_pop() {
            tasks.push(task);
        }

        tasks.into_iter()
    }

    #[inline]
    fn get_thread_waker(&self) -> Option<&Arc<(AtomicBool, Mutex<()>, Condvar)>> {
        //多线程任务运行时不支持此方法
        None
    }
}

// 获取指定数字的MSB
const fn get_msb(n: usize) -> usize {
    usize::BITS as usize - n.leading_zeros() as usize
}

/// 使用既有近似流量统计刷新当前 owner worker 的 selector，并按权重尝试取任务。
///
/// 参数：
/// - `pool`：所有 worker 共享的 exact `StealableTaskPool`；只借用，不 clone/保存。
/// - `local_worker`：已经由调用方 owner 校验选出的当前 worker slot。
/// - `local_worker_id`：上述 slot 的索引，用于偷取时排除自己。
///
/// 返回：按原 selector、fallback 和 steal 顺序取得一个任务，或队列均为空时返回 `None`。
/// 任务所有权随 `Arc` 返回给工作循环；本函数不 poll、drop 或执行任务。
///
/// 并发与边界：
/// - `last_time` 是 pool-wide `AtomicCell<QInstant>`；所有 worker 可并发 load/store，不存在普通
///   共享读写。允许多个 worker 在同一近似窗口刷新，保持旧 best-effort 行为，故不使用 CAS。
/// - traffic counters 是 Relaxed 近似统计，不承担任务对象发布；交错采样只影响启发式权重。
/// - selector 仍只修改 `local_worker`，其 UnsafeCell 安全性依赖调用方已完成 exact pool owner
///   校验。该前置条件由私有调用链 `StealableTaskPool::try_pop` 保证。
/// - interval 非零由构造器保证；quanta 的 `duration_since` 对倒退采样饱和为零。
///
/// 性能、纯度与安全：
/// - 常规选择 O(1)；steal 最坏 O(W)，W 为 worker 数；本轮新增原子访问 O(1)、零分配。
/// - 非纯、非幂等：可能更新统计、selector、刷新时间并消费队列任务。
/// - 本实现不显式加锁、自旋等待或阻塞，不执行 I/O/回调/V8/FFI，不跨 await 保存状态。
/// - x86_64 上 `AtomicCell<QInstant>` 的 lock-free 前提由专项测试固定；其它 target 的
///   crossbeam fallback 可能使用内部同步，只保证线程安全正确性，不承诺无锁性能。
fn try_pop_by_weight<O: Default + 'static>(pool: &StealableTaskPool<O>,
                                           local_worker: &StealableTaskQueue<O>,
                                           local_worker_id: usize)
                                           -> Option<Arc<AsyncTask<StealableTaskPool<O>, O>>> {
    unsafe {
        // SAFETY: this private helper is called only after try_pop validates the current thread as
        // owner of `local_worker` in this exact pool. Consequently selector has one mutable owner;
        // pool-wide timestamp/counters are atomic and stealing uses dedicated thread-safe stealers.
        let duration = pool
            .clock
            .recent()
            .duration_since(pool.last_time.load())
            .as_millis() as usize;
        if duration >= pool.interval {
            //开始整理外部任务队列和内部任务队列的任务数量，并更新权重
            let new_external_traffic_statistics = pool
                .external_produce
                .load(Ordering::Relaxed);
            let new_internal_traffic_statistics = pool
                .internal_produce
                .load(Ordering::Relaxed);

            //获取外部任务增量和内部任务增量
            let external_delta = if new_external_traffic_statistics == 0 {
                //上次整理到本次整理之间，外部任务数量为空，则增量为1
                1
            } else {
                //上次整理到本次整理之间，外部任务数量不为空，则计算两次整理之间的外部任务数量的增量
                new_external_traffic_statistics
                    .checked_sub(pool
                        .external_traffic_statistics
                        .load(Ordering::Relaxed))
                    .unwrap_or(1)
            };
            pool
                .external_traffic_statistics
                .store(new_external_traffic_statistics, Ordering::Relaxed); //更新外部任务流量统计
            let internal_delta = if new_internal_traffic_statistics == 0 {
                //上次整理到本次整理之间，内部任务数量为空，则增量为1
                1
            } else {
                //上次整理到本次整理之间，内部任务数量不为空，则计算两次整理之间的内部任务数量的增量
                new_internal_traffic_statistics
                    .checked_sub(pool
                        .internal_traffic_statistics
                        .load(Ordering::Relaxed))
                    .unwrap_or(1)
            };
            pool
                .internal_traffic_statistics
                .store(new_internal_traffic_statistics, Ordering::Relaxed); //更新内部任务流量统计

            //更新外部任务队列和内部任务队列的权重
            let selector = &mut *local_worker.selector.get();
            if external_delta > internal_delta {
                //内部任务增量较小
                let msb = get_msb(internal_delta);
                let internal_weight
                    = (internal_delta >> msb.checked_sub(2).unwrap_or(0)).max(1);
                let external_weight
                    = ((external_delta >> msb).min(DEFAULT_MAX_WEIGHT as usize)).max(1);

                selector.change_weight(0, external_weight as u8);
                selector.change_weight(1, internal_weight as u8);
            } else if external_delta < internal_delta {
                //外部任务增量较小
                let msb = get_msb(external_delta);
                let external_weight
                    = (external_delta >> msb.checked_sub(2).unwrap_or(0)).max(1);
                let internal_weight
                    = ((internal_delta >> msb).min(DEFAULT_MAX_WEIGHT as usize)).max(1);

                selector.change_weight(0, external_weight as u8);
                selector.change_weight(1, internal_weight as u8);
            } else {
                //外部任务和内部任务增量相同
                selector.change_weight(0, 1);
                selector.change_weight(1, 1);
            }

            pool.last_time.store(pool.clock.recent()); //线程安全地更新上一次整理时间
        }

        //根据权重选择从指定的任务队列弹出任务
        match (&mut *local_worker.selector.get()).select() {
            0 => {
                //弹出外部任务
                let task = try_pop_external(pool, local_worker, local_worker_id);
                if task.is_some() {
                    task
                } else {
                    //当前没有外部任务，则尝试弹出内部任务
                    try_pop_internal(pool, local_worker, local_worker_id)
                }
            },
            _ => {
                //弹出内部任务
                let task = try_pop_internal(pool, local_worker, local_worker_id);
                if task.is_some() {
                    task
                } else {
                    //当前没有内部任务，则尝试弹出外部任务
                    try_pop_external(pool, local_worker, local_worker_id)
                }
            },
        }
    }
}

// 尝试弹出内部任务队列的任务
#[inline]
fn try_pop_internal<O: Default + 'static>(pool: &StealableTaskPool<O>,
                                          local_worker: &StealableTaskQueue<O>,
                                          local_worker_id: usize)
    -> Option<Arc<AsyncTask<StealableTaskPool<O>, O>>> {
    let task = local_worker
        .internal
        .pop();
    if task.is_some() {
        //如果工作者有内部任务，则立即返回
        pool
            .internal_consume
            .fetch_add(1, Ordering::Relaxed);
        task
    } else {
        //工作者的内部任务队列为空，则随机从其它工作者的内部任务队列中窃取任务
        let mut gen = thread_rng();
        let mut worker_stealers: Vec<&FIFOStealer<Arc<AsyncTask<StealableTaskPool<O>, O>>>> = pool
            .internal_stealers
            .iter()
            .enumerate()
            .filter_map(|(index, other)| {
                if index != local_worker_id {
                    Some(other)
                } else {
                    //忽略本地工作者
                    None
                }
            })
            .collect();

        let remaining_len = local_worker.remaining_internal_capacity();
        loop {
            //随机窃取其它工作者的任务队列
            if worker_stealers.len() == 0 {
                //所有其它工作者的任务队列都为空，则返回空
                break;
            }

            let index = gen.gen_range(0..worker_stealers.len());
            let worker_stealer = worker_stealers.swap_remove(index);

            match worker_stealer.steal_and_pop(&local_worker.internal,
                                               |count| {
                                                   let stealable_len = count / 2;
                                                   if stealable_len <= remaining_len {
                                                       //当前工作者内部任务队列的剩余容量足够，则窃取指定的其它工作者的内部任务队列中一半的任务
                                                       if stealable_len == 0 {
                                                           1
                                                       } else {
                                                           stealable_len
                                                       }
                                                   } else {
                                                       //当前工作者内部任务队列的剩余容量不足够，则从指定的其它工作者的内部任务队列中窃取当前工作者内部任务队列剩余容量的任务
                                                       remaining_len
                                                   }
                                               }) {
                Err(StealError::Empty) => {
                    //指定的其它工作者的内部任务队列中没有可窃取的任务，则继续窃取下一个其它工作者的内部任务队列
                    continue;
                },
                Err(StealError::Busy) => {
                    //需要重试窃取指定的其它工作者的内部任务队列中的任务
                    continue;
                },
                Ok((task, _)) => {
                    //从从已窃取到的其它工作者内部任务中获取到首个任务，并立即返回
                    pool.internal_consume.fetch_add(1, Ordering::Relaxed);
                    return Some(task);
                },
            }
        }

        None
    }
}

// 尝试弹出外部任务队列的任务
#[inline]
fn try_pop_external<O: Default + 'static>(pool: &StealableTaskPool<O>,
                                          local_worker: &StealableTaskQueue<O>,
                                          local_worker_id: usize)
    -> Option<Arc<AsyncTask<StealableTaskPool<O>, O>>> {
    let task = local_worker
        .external
        .pop();
    if task.is_some() {
        //如果工作者有外部任务，则立即返回
        pool
            .external_consume
            .fetch_add(1, Ordering::Relaxed);
        task
    } else {
        //工作者的外部任务队列为空，则从公共任务池中弹出任务
        let task = try_pop_public(pool, local_worker);
        if task.is_some() {
            //如果公共任务池有外部任务，则立即返回
            pool
                .external_consume
                .fetch_add(1, Ordering::Relaxed);
            task
        } else {
            //公共任务池为空，则随机从其它工作者的外部任务队列中窃取任务
            let mut gen = thread_rng();
            let mut worker_stealers: Vec<&Stealer<Arc<AsyncTask<StealableTaskPool<O>, O>>>> = pool
                .external_stealers
                .iter()
                .enumerate()
                .filter_map(|(index, other)| {
                    if index != local_worker_id {
                        Some(other)
                    } else {
                        //忽略当前工作者
                        None
                    }
                })
                .collect();

            loop {
                //随机窃取其它工作者的任务队列
                if worker_stealers.len() == 0 {
                    //所有其它工作者的外部任务队列都为空，则返回空
                    break;
                }

                let index = gen.gen_range(0..worker_stealers.len());
                let worker_stealer = worker_stealers.swap_remove(index);

                match worker_stealer.steal_batch_and_pop(&local_worker.external) {
                    Steal::Success(task) => {
                        //从从已窃取到的其它工作者外部任务中获取到首个任务，并立即返回
                        pool.external_consume.fetch_add(1, Ordering::Relaxed);
                        return Some(task);
                    },
                    Steal::Retry => {
                        //需要重试窃取指定的其它工作者的外部任务队列中的任务
                        continue;
                    },
                    Steal::Empty => {
                        //指定的其它工作者的外部任务队列中没有可窃取的任务，则继续窃取下一个其它工作者的外部任务队列
                        continue;
                    },
                }
            }

            None
        }
    }
}

// 尝试弹出公共任务池的任务
#[inline]
fn try_pop_public<O: Default + 'static>(pool: &StealableTaskPool<O>,
                                        local_worker: &StealableTaskQueue<O>)
    -> Option<Arc<AsyncTask<StealableTaskPool<O>, O>>> {
    loop {
        match pool.public.steal_batch_and_pop(&local_worker.external) {
            Steal::Empty => {
                //当前公共任务池没有任务
                return None;
            },
            Steal::Retry => {
                //需要重试窃取公共任务池的任务
                continue;
            },
            Steal::Success(task) => {
                //从已窃取到的公共任务中获取到首个任务，并立即返回
                pool.external_consume.fetch_add(1, Ordering::Relaxed);
                return Some(task);
            },
        }
    }
}

impl<O: Default + 'static> AsyncTaskPoolExt<O> for StealableTaskPool<O> {
    #[inline]
    fn set_waits(&mut self, waits: Arc<ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>>) {
        self.waits = Some(waits);
    }

    #[inline]
    fn get_waits(&self) -> Option<&Arc<ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>>> {
        self.waits.as_ref()
    }

    #[inline]
    fn worker_len(&self) -> usize {
        self.workers.len()
    }

    /// clone 当前 exact pool/worker 的休眠唤醒器。
    ///
    /// 合法 owner 返回 `Some(Arc<...>)`；wrong-pool 或未绑定线程在 worker lookup 前 panic。
    /// O(1)，按既有语义执行一次 `Arc::clone`，owner guard 自身不 clone/分配/锁/阻塞，也不会
    /// notify 或产生错误唤醒。查询本身幂等但增加引用计数；调用方负责释放返回 Arc。线程、
    /// 内存和异步安全，不执行用户代码或 V8/FFI。
    #[inline]
    fn clone_thread_waker(&self) -> Option<Arc<(AtomicBool, Mutex<()>, Condvar)>> {
        let id = current_multi_thread_worker_context().owner_worker_id(self);
        if let Some(worker) = self.workers.get(id) {
            return Some(worker.thread_waker.clone());
        }

        None
    }
}

impl<O: Default + 'static> StealableTaskPool<O> {
    /// 使用平台默认 worker slot 数构建可窃取任务池。
    ///
    /// 非 wasm32 使用物理核数的两倍，wasm32 使用 1；内部 queue capacity、初始权重参数和刷新
    /// interval 保持既有默认值。返回值尚未启动线程或绑定 runtime。
    ///
    /// 时间/空间复杂度 O(W)，会分配 W 组 queue/stealer/waker，因此不是纯函数且非幂等；不
    /// 阻塞、不执行 I/O 或 future。owner、安全和错误边界见 [`StealableTaskPool`] 类型文档。
    pub fn new() -> Self {
        #[cfg(not(target_arch = "wasm32"))]
            let size = num_cpus::get_physical() * 2; //默认最大工作者任务池数量是当前cpu物理核的2倍
        #[cfg(target_arch = "wasm32")]
            let size = 1; //默认最大工作者任务池数量是1
        StealableTaskPool::with(size,
                                0x8000,
                                [1, 1],
                                3000)
    }

    /// 构建指定 worker slot、internal queue capacity、权重参数和刷新间隔的任务池。
    ///
    /// # 参数
    ///
    /// - `worker_size`：worker slot 数，必须大于 0；为 0 时保持旧行为并 panic。
    /// - `internal_queue_capacity`：每个 worker internal FIFO 的初始容量；允许 0，由底层 queue
    ///   按其既有规则归一化。值只在构建期消费。
    /// - `weights`：保留的两类队列权重配置。当前版本保持历史存储/调度行为，不新增“所有
    ///   selector 以该值初始化”或“全局收敛”保证；调用方不得据此假设硬公平性。
    /// - `interval`：近似流量统计刷新间隔，单位 ms，必须大于 0；为 0 时 panic。
    ///
    /// # 返回与副作用
    ///
    /// 返回未绑定 runtime 的独立 pool，持有 W 组 worker queue/stealer/waker 和一个 pool-wide
    /// 原子刷新时间。函数会分配内存，不创建线程、不执行用户 future、不进行 I/O。
    ///
    /// # 性能、幂等与安全
    ///
    /// 构建时间/空间 O(W)；不是纯函数且每次创建不同资源，非幂等。运行时 owner 限制、panic
    /// 边界、线程/异步/内存安全和跨 runtime fallback 见类型文档。专项入口为
    /// `tests/stealable_task_pool_concurrency.rs`。
    pub fn with(worker_size: usize,
                internal_queue_capacity: usize,
                weights: [u8; 2],
                interval: usize) -> Self {
        if worker_size == 0 {
            //工作者任务池数量无效，则立即抛出异常
            panic!(
                "Create WorkerTaskPool failed, worker size: {}, reason: invalid worker size",
                worker_size
            );
        }
        if interval == 0 {
            panic!(
                "Create WorkerTaskPool failed, interval: {}, reason: invalid interval",
                worker_size
            );
        }

        let public = Injector::new();
        let mut workers = Vec::with_capacity(worker_size);
        let mut internal_stealers = Vec::with_capacity(worker_size);
        let mut external_stealers = Vec::with_capacity(worker_size);
        for _ in 0..worker_size {
            //初始化指定初始作者任务池数量的工作者任务池和窃取者
            let thread_waker = Arc::new((AtomicBool::new(false), Mutex::new(()), Condvar::new()));
            let (worker,
                internal_stealer,
                external_stealer) =
                StealableTaskQueue::new(internal_queue_capacity,
                                        thread_waker);
            workers.push(worker);
            internal_stealers.push(internal_stealer);
            external_stealers.push(external_stealer);
        }
        let internal_consume = AtomicUsize::new(0);
        let internal_produce = AtomicUsize::new(0);
        let internal_traffic_statistics = AtomicUsize::new(0);
        let external_consume = AtomicUsize::new(0);
        let external_produce = AtomicUsize::new(0);
        let external_traffic_statistics = AtomicUsize::new(0);
        let clock = Clock::new();
        let last_time = AtomicCell::new(clock.recent());

        StealableTaskPool {
            public,
            workers,
            internal_stealers,
            external_stealers,
            internal_consume,
            internal_produce,
            internal_traffic_statistics,
            external_consume,
            external_produce,
            external_traffic_statistics,
            weights,
            clock,
            interval,
            last_time,
            waits: None,
        }
    }
}

///
/// 异步多线程任务运行时，支持运行时线程伸缩
///
pub struct MultiTaskRuntime<
    O: Default + 'static = (),
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O> = StealableTaskPool<O>,
>(
    Arc<(
        usize,                                                  //运行时唯一id
        Arc<P>,                                                 //异步任务池
        Option<
            Vec<(
                Sender<(usize, AsyncTimingTask<P, O>)>,
                Arc<AsyncTaskTimerByNotCancel<P, O>>,
            )>,
        >,                                                      //休眠的异步任务生产者和本地定时器
        AtomicUsize,                                            //定时任务计数器
        Arc<ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>>, //待唤醒的工作者唤醒器队列
        AtomicUsize,                                            //定时器生产计数
        AtomicUsize,                                            //定时器消费计数
    )>,
);

unsafe impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Send
    for MultiTaskRuntime<O, P>
{
}
unsafe impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Sync
    for MultiTaskRuntime<O, P>
{
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Clone
    for MultiTaskRuntime<O, P>
{
    fn clone(&self) -> Self {
        MultiTaskRuntime(self.0.clone())
    }
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>> AsyncRuntime<O>
    for MultiTaskRuntime<O, P>
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
            .5
            .load(Ordering::Relaxed)
            .checked_sub((self.0).6.load(Ordering::Relaxed))
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
            F: Future<Output=O> + Send + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_local_by_id(task_id.clone(), future) {
            return Err(e);
        }

        Ok(task_id)
    }

    /// 派发一个指定优先级的异步任务到异步运行时
    fn spawn_priority<F>(&self, priority: usize, future: F) -> Result<TaskId>
        where
            F: Future<Output=O> + Send + 'static {
        let task_id = self.alloc::<F::Output>();
        if let Err(e) = self.spawn_priority_by_id(task_id.clone(), priority, future) {
            return Err(e);
        }

        Ok(task_id)
    }

    /// 派发一个异步任务到异步运行时，并立即让出任务的当前运行
    fn spawn_yield<F>(&self, future: F) -> Result<TaskId>
        where
            F: Future<Output=O> + Send + 'static {
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
            F: Future<Output=O> + Send + 'static {
        let result = {
            (self.0).1.push(Arc::new(AsyncTask::new(
                task_id,
                (self.0).1.clone(),
                DEFAULT_MAX_LOW_PRIORITY_BOUNDED,
                Some(future.boxed()),
            )))
        };

        let _ = wake_waiting_worker(&(self.0).4);

        result
    }

    fn spawn_local_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where
            F: Future<Output=O> + Send + 'static {
        let should_wake = PI_ASYNC_THREAD_LOCAL_ID
            .try_with(|thread_id| unsafe { ((*thread_id.get()) >> 32) != self.get_id() })
            .unwrap_or(true);
        let result = (self.0).1.push_local(Arc::new(AsyncTask::new(
            task_id,
            (self.0).1.clone(),
            DEFAULT_HIGH_PRIORITY_BOUNDED,
            Some(future.boxed()),
        )));

        if should_wake {
            let _ = wake_waiting_worker(&(self.0).4);
        }

        result
    }

    /// 派发一个指定任务唯一id和任务优先级的异步任务到异步运行时
    fn spawn_priority_by_id<F>(&self,
                               task_id: TaskId,
                               priority: usize,
                               future: F) -> Result<()>
        where
            F: Future<Output=O> + Send + 'static {
        let result = {
            (self.0).1.push_priority(priority, Arc::new(AsyncTask::new(
                task_id,
                (self.0).1.clone(),
                priority,
                Some(future.boxed()),
            )))
        };

        let _ = wake_waiting_worker(&(self.0).4);

        result
    }

    /// 派发一个指定任务唯一id的异步任务到异步运行时，并立即让出任务的当前运行
    #[inline]
    fn spawn_yield_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where
            F: Future<Output=O> + Send + 'static {
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
            F: Future<Output=O> + Send + 'static {
        let rt = self.clone();
        self.spawn_by_id(task_id, async move {
            if let Some(timers) = &(rt.0).2 {
                //为定时器设置定时异步任务
                let id = (rt.0).1.get_thread_id() & 0xffffffff;
                let (_, timer) = &timers[id];
                timer.set_timer(
                    AsyncTimingTask::WaitRun(Arc::new(AsyncTask::new(
                        rt.alloc::<F::Output>(),
                        (rt.0).1.clone(),
                        DEFAULT_MAX_HIGH_PRIORITY_BOUNDED,
                        Some(future.boxed()),
                    ))),
                    time,
                );

                (rt.0).5.fetch_add(1, Ordering::Relaxed);
            }

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

        if let Some(timers) = &(self.0).2 {
            //有本地定时器，则异步等待指定时间
            match PI_ASYNC_THREAD_LOCAL_ID.try_with(move |thread_id| {
                //将休眠的异步任务投递到当前派发线程的定时器内
                let thread_id = unsafe { *thread_id.get() };
                let index = thread_id & 0xffffffff;
                if index > timers.len() {
                    //当前线程还未初始化运行时的线程id，说明当前线程不是当前多线程运行时的所属线程
                    TimerTaskProducor::Foreign(timers[(self.0).3.load(Ordering::Relaxed) % timers.len()].0.clone())
                } else {
                    TimerTaskProducor::Local(timers[index].1.clone())
                }
            }) {
                Err(_) => {
                    panic!("Multi thread runtime timeout failed, reason: local thread id not match")
                }
                Ok(producor) => match producor {
                    TimerTaskProducor::Local(timer) => {
                        LocalAsyncWaitTimeout::new(rt, timer, timeout).boxed()
                    },
                    TimerTaskProducor::Foreign(producor) => {
                        AsyncWaitTimeout::new(rt, producor, timeout).boxed()
                    },
                },
            }
        } else {
            //没有本地定时器，则同步休眠指定时间
            async move {
                thread::sleep(Duration::from_millis(timeout as u64));
            }
            .boxed()
        }
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
    for MultiTaskRuntime<O, P>
{
    fn spawn_with_context<F, C>(&self, task_id: TaskId, future: F, context: C) -> Result<()>
    where
        F: Future<Output = O> + Send + 'static,
        C: 'static,
    {
        let task = Arc::new(AsyncTask::with_context(
            task_id,
            (self.0).1.clone(),
            DEFAULT_MAX_LOW_PRIORITY_BOUNDED,
            Some(future.boxed()),
            context,
        ));
        let result = (self.0).1.push(task);

        let _ = wake_waiting_worker(&(self.0).4);

        result
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
            if let Some(timers) = &(rt.0).2 {
                //为定时器设置定时异步任务
                let id = (rt.0).1.get_thread_id() & 0xffffffff;
                let (_, timer) = &timers[id];
                timer.set_timer(
                    AsyncTimingTask::WaitRun(Arc::new(AsyncTask::with_context(
                        rt.alloc::<F::Output>(),
                        (rt.0).1.clone(),
                        DEFAULT_MAX_HIGH_PRIORITY_BOUNDED,
                        Some(future.boxed()),
                        context,
                    ))),
                    time,
                );

                (rt.0).5.fetch_add(1, Ordering::Relaxed);
            }

            Default::default()
        })
    }

    fn block_on<F>(&self, future: F) -> Result<F::Output>
    where
        F: Future + Send + 'static,
        <F as Future>::Output: Default + Send + 'static,
    {
        //从本地线程获取当前异步运行时
        if let Some(local_rt) = local_async_runtime::<F::Output>() {
            //本地线程绑定了异步运行时
            if local_rt.get_id() == self.get_id() {
                //如果是相同运行时，则立即返回错误
                return Err(Error::new(
                    ErrorKind::WouldBlock,
                    format!("Block on failed, reason: would block"),
                ));
            }
        }

        let (sender, receiver) = bounded(1);
        if let Err(e) = self.spawn(async move {
            //在指定运行时中执行，并返回结果
            let r = future.await;
            sender.send(r);

            Default::default()
        }) {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Block on failed, reason: {:?}", e),
            ));
        }

        //同步阻塞等待异步任务返回
        match receiver.recv() {
            Err(e) => Err(Error::new(
                ErrorKind::Other,
                format!("Block on failed, reason: {:?}", e),
            )),
            Ok(result) => Ok(result),
        }
    }
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>
    MultiTaskRuntime<O, P>
{
    /// 获取当前运行时可新增的工作者数量
    pub fn idler_len(&self) -> usize {
        (self.0).1.idler_len()
    }

    /// 获取当前运行时的工作者数量
    pub fn worker_len(&self) -> usize {
        (self.0).1.worker_len()
    }

    /// 获取当前运行时缓冲区的任务数量，缓冲区的任务暂时没有分配给工作者
    pub fn buffer_len(&self) -> usize {
        (self.0).1.buffer_len()
    }

    /// 获取当前多线程异步运行时的本地异步运行时
    pub fn to_local_runtime(&self) -> LocalAsyncRuntime<O> {
        LocalAsyncRuntime {
            inner: self.as_raw(),
            get_id_func: MultiTaskRuntime::<O, P>::get_id_raw,
            spawn_func: MultiTaskRuntime::<O, P>::spawn_raw,
            spawn_local_func: MultiTaskRuntime::<O, P>::spawn_local_raw,
            spawn_timing_func: MultiTaskRuntime::<O, P>::spawn_timing_raw,
            timeout_func: MultiTaskRuntime::<O, P>::timeout_raw,
        }
    }

    /// 获取当前多线程异步运行时的指针
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
                    Option<
                        Vec<(
                            Sender<(usize, AsyncTimingTask<P, O>)>,
                            Arc<AsyncTaskTimerByNotCancel<P, O>>,
                        )>,
                    >,
                    AtomicUsize,
                    Arc<ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>>,
                    AtomicUsize,
                    AtomicUsize,
                ),
            )
        };
        MultiTaskRuntime(inner)
    }

    // 获取当前异步运行时的唯一id
    pub(crate) fn get_id_raw(raw: *const ()) -> usize {
        let rt = MultiTaskRuntime::<O, P>::from_raw(raw);
        let id = rt.get_id();
        Arc::into_raw(rt.0); //避免提前释放
        id
    }

    // 派发一个指定的异步任务到异步运行时
    pub(crate) fn spawn_raw<F>(raw: *const (), future: F) -> Result<()>
    where
        F: Future<Output = O> + Send + 'static,
    {
        let rt = MultiTaskRuntime::<O, P>::from_raw(raw);
        let result = rt.spawn_by_id(rt.alloc::<F::Output>(), future);
        Arc::into_raw(rt.0); //避免提前释放
        result
    }

    // 派发一个指定的异步任务到本地异步运行时
    pub(crate) fn spawn_local_raw<F>(raw: *const (), future: F) -> Result<()>
    where
        F: Future<Output = O> + Send + 'static,
    {
        let rt = MultiTaskRuntime::<O, P>::from_raw(raw);
        let result = rt.spawn_local_by_id(rt.alloc::<F::Output>(), future);
        Arc::into_raw(rt.0); //避免提前释放
        result
    }

    // 定时派发一个指定的异步任务到异步运行时
    pub(crate) fn spawn_timing_raw(
        raw: *const (),
        future: BoxFuture<'static, O>,
        timeout: usize,
    ) -> Result<()> {
        let rt = MultiTaskRuntime::<O, P>::from_raw(raw);
        let result = rt.spawn_timing_by_id(rt.alloc::<O>(), future, timeout);
        Arc::into_raw(rt.0); //避免提前释放
        result
    }

    // 挂起当前异步运行时的当前任务，等待指定的时间后唤醒当前任务
    pub(crate) fn timeout_raw(raw: *const (), timeout: usize) -> BoxFuture<'static, ()> {
        let rt = MultiTaskRuntime::<O, P>::from_raw(raw);
        let boxed = rt.timeout(timeout);
        Arc::into_raw(rt.0); //避免提前释放
        boxed
    }
}

///
/// 异步多线程任务运行时构建器
///
pub struct MultiTaskRuntimeBuilder<
    O: Default + 'static = (),
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O> = StealableTaskPool<O>,
> {
    pool: P,                 //异步多线程任务运行时
    prefix: String,          //工作者线程名称前缀
    init: usize,             //初始工作者数量
    min: usize,              //最少工作者数量
    max: usize,              //最大工作者数量
    stack_size: usize,       //工作者线程栈大小
    timeout: u64,            //工作者空闲时最长休眠时间
    interval: Option<usize>, //工作者定时器间隔
    marker: PhantomData<O>,
}

unsafe impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Send
    for MultiTaskRuntimeBuilder<O, P>
{
}
unsafe impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>> Sync
    for MultiTaskRuntimeBuilder<O, P>
{
}

impl<O: Default + 'static> Default for MultiTaskRuntimeBuilder<O> {
    //默认构建可窃取可伸缩的多线程运行时
    fn default() -> Self {
        let core_len = default_init_worker_size(); //同步调整默认池容量，避免新增工作者被槽位上限截断
        let pool = StealableTaskPool::with(core_len,
                                           65535,
                                           [1, 1],
                                           3000);
        MultiTaskRuntimeBuilder::new(pool)
            .thread_stack_size(2 * 1024 * 1024)
            .set_timer_interval(1)
    }
}

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>
    MultiTaskRuntimeBuilder<O, P>
{
    /// 构建指定任务池、线程名前缀、初始线程数量、最少线程数量、最大线程数量、线程栈大小、线程空闲时最长休眠时间和是否使用本地定时器的多线程任务池
    /// 未显式配置时使用物理核数加 1，wasm32 为 1；build 仍按实际任务池槽位数收敛。
    pub fn new(mut pool: P) -> Self {
        let core_len = default_init_worker_size();

        MultiTaskRuntimeBuilder {
            pool,
            prefix: DEFAULT_WORKER_THREAD_PREFIX.to_string(),
            init: core_len,
            min: core_len,
            max: core_len,
            stack_size: DEFAULT_THREAD_STACK_SIZE,
            timeout: DEFAULT_WORKER_THREAD_SLEEP_TIME,
            interval: None,
            marker: PhantomData,
        }
    }

    /// 设置工作者线程名称前缀
    pub fn thread_prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.to_string();
        self
    }

    /// 设置工作者线程栈大小
    pub fn thread_stack_size(mut self, stack_size: usize) -> Self {
        self.stack_size = stack_size;
        self
    }

    /// 设置初始工作者数量
    /// 0 使用物理核数加 1 的默认值（wasm32 为 1）；非零配置及任务池容量限制保持原行为。
    pub fn init_worker_size(mut self, mut init: usize) -> Self {
        if init == 0 {
            //初始线程数量过小，则设置默认的初始线程数量
            init = default_init_worker_size();
        }

        self.init = init;
        self
    }

    /// 设置最小工作者数量和最大工作者数量
    pub fn set_worker_limit(mut self, mut min: usize, mut max: usize) -> Self {
        if self.init > max {
            //初始线程数量大于最大线程数量，则设置最大线程数量为初始线程数量
            max = self.init;
        }

        if min == 0 || min > max {
            //最少线程数量无效，则设置最少线程数量为最大线程数量
            min = max;
        }

        self.min = min;
        self.max = max;
        self
    }

    /// 设置工作者空闲时最大休眠时长
    pub fn set_timeout(mut self, timeout: u64) -> Self {
        self.timeout = timeout;
        self
    }

    /// 设置工作者定时器间隔
    pub fn set_timer_interval(mut self, interval: usize) -> Self {
        self.interval = Some(interval);
        self
    }

    /// 构建并启动多线程异步运行时。
    ///
    /// 说明：
    /// - 该函数消费 builder，创建 runtime、定时器、waiting worker 队列，并启动初始
    ///   worker 线程。
    /// - 本轮保持公开 API 和启动流程不变，只在构建期增加 worker 数边界收敛，并确保
    ///   任务池保存 runtime 共享 waits 队列。
    ///
    /// 入参：
    /// - 使用 builder 中已经配置好的 pool、线程名前缀、栈大小、worker 数、sleep timeout
    ///   和 timer interval。
    ///
    /// 返回：
    /// - 已启动的 `MultiTaskRuntime<O, P>`。
    ///
    /// 边界条件：
    /// - 如果 pool 的 `worker_len()` 为 0，立即 panic；有效任务池不允许没有 worker slot。
    /// - 如果 `init/max` 大于 pool worker slot 数，会收敛到 `pool.worker_len()`。
    /// - 如果收敛后 `min > max`，会把 `min` 收敛到 `max`。
    /// - 上述收敛只避免内部 worker slot 越界，不改变已存在的公开方法签名。
    ///
    /// 性能：
    /// - 构建时间 O(W)，空间 O(W)，W 为最终 `max` worker 数。
    /// - 该函数不是任务调度热路径。
    ///
    /// 副作用：
    /// - 非纯函数，会分配 runtime 内部结构、注入 waits 队列、启动 worker 线程。
    /// - 不执行用户 future；worker 启动后由工作循环正常消费任务。
    ///
    /// 安全性：
    /// - 不引入新的 unsafe。
    /// - 线程安全依赖 `AsyncTaskPoolExt::set_waits` 在 pool 被放入 `Arc` 前完成，之后 waits
    ///   通过 `Arc<ArrayQueue<...>>` 在线程间共享。
    pub fn build(mut self) -> MultiTaskRuntime<O, P> {
        let pool_worker_len = self.pool.worker_len();
        if pool_worker_len == 0 {
            panic!("Build multi thread runtime failed, reason: worker pool is empty");
        }
        if self.init > pool_worker_len {
            self.init = pool_worker_len;
        }
        if self.max > pool_worker_len {
            self.max = pool_worker_len;
        }
        if self.min > self.max {
            self.min = self.max;
        }

        //构建多线程任务运行时的本地定时器和定时异步任务生产者
        let interval = self.interval;
        let mut timers = if let Some(_) = interval {
            Some(Vec::with_capacity(self.max))
        } else {
            None
        };
        for _ in 0..self.max {
            //初始化指定的最大线程数量的本地定时器和定时异步任务生产者，定时器不会在关闭工作者时被移除
            if let Some(vec) = &mut timers {
                let timer = AsyncTaskTimerByNotCancel::new();
                let producor = timer.producor.clone();
                let timer = Arc::new(timer);
                vec.push((producor, timer));
            };
        }

        //构建多线程任务运行时
        let rt_uid = alloc_rt_uid();
        let waits = Arc::new(ArrayQueue::new(self.max));
        let mut pool = self.pool;
        pool.set_waits(waits.clone()); //设置待唤醒的工作者唤醒器队列
        let pool = Arc::new(pool);
        let runtime = MultiTaskRuntime(Arc::new((
            rt_uid,
            pool,
            timers,
            AtomicUsize::new(0),
            waits,
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        )));

        //构建初始化线程数量的线程构建器
        let mut builders = Vec::with_capacity(self.init);
        for index in 0..self.init {
            let builder = Builder::new()
                .name(self.prefix.clone() + "-" + index.to_string().as_str())
                .stack_size(self.stack_size);
            builders.push(builder);
        }

        //启动工作者线程
        let min = self.min;
        for index in 0..builders.len() {
            let builder = builders.remove(0);
            let runtime = runtime.clone();
            let timeout = self.timeout;
            let timer = if let Some(timers) = &(runtime.0).2 {
                let (_, timer) = &timers[index];
                Some(timer.clone())
            } else {
                None
            };

            spawn_worker_thread(builder, index, runtime, min, timeout, interval, timer);
        }

        runtime
    }
}

/// 将当前 OS worker 绑定到既有 runtime thread id 和 exact task pool。
///
/// 参数：
/// - `thread_id`：由 runtime uid 和 worker index 组成的既有 packed id。
/// - `pool`：worker 即将驱动的 exact pool；只取稳定数据地址，不保存引用或增加引用计数。
///
/// 副作用与幂等：
/// - 非纯函数，会写当前线程的 `PI_ASYNC_THREAD_LOCAL_ID` 和模块私有 owner context。
/// - 对相同线程、相同 id/pool 重复调用是幂等的；本实现只在 worker 启动时调用一次。
///
/// 性能与阻塞：
/// - O(1) 时间、每线程 O(1) TLS 空间；无 heap allocation、Arc clone、锁、自旋、阻塞或 I/O。
/// - 不是任务 poll 热路径，只在 worker startup 执行。
///
/// 安全与错误边界：
/// - 写入旧 TLS 的 UnsafeCell 是安全的，因为 thread-local 实例只由当前 OS 线程访问。
/// - raw pool pointer 只在后续 owner guard 中比较，永不解引用；worker closure 持有 runtime Arc。
/// - TLS 已销毁时 panic；此时禁止启动/重绑 worker。无用户回调、V8/FFI 或跨 await 行为。
fn bind_multi_thread_worker_context<P>(thread_id: usize, pool: &P) {
    if let Err(e) = PI_ASYNC_THREAD_LOCAL_ID.try_with(|local_thread_id| unsafe {
        // SAFETY: this UnsafeCell belongs to the current OS thread's TLS instance. Worker startup
        // writes it before entering any task-pool operation, and no other thread can alias it.
        *local_thread_id.get() = thread_id;
    }) {
        panic!(
            "Multi thread runtime startup failed, thread id: {:?}, reason: {:?}",
            thread_id & MULTI_THREAD_WORKER_ID_MASK,
            e
        );
    }

    let context = MultiThreadWorkerContext {
        thread_id,
        pool: pool as *const P as *const (),
    };
    if let Err(e) = PI_ASYNC_MULTI_THREAD_WORKER_CONTEXT.try_with(|current| {
        current.set(context);
    }) {
        panic!(
            "Bind multi-thread worker pool failed, thread id: {:?}, reason: {:?}",
            thread_id & MULTI_THREAD_WORKER_ID_MASK,
            e
        );
    }
}

/// 创建一个 OS worker，绑定 runtime/pool 上下文，并进入 timer 或 non-timer 工作循环。
///
/// `builder`、`index`、`runtime`、worker limit、sleep timeout 和可选 timer 均由已校验的
/// `MultiTaskRuntimeBuilder::build` 传入。成功时函数只提交线程创建并立即返回；线程内部先绑定
/// TLS，再绑定 local runtime，最后进入原工作循环。线程创建失败保持旧行为，由 `spawn` 返回值
/// 被忽略；本轮不改变该既有错误语义。
///
/// 构建入口 O(1)，每个 worker 固定 O(1) 额外 TLS；会创建线程和分配线程栈，但不是任务热路径。
/// 不在锁内绑定或执行 future，不增加阻塞/死锁/重入边界。`timer` 为 `Some` 时仍只归该 worker
/// 使用，本 helper 不共享或迁移 timer。公开 API、任务执行顺序和 V8/FFI 边界不变。
fn spawn_worker_thread<
    O: Default + 'static,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
>(
    builder: Builder,
    index: usize,
    runtime: MultiTaskRuntime<O, P>,
    min: usize,
    timeout: u64,
    interval: Option<usize>,
    timer: Option<Arc<AsyncTaskTimerByNotCancel<P, O>>>,
) {
    if let Some(timer) = timer {
        //设置了定时器
        let rt_uid = runtime.get_id();
        let _ = builder.spawn(move || {
            //设置线程本地唯一id并绑定exact pool owner上下文
            let thread_id = rt_uid << 32 | index & MULTI_THREAD_WORKER_ID_MASK;
            bind_multi_thread_worker_context(thread_id, (runtime.0).1.as_ref());

            //绑定运行时到线程
            let runtime_copy = runtime.clone();
            match PI_ASYNC_LOCAL_THREAD_ASYNC_RUNTIME.try_with(move |rt| {
                let raw = Arc::into_raw(Arc::new(runtime_copy.to_local_runtime()))
                    as *mut LocalAsyncRuntime<O> as *mut ();
                rt.store(raw, Ordering::Relaxed);
            }) {
                Err(e) => {
                    panic!("Bind multi runtime to local thread failed, reason: {:?}", e);
                }
                Ok(_) => (),
            }

            //执行有定时器的工作循环
            timer_work_loop(
                runtime,
                index,
                min,
                timeout,
                interval.unwrap() as u64,
                timer,
            );
        });
    } else {
        //未设置定时器
        let rt_uid = runtime.get_id();
        let _ = builder.spawn(move || {
            //设置线程本地唯一id并绑定exact pool owner上下文
            let thread_id = rt_uid << 32 | index & MULTI_THREAD_WORKER_ID_MASK;
            bind_multi_thread_worker_context(thread_id, (runtime.0).1.as_ref());

            //绑定运行时到线程
            let runtime_copy = runtime.clone();
            match PI_ASYNC_LOCAL_THREAD_ASYNC_RUNTIME.try_with(move |rt| {
                let raw = Arc::into_raw(Arc::new(runtime_copy.to_local_runtime()))
                    as *mut LocalAsyncRuntime<O> as *mut ();
                rt.store(raw, Ordering::Relaxed);
            }) {
                Err(e) => {
                    panic!("Bind multi runtime to local thread failed, reason: {:?}", e);
                }
                Ok(_) => (),
            }

            //执行无定时器的工作循环
            work_loop(runtime, index, min, timeout);
        });
    }
}

/// worker 空闲等待的结果。
///
/// 说明：
/// - 该枚举只用于多线程运行时内部工作循环，不属于公开 API。
/// - 它把“休眠超时”“未进入休眠/被唤醒”“在休眠前二次检查直接拿到任务”三个结果
///   分开，避免工作循环用布尔值推断调度状态。
///
/// 业务边界：
/// - 不表达任务执行结果，也不表达 runtime 关闭状态。
/// - `Task` 只表示 worker 在进入 condvar wait 前从真实任务池取到了一个任务，调用方
///   必须立即走正常 `run_task` 路径。
///
/// 性能与安全：
/// - 纯数据枚举，本身无副作用、不分配、不阻塞。
/// - 持有 `Arc<AsyncTask<...>>` 的 `Task` 分支遵循原任务池所有权语义。
enum WorkerWaitResult<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>> {
    TimedOut,
    NotSlept,
    Task(Arc<AsyncTask<P, O>>),
}

/// 在 worker 空闲时注册可唤醒状态，并在必要时进入 condvar 等待。
///
/// 说明：
/// - 这是多线程运行时 worker sleep/wake 协议的唯一入口。
/// - 目标是保证外部线程在任务入队后只要存在 sleeping worker，就能即时唤醒一个 worker；
///   同时 worker 不会在“任务已入队但未被 notify”的状态下睡到 `sleep_timeout`。
/// - 该函数不改变任务执行语义，不创建/销毁任务，不修改公开 API。
///
/// 核心协议：
/// 1. 锁内注册：短暂持有当前 worker 的 `worker_waker` 锁，把唤醒器放入 waits 队列，
///    并在同一临界区发布 `is_sleep = true`。
/// 2. 锁外二次检查：释放 worker_waker 锁后检查真实任务池。如果已有任务，则取消
///    `is_sleep` 并直接返回任务或返回 `NotSlept`。
/// 3. 锁内等待：再次短暂持锁确认 `is_sleep` 仍为 true。若外部唤醒已把它置为 false，
///    直接返回 `NotSlept`；否则执行 `condvar.wait_for`。
///
/// 为什么这样设计：
/// - 注册和发布在同一把锁内连续完成，外部 wake 端弹出 waits 条目后会获取同一把锁，
///   因而不会把“已入队但尚未发布 true”的 worker 当成 stale，也不会漏唤醒。
/// - 任务队列 `try_pop` / `len` 放在 worker_waker 锁外，避免 worker_waker 临界区与
///   任务队列窃取、随机选择、统计更新等热路径逻辑重叠。
/// - `condvar.wait_for` 是唯一可能阻塞点；它只发生在确认队列无任务且 `is_sleep` 仍为
///   true 之后，并且 parking_lot 会在等待期间释放 mutex。
///
/// 参数：
/// - `runtime`：当前 worker 所属的多线程 runtime。
/// - `worker_waker`：当前 worker 独占使用的线程唤醒器。
/// - `sleep_timeout`：本次允许休眠的最长时长，单位 ms。定时器 worker 会传入计算后的
///   timer-aware timeout，普通 worker 会传入 builder 配置的 worker sleep timeout。
///
/// 返回：
/// - `TimedOut`：进入了 condvar wait，且本次由超时返回。调用方可增加连续休眠计数。
/// - `NotSlept`：没有进入有效休眠，或被 notify/取消后需要回到 poll loop 重新检查队列。
/// - `Task(task)`：休眠前二次检查直接取到任务，调用方应立即执行该任务。
///
/// 边界条件：
/// - waits 队列满时会释放当前 worker 锁，再清理 stale entry；若清理后仍无法注册，
///   返回 `NotSlept`，禁止无唤醒入口地休眠。
/// - 外部 wake 与 worker 二次检查竞态时，`is_sleep` 的 CAS/store 会收敛到最多一次
///   notify；额外的 `NotSlept` 只会让 worker 回到 poll loop，不会丢任务。
/// - sleep_timeout 为 0 时，`wait_for(0ms)` 会立即返回，不改变语义。
///
/// 性能：
/// - 快路径时间复杂度 O(1)，空间复杂度 O(1)。
/// - waits 满且需要清理 stale 时最坏 O(W)，W 为最大 worker 数；该慢路径只在注册失败
///   时触发，不在每次 wake 热路径上执行。
/// - 每个休眠周期最多 clone 一次 `worker_waker` Arc 用于队列登记；任务唤醒路径不额外
///   clone worker_waker。
///
/// 纯度与副作用：
/// - 非纯函数。会修改 waits 队列、当前 worker 的 `is_sleep` 状态，并可能从任务池取出
///   一个任务。
/// - 非幂等：每次调用代表一个新的 worker 空闲等待尝试。
///
/// 阻塞性：
/// - 除 `condvar.wait_for` 外不执行阻塞等待。
/// - 不在 worker_waker 锁内执行任务 poll、用户 future、I/O 或回调。
///
/// 安全性：
/// - 不引入新的 unsafe。
/// - 线程安全：依赖 `ArrayQueue`、`AtomicBool` 和 `Mutex/Condvar` 的组合协议。
/// - 内存安全：waits 中保存的是 worker_waker 的 Arc，生命周期由 runtime/worker 持有；
///   stale entry 被弹出后自然释放引用。
/// - 异步安全：只调度任务，不在锁内 poll future，不跨 await 持有锁。
/// - 运行时依赖：要求同一个 runtime 的所有 spawn/wake 路径在任务入队后调用
///   `wake_waiting_worker`。
#[inline]
fn worker_wait_for_task<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>(
    runtime: &MultiTaskRuntime<O, P>,
    worker_waker: &Arc<(AtomicBool, Mutex<()>, Condvar)>,
    sleep_timeout: u64,
) -> WorkerWaitResult<O, P> {
    let (is_sleep, lock, condvar) = &**worker_waker;

    loop {
        let _locked = lock.lock();
        if is_sleep.load(Ordering::Acquire) {
            break;
        }

        if register_waiting_worker(&(runtime.0).4, worker_waker) {
            is_sleep.store(true, Ordering::Release);
            break;
        }

        drop(_locked);
        if prune_stale_waiting_workers(&(runtime.0).4) == 0 {
            return WorkerWaitResult::NotSlept;
        }
    }

    if let Some(task) = (runtime.0).1.try_pop() {
        is_sleep.store(false, Ordering::Release);
        return WorkerWaitResult::Task(task);
    }

    if runtime.len() > 0 {
        is_sleep.store(false, Ordering::Release);
        return WorkerWaitResult::NotSlept;
    }

    let mut locked = lock.lock();
    if !is_sleep.load(Ordering::Acquire) {
        return WorkerWaitResult::NotSlept;
    }

    let timed_out = condvar
        .wait_for(&mut locked, Duration::from_millis(sleep_timeout))
        .timed_out();
    is_sleep.store(false, Ordering::Release);

    if timed_out {
        WorkerWaitResult::TimedOut
    } else {
        WorkerWaitResult::NotSlept
    }
}

//线程工作循环
fn timer_work_loop<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>(
    runtime: MultiTaskRuntime<O, P>,
    index: usize,
    min: usize,
    sleep_timeout: u64,
    timer_interval: u64,
    timer: Arc<AsyncTaskTimerByNotCancel<P, O>>,
) {
    //初始化当前线程的线程id和线程活动状态
    let pool = (runtime.0).1.clone();
    let worker_waker = pool.clone_thread_waker().unwrap();

    let mut sleep_count = 0; //连续休眠计数器
    let clock = Clock::new();
    loop {
        //设置新的定时异步任务，并唤醒已到期的定时异步任务
        let timer_run_millis = clock.recent(); //重置定时器运行时长
        let mut pop_len = 0;
        (runtime.0)
            .5
            .fetch_add(timer.consume(),
                       Ordering::Relaxed);
        loop {
            let current_time = timer.is_require_pop();
            if let Some(current_time) = current_time {
                //当前有到期的定时异步任务，则开始处理到期的所有定时异步任务
                loop {
                    let timed_out = timer.pop(current_time);
                    if let Some(timing_task) = timed_out {
                        match timing_task {
                            AsyncTimingTask::Pended(expired) => {
                                //唤醒休眠的异步任务，不需要立即在本工作者中执行，因为休眠的异步任务无法取消
                                runtime.wakeup::<O>(&expired);
                            }
                            AsyncTimingTask::WaitRun(expired) => {
                                //执行到期的定时异步任务，需要立即在本工作者中执行，因为定时异步任务可以取消
                                (runtime.0)
                                    .1
                                    .push_priority(DEFAULT_MAX_HIGH_PRIORITY_BOUNDED,
                                                   expired);
                                if let Some(task) = pool.try_pop() {
                                    sleep_count = 0; //重置连续休眠次数
                                    run_task(&runtime, task);
                                }
                            }
                            AsyncTimingTask::TimeoutWake(waiter) => {
                                //唤醒等待timeout到期的任务
                                waiter.fire();
                            }
                        }
                        pop_len += 1;

                        if let Some(task) = pool.try_pop() {
                            //执行当前工作者任务池中的异步任务，避免定时异步任务占用当前工作者的所有工作时间
                            sleep_count = 0; //重置连续休眠次数
                            run_task(&runtime, task);
                        }
                    } else {
                        //当前所有的到期任务已处理完，则退出本次定时异步任务处理
                        break;
                    }
                }
            } else {
                //当前没有到期的定时异步任务，则退出本次定时异步任务处理
                break;
            }
        }
        (runtime.0)
            .6
            .fetch_add(pop_len,
                       Ordering::Relaxed);

        //继续执行当前工作者任务池中的异步任务
        match pool.try_pop() {
            None => {
                if runtime.len() > 0 {
                    //确认当前还有任务需要处理，可能还没分配到当前工作者，则当前工作者继续工作
                    continue;
                }

                //获取休眠的实际时长
                let diff_time = clock
                    .recent()
                    .duration_since(timer_run_millis)
                    .as_millis() as u64; //获取定时器运行时长
                let real_timeout = if timer.len() == 0 {
                    //当前定时器没有未到期的任务，则休眠指定时长
                    sleep_timeout
                } else {
                    //当前定时器还有未到期的任务，则计算需要休眠的时长
                    if diff_time >= timer_interval {
                        //定时器内部时间与当前时间差距过大，则忽略休眠，并继续工作
                        continue;
                    } else {
                        //定时器内部时间与当前时间差距不大，则休眠差值时间
                        timer_interval - diff_time
                    }
                };

                //无任务，则准备休眠
                match worker_wait_for_task(&runtime, &worker_waker, real_timeout) {
                    WorkerWaitResult::TimedOut => {
                        //记录连续休眠次数，因为任务导致的唤醒不会计数
                        sleep_count += 1;
                    },
                    WorkerWaitResult::Task(task) => {
                        sleep_count = 0; //重置连续休眠次数
                        run_task(&runtime, task);
                    },
                    WorkerWaitResult::NotSlept => (),
                }
            }
            Some(task) => {
                //有任务，则执行
                sleep_count = 0; //重置连续休眠次数
                run_task(&runtime, task);
            }
        }
    }

    //关闭当前工作者的任务池
    (runtime.0).1.close_worker();
    warn!(
        "Worker of runtime closed, runtime: {}, worker: {}, thread: {:?}",
        runtime.get_id(),
        index,
        thread::current()
    );
}

//线程工作循环
fn work_loop<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>(
    runtime: MultiTaskRuntime<O, P>,
    index: usize,
    min: usize,
    sleep_timeout: u64,
) {
    //初始化当前线程的线程id和线程活动状态
    let pool = (runtime.0).1.clone();
    let worker_waker = pool.clone_thread_waker().unwrap();

    let mut sleep_count = 0; //连续休眠计数器
    loop {
        match pool.try_pop() {
            None => {
                //无任务，则准备休眠
                if runtime.len() > 0 {
                    //确认当前还有任务需要处理，可能还没分配到当前工作者，则当前工作者继续工作
                    continue;
                }

                match worker_wait_for_task(&runtime, &worker_waker, sleep_timeout) {
                    WorkerWaitResult::TimedOut => {
                        //记录连续休眠次数，因为任务导致的唤醒不会计数
                        sleep_count += 1;
                    },
                    WorkerWaitResult::Task(task) => {
                        sleep_count = 0; //重置连续休眠次数
                        run_task(&runtime, task);
                    },
                    WorkerWaitResult::NotSlept => (),
                }
            }
            Some(task) => {
                //有任务，则执行
                sleep_count = 0; //重置连续休眠次数
                run_task(&runtime, task);
            }
        }
    }

    //关闭当前工作者的任务池
    (runtime.0).1.close_worker();
    warn!(
        "Worker of runtime closed, runtime: {}, worker: {}, thread: {:?}",
        runtime.get_id(),
        index,
        thread::current()
    );
}

/// 对从多线程运行时任务池弹出的一个任务执行轮询。
///
/// 托管任务先原子认领其唯一已调度轮询义务。陈旧或已完成的队列引用会直接释放，
/// 因而不会再进入原来的 `None -> push -> pop` 活锁。`RUNNING` 期间的唤醒会被合并；
/// 返回 `Pending` 后先恢复 Future，再发布状态并生成一个延期队列项。`Ready` 和栈展开
/// 都会发布终态。
///
/// 通过公开 `AsyncTask::get_inner/set_inner` 取出的兼容手工任务保留原手工驱动行为，
/// 包括未知外部驱动可能依赖的历史临时 None 重排逻辑。
///
/// 每次托管轮询的成本为 O(1)，另加 `Future::poll` 和可选的一次任务池入队。状态转换
/// 无锁；Future 互斥锁只覆盖取出/恢复，绝不与用户代码、任务池访问或工作线程通知
/// 重叠。函数消费一个物理队列 `Arc` 并返回 `()`。它可能推进/释放 Future、入队一次
/// 后续任务并通知一个工作线程，因此非纯且非幂等。状态处理自身不新增分配；可选入队
/// 继续服从任务池既有的容量和扩容行为。函数不执行 I/O/FFI，并保持 Future/context
/// 的原析构线程和既有 V8 所有者线程边界。
#[inline]
fn run_task<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>(
    runtime: &MultiTaskRuntime<O, P>,
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
            } else {
                // 保留公开手工驱动既有的重试行为。
                (runtime.0).1.push(task);
            }
            return;
        },
        AsyncTaskPollClaim::Managed => (),
    }

    // 守卫必须先于局部 Future 声明，使栈展开时先析构 Future；`Future::drop` 发出的
    // 唤醒随后会被守卫发布的完成状态吸收。
    let guard = AsyncTaskPollGuard::new(&task);
    let waker = waker_ref(&task);
    let mut context = Context::from_waker(&*waker);
    let mut future = match task.take_inner_for_runtime_poll() {
        Some(future) => future,
        None => {
            // 已成功认领却没有 Future 的托管任务无效或陈旧，必须进入终态；重新入队会
            // 再次产生生产环境中的活锁。
            guard.finish_ready();
            return;
        },
    };

    match future.as_mut().poll(&mut context) {
        Poll::Pending => {
            task.restore_inner_after_runtime_poll(future);
            if guard.finish_pending() {
                requeue_runtime_task((runtime.0).1.as_ref(), &task);
            }
        },
        Poll::Ready(_) => guard.finish_ready(),
    }
}

// 定时器任务生产者
enum TimerTaskProducor<
    O: Default + 'static = (),
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O> = StealableTaskPool<O>,
> {
    Local(Arc<AsyncTaskTimerByNotCancel<P, O>>),        //本地定时器任务生产者
    Foreign(Sender<(usize, AsyncTimingTask<P, O>)>),    //外部定时器任务生产者
}
