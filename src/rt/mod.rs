//! # 提供了通用的异步运行时
//!

use std::thread;
use std::pin::Pin;
use std::sync::Arc;
use std::ptr::null_mut;
use std::vec::IntoIter;
use std::future::Future;
use std::panic::set_hook;
use std::any::{Any, TypeId};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::cell::{RefCell, UnsafeCell};
use std::task::{Waker, Context, Poll};
use std::time::{Duration, SystemTime};
use std::io::{Error, Result, ErrorKind};
use std::alloc::{Layout, set_alloc_error_hook};
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, AtomicPtr, Ordering};

pub mod single_thread;
mod single_task_owner;
pub mod multi_thread;
pub mod worker_thread;
pub mod serial;
pub mod serial_local_thread;
pub mod serial_single_thread;
pub mod serial_worker_thread;
pub mod serial_local_compatible_wasm_runtime;

use libc;
use futures::{future::{FutureExt, BoxFuture},
              stream::{Stream, BoxStream},
              task::{ArcWake, AtomicWaker}};
use parking_lot::{Mutex, Condvar};
use crossbeam_channel::{Sender, Receiver, unbounded};
use crossbeam_queue::ArrayQueue;
use crossbeam_utils::atomic::AtomicCell;
use flume::{Sender as AsyncSender, Receiver as AsyncReceiver};
use num_cpus;
use backtrace::Backtrace;
use slotmap::{Key, KeyData};
use quanta::{Clock, Upkeep, Handle, Instant as QInstant};

use pi_hash::XHashMap;
use pi_cancel_timer::Timer;
use pi_timer::Timer as NotCancelTimer;

use single_thread::SingleTaskRuntime;
use worker_thread::{WorkerTaskRunner, WorkerRuntime};
use multi_thread::{MultiTaskRuntimeBuilder, MultiTaskRuntime, StealableTaskPool};

use crate::lock::spin;

/*
* 本地线程绑定的异步运行时
*/
thread_local! {
    static PI_ASYNC_LOCAL_THREAD_ASYNC_RUNTIME: AtomicPtr<()> = AtomicPtr::new(null_mut());
    static PI_ASYNC_LOCAL_THREAD_ASYNC_RUNTIME_DICT: UnsafeCell<XHashMap<TypeId, Box<dyn Any + 'static>>> = UnsafeCell::new(XHashMap::default());
}

/*
* 本地线程唯一id
*/
thread_local! {
    static PI_ASYNC_THREAD_LOCAL_ID: UnsafeCell<usize> = UnsafeCell::new(usize::MAX);
}

/*
* 默认的最高优先级边界
*/
const DEFAULT_MAX_HIGH_PRIORITY_BOUNDED: usize = 10;

/*
* 默认的高优先级边界
*/
const DEFAULT_HIGH_PRIORITY_BOUNDED: usize = 5;

/*
* 默认的最低优先级
*/
const DEFAULT_MAX_LOW_PRIORITY_BOUNDED: usize = 0;

/*
* 异步运行时唯一id生成器
*/
static RUNTIME_UID_GEN: AtomicUsize = AtomicUsize::new(1);

/*
* 全局时间状态
*/
static GLOBAL_TIME_LOOP_STATUS: AtomicBool = AtomicBool::new(false);

///
/// 启动全局时间循环，成功则返回句柄，释放句柄将关闭全局时间循环，失败表示已启动，则返回空
/// 更新间隔时长为毫秒
///
pub fn startup_global_time_loop(interval: u64) -> Option<GlobalTimeLoopHandle> {
    if let Err(_) = GLOBAL_TIME_LOOP_STATUS.compare_exchange(false,
                                                             true,
                                                             Ordering::AcqRel,
                                                             Ordering::Relaxed) {
        //已启动
        None
    } else {
        //未启动
        let timer = Upkeep::new_with_clock(Duration::from_millis(interval), Clock::new());
        let handle = timer.start().unwrap();
        let clock = Clock::new();
        let _now = clock.recent();

        Some(GlobalTimeLoopHandle(handle))
    }
}

///
/// 全局时间循环句柄
///
pub struct GlobalTimeLoopHandle(Handle);

impl Drop for GlobalTimeLoopHandle {
    fn drop(&mut self) {
        GLOBAL_TIME_LOOP_STATUS.store(false, Ordering::Release);
    }
}

///
/// 分配异步运行时唯一id
///
pub fn alloc_rt_uid() -> usize {
    RUNTIME_UID_GEN.fetch_add(1, Ordering::Relaxed)
}

///
/// 异步任务唯一id
///
pub struct TaskId(UnsafeCell<u128>);

impl Debug for TaskId {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "TaskId[inner = {}]", unsafe { *self.0.get() })
    }
}

impl Clone for TaskId {
    fn clone(&self) -> Self {
        unsafe {
            TaskId(UnsafeCell::new(*self.0.get()))
        }
    }
}

impl TaskId {
    /// 线程安全的判断异步任务唯一id对应的异步任务的唤醒器是否存在
    #[inline]
    pub fn exist_waker<R: 'static>(&self) -> bool {
        unsafe {
            let handle = unsafe { TaskHandle::<R>::from_raw((*self.0.get() >> 64) as *const ()) };
            let inner = &*handle.0;
            let r = if let Some(waker) = inner.0.swap(None) {
                inner.0.swap(Some(waker));
                true
            } else {
                false
            };

            //避免提前释放
            handle.into_raw();

            r
        }
    }

    /// 线程安全的唤醒异步任务唯一id对应的异步任务
    #[inline]
    pub fn wakeup<R: 'static>(&self) {
        unsafe {
            let handle = unsafe { TaskHandle::<R>::from_raw((*self.0.get() >> 64) as *const ()) };
            let inner = &*handle.0;
            if let Some(waker) = inner.0.swap(None) {
                //当前异步任务的唤醒器存在，则唤醒
                waker.wake();
            }

            //避免提前释放
            handle.into_raw();
        }
    }

    /// 线程安全的为异步任务唯一id对应的异步任务设置唤醒器
    #[inline]
    pub fn set_waker<R: 'static>(&self, waker: Waker) -> Option<Waker> {
        unsafe {
            let handle = unsafe { TaskHandle::<R>::from_raw((*self.0.get() >> 64) as *const ()) };
            let inner = &*handle.0;
            let r = inner.0.swap(Some(waker));

            //避免提前释放
            handle.into_raw();

            r
        }
    }

    /// 线程安全的获取异步任务唯一id对应的异步任务的返回值
    #[inline]
    pub fn result<R: 'static>(&self) -> Option<R> {
        unsafe {
            let handle = unsafe { TaskHandle::<R>::from_raw((*self.0.get() >> 64) as *const ()) };
            let inner = &*handle.0;
            let r = inner.1.swap(None);

            //避免提前释放
            handle.into_raw();

            r
        }
    }

    /// 线程安全的为异步任务唯一id对应的异步任务设置返回值
    #[inline]
    pub fn set_result<R: 'static>(&self, result: R) -> Option<R> {
        unsafe {
            let handle = unsafe { TaskHandle::<R>::from_raw((*self.0.get() >> 64) as *const ()) };
            let inner = &*handle.0;
            let r = inner.1.swap(Some(result));

            //避免提前释放
            handle.into_raw();

            r
        }
    }
}

// 异步任务句柄
pub(crate) struct TaskHandle<R: 'static>(Box<(
    AtomicCell<Option<Waker>>,  //任务唤醒器
    AtomicCell<Option<R>>,      //任务返回值
)>);

impl<R: 'static> Default for TaskHandle<R> {
    fn default() -> Self {
        TaskHandle(Box::new((AtomicCell::new(None), AtomicCell::new(None))))
    }
}

impl<R: 'static> TaskHandle<R> {
    /// 将祼指针转换为异步任务句柄
    pub unsafe fn from_raw(raw: *const ()) -> TaskHandle<R> {
        let inner
            = Box::from_raw(raw as *const (AtomicCell<Option<Waker>>, AtomicCell<Option<R>>) as *mut (AtomicCell<Option<Waker>>, AtomicCell<Option<R>>));
        TaskHandle(inner)
    }

    /// 将异步任务句柄转换为祼指针
    pub fn into_raw(self) -> *const () {
        Box::into_raw(self.0)
            as *mut (AtomicCell<Option<Waker>>, AtomicCell<Option<R>>)
            as *const (AtomicCell<Option<Waker>>, AtomicCell<Option<R>>)
            as *const ()
    }
}

/// timeout专用等待句柄
pub(crate) struct TimeoutWaiter {
    fired: AtomicBool,
    waker: AtomicWaker,
}

impl TimeoutWaiter {
    #[inline]
    pub fn new() -> Self {
        TimeoutWaiter {
            fired: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        }
    }

    #[inline]
    pub fn is_fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }

    #[inline]
    pub fn register(&self, waker: &Waker) {
        self.waker.register(waker);
    }

    #[inline]
    pub fn fire(&self) {
        if !self.fired.swap(true, Ordering::AcqRel) {
            self.waker.wake();
        }
    }

    #[inline]
    pub fn clear_waker(&self) {
        let _ = self.waker.take();
    }
}

/// 唤醒一个已经从等待队列中取出的工作者线程唤醒器。
///
/// 说明：
/// - 该 helper 只用于“调用方已经确认这个 `worker_waker` 来自等待队列”的场景。
/// - 它会先获取 `worker_waker` 内部的互斥锁，再检查并切换 `is_sleep`。
/// - 锁内检查是为了覆盖 worker 进入休眠时的发布窗口：worker 先把唤醒器放入
///   waits 队列，再在同一把锁保护下发布 `is_sleep = true`，外部唤醒端必须等这个
///   发布动作完成后再判断是否 notify。
///
/// 参数：
/// - `worker_waker`：工作者线程的 `(is_sleep, lock, condvar)` 三元组。
///
/// 返回：
/// - `true`：本次成功把 `is_sleep` 从 `true` 切为 `false`，并调用了 `notify_one()`。
/// - `false`：该唤醒器已经失效、已被其它唤醒者消费，或 worker 已经自行取消休眠。
///
/// 边界与业务范围：
/// - 不创建任务、不修改任务队列，不负责判断 runtime 中是否已有任务。
/// - 只唤醒一个已注册的 worker，不广播，不循环 notify，不负责 worker 选择策略。
/// - 如果 worker 在被 notify 前已经通过二次检查取到任务并取消休眠，本函数会返回
///   `false`，这是正确的无操作。
///
/// 性能：
/// - 时间复杂度 O(1)，空间复杂度 O(1)，不分配内存，不 clone。
/// - 可能短暂获取 parking_lot mutex；不在 poll future 的内部持锁等待，也不会执行
///   condvar wait。
///
/// 纯度与副作用：
/// - 非纯函数。副作用是原子状态切换和一次条件变量通知。
/// - 对同一个已注册唤醒器重复调用是幂等收敛的：最多一次调用能从 `true` 切到
///   `false` 并 notify。
///
/// 安全性：
/// - 不使用 unsafe。
/// - 线程安全：依赖 `AtomicBool` 的 Acquire/AcqRel 可见性和 `Mutex` 对休眠发布窗口
///   的互斥保护。
/// - 异步安全：不会阻塞 executor worker 的异步任务 poll；只在外部唤醒或 spawn
///   入队后的线程级唤醒路径上短暂执行。
/// - 运行时依赖：要求传入的 `worker_waker` 与对应 worker 的 condvar wait 使用同一把
///   lock。
#[inline]
pub(crate) fn wake_registered_thread_waker(worker_waker: &Arc<(AtomicBool, Mutex<()>, Condvar)>) -> bool {
    let (is_sleep, lock, condvar) = &**worker_waker;
    let _locked = lock.lock();
    if is_sleep
        .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        condvar.notify_one();
        return true;
    }

    false
}

/// 快速唤醒单个线程唤醒器。
///
/// 说明：
/// - 该 helper 用于没有 waits 队列的单 worker / 单线程运行时唤醒路径。
/// - 与 `wake_registered_thread_waker` 相比，它先用一次 Acquire load 做快速过滤；
///   当 `is_sleep == false` 时不获取锁。
///
/// 使用指导：
/// - waits 队列中弹出的条目必须使用 `wake_registered_thread_waker`，因为队列条目可能
///   处于“已入队但尚未发布 `is_sleep = true`”的临界窗口。
/// - 直接持有线程唤醒器、且没有队列发布窗口时，可以使用本函数。
///
/// 参数与返回：
/// - 参数同 `wake_registered_thread_waker`。
/// - 返回 `true` 表示实际 notify 了一次；返回 `false` 表示无需唤醒或已被消费。
///
/// 性能与副作用：
/// - 常见无休眠路径 O(1) 且无锁；需要唤醒时 O(1) 并短暂持锁。
/// - 不分配内存，不 clone，不广播，不会形成唤醒风暴。
///
/// 安全性：
/// - 不使用 unsafe。
/// - 线程安全、内存安全；依赖同一 `worker_waker` 被 worker wait 和 wake 端共享。
#[inline]
pub(crate) fn wake_thread_waker(worker_waker: &Arc<(AtomicBool, Mutex<()>, Condvar)>) -> bool {
    if !worker_waker.0.load(Ordering::Acquire) {
        return false;
    }

    wake_registered_thread_waker(worker_waker)
}

/// 从多线程运行时的等待队列中唤醒一个可唤醒 worker。
///
/// 说明：
/// - waits 是一个有界队列，队列项是 worker 注册的线程唤醒器。
/// - 本函数每次最多成功唤醒一个 worker；遇到已经失效的陈旧项会丢弃并继续扫描。
/// - 该行为用于避免漏唤醒，同时避免对所有 worker 广播造成唤醒风暴。
///
/// 使用指导：
/// - 任务被外部线程入队后调用，例如 `spawn_by_id`、`spawn_priority_by_id` 和
///   `AsyncTask::wake_by_ref`。
/// - 调用方不应在持有任务队列内部锁时调用；当前任务池入队 API 本身不暴露需要
///   调用方持有的锁。
///
/// 参数：
/// - `waits`：当前 runtime 共享的 sleeping worker 等待队列。
///
/// 返回：
/// - `true`：成功唤醒了一个仍处于休眠发布状态的 worker。
/// - `false`：队列为空，或扫描到的条目均已失效。
///
/// 边界条件：
/// - 队列容量等于 runtime 最大 worker 数。扫描上限固定为 `capacity`，不会无限循环。
/// - 并发唤醒同一个队列项时，只有一个调用者能 CAS 成功并 notify。
/// - 陈旧项来自 worker timeout 或二次检查取消休眠；丢弃它们不会丢任务，因为任务
///   已经在任务队列中，或 worker 已经自行继续轮询。
///
/// 性能：
/// - 最坏时间复杂度 O(W)，W 为 waits 容量，即最大 worker 数；常见路径接近 O(1)。
/// - 空间复杂度 O(1)，不分配内存，不 clone。
/// - 每次调用最多一次 notify，避免唤醒风暴。
///
/// 纯度与副作用：
/// - 非纯函数。会从 waits 队列弹出条目，可能切换 worker 休眠状态并 notify。
/// - 对同一批陈旧项重复调用是幂等收敛的：陈旧项会被逐步清理。
///
/// 安全性：
/// - 不使用 unsafe。
/// - 线程安全：ArrayQueue 提供并发队列安全；worker 状态由原子和 mutex 保护。
/// - 异步安全：不会执行 condvar wait，不会阻塞当前异步任务，只在调度唤醒路径短暂
///   执行。
#[inline]
pub(crate) fn wake_waiting_worker(
    waits: &ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>,
) -> bool {
    let scan_len = waits.capacity();
    for _ in 0..scan_len {
        match waits.pop() {
            Some(worker_waker) => {
                if wake_registered_thread_waker(&worker_waker) {
                    return true;
                }
            },
            None => {
                return false;
            },
        }
    }

    false
}

/// 清理 waits 队列中的陈旧 worker 唤醒器。
///
/// 说明：
/// - worker 可能因为 sleep timeout、二次检查发现任务、或被其它唤醒者消费而把
///   `is_sleep` 清为 `false`，但旧队列项仍留在有界 waits 队列中。
/// - 本函数用于注册新 sleep 前释放这些陈旧槽位，避免 waits 被 stale entry 填满后
///   worker 只能忙等。
///
/// 使用指导：
/// - 只在 `register_waiting_worker` 遇到队列满时调用。
/// - 不作为常规 wake 路径使用，避免在热唤醒路径上做额外扫描。
///
/// 参数与返回：
/// - `waits`：当前 runtime 的 waiting worker 队列。
/// - 返回实际移除的陈旧项数量。
///
/// 边界条件：
/// - 函数只扫描调用开始时观察到的 `waits.len()` 个条目，不无限循环。
/// - 每个弹出的条目都会先短暂获取该 worker 的锁再判断 `is_sleep`，避免把“已入队但
///   尚未发布 `is_sleep = true`”的注册窗口误判为 stale。
/// - 仍为 `true` 的 live 条目会放回队列；如果并发竞争导致放回失败，则立即尝试唤醒
///   该 live worker，避免丢失一个真实 sleeping worker 的唤醒入口。
///
/// 性能：
/// - 最坏时间复杂度 O(N)，N 为调用开始时的队列长度，N <= worker 上限。
/// - 空间复杂度 O(1)，不分配内存；live 条目放回时复用已弹出的 Arc，不额外 clone。
///
/// 纯度与副作用：
/// - 非纯函数。会重排 waits 队列中的 live 条目，移除 stale 条目，极端竞争下可能
///   notify 一个 live worker。
/// - 幂等：重复调用会逐步收敛到没有 stale entry。
///
/// 安全性：
/// - 不使用 unsafe。
/// - 线程安全；依赖 ArrayQueue、AtomicBool 和 worker_waker mutex。
#[inline]
pub(crate) fn prune_stale_waiting_workers(
    waits: &ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>,
) -> usize {
    let scan_len = waits.len();
    let mut pruned = 0;

    for _ in 0..scan_len {
        let Some(worker_waker) = waits.pop() else {
            break;
        };

        let is_live = {
            let _locked = worker_waker.1.lock();
            worker_waker.0.load(Ordering::Acquire)
        };

        if is_live {
            match waits.push(worker_waker) {
                Ok(()) => (),
                Err(worker_waker) => {
                    let _ = wake_registered_thread_waker(&worker_waker);
                },
            }
        } else {
            pruned += 1;
        }
    }

    pruned
}

/// 将当前 worker 注册为可被外部任务入队唤醒的候选 worker。
///
/// 说明：
/// - 本函数只负责把 `worker_waker` 放入 waits 队列，不负责把 `is_sleep` 置为 true。
/// - 调用方必须在持有 `worker_waker` 内部 mutex 的情况下调用成功快路径，并在成功入队
///   后、同一把锁释放前发布 `is_sleep = true`。这样外部唤醒端弹出队列项后会在锁上等待
///   发布完成，不会出现“队列中已有条目但状态尚未可唤醒”的漏唤醒窗口。
/// - 队列满时调用方应释放当前 worker 锁后再调用 `prune_stale_waiting_workers`，避免当前
///   worker 锁与其它 worker 唤醒锁形成嵌套临界区。
///
/// 参数：
/// - `waits`：当前 runtime 的 waiting worker 队列。
/// - `worker_waker`：当前 worker 的线程唤醒器。
///
/// 返回：
/// - `true`：注册成功；调用方可以继续发布 `is_sleep = true` 并进入二次检查/等待。
/// - `false`：队列满；调用方不得进入 condvar wait，应先释放锁并尝试清理 stale，或
///   继续 poll loop，避免无唤醒入口地睡眠。
///
/// 边界条件：
/// - 若队列全是 live worker，返回 `false` 是允许的：已有其它 worker 可被唤醒，当前
///   worker 继续循环即可。
///
/// 性能：
/// - 成功快路径 O(1)，一次 Arc clone 用于把 worker 唤醒器登记到队列。
/// - 队列满时 O(1) 返回，不在该 helper 内扫描队列。
///
/// 纯度与副作用：
/// - 非纯函数。会向 waits 入队。
/// - 非幂等：重复成功调用会重复登记同一个 worker，因此必须由调用方的 `is_sleep`
///   状态保证同一 worker 同一休眠周期只注册一次。
///
/// 安全性：
/// - 不使用 unsafe。
/// - 线程安全；要求调用方遵守“持锁注册，锁内发布 true”的协议。
#[inline]
pub(crate) fn register_waiting_worker(
    waits: &ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>,
    worker_waker: &Arc<(AtomicBool, Mutex<()>, Condvar)>,
) -> bool {
    waits.push(worker_waker.clone()).is_ok()
}

#[cfg(test)]
mod timeout_waiter_tests {
    use super::{
        prune_stale_waiting_workers, register_waiting_worker, wake_thread_waker,
        wake_waiting_worker, TimeoutWaiter,
    };
    use crossbeam_queue::ArrayQueue;
    use futures::task::{waker_ref, ArcWake};
    use parking_lot::{Condvar, Mutex};
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };

    struct WakeCounter(AtomicUsize);

    impl ArcWake for WakeCounter {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_timeout_waiter_fire_wakes_once() {
        let waiter = TimeoutWaiter::new();
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = waker_ref(&counter);

        waiter.register(&waker);
        waiter.fire();
        waiter.fire();

        assert!(waiter.is_fired());
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_timeout_waiter_clear_waker_before_fire() {
        let waiter = TimeoutWaiter::new();
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = waker_ref(&counter);

        waiter.register(&waker);
        waiter.clear_waker();
        waiter.fire();

        assert!(waiter.is_fired());
        assert_eq!(counter.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_timeout_waiter_replaces_waker() {
        let waiter = TimeoutWaiter::new();
        let old_counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let new_counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let old_waker = waker_ref(&old_counter);
        let new_waker = waker_ref(&new_counter);

        waiter.register(&old_waker);
        waiter.register(&new_waker);
        waiter.fire();

        assert!(waiter.is_fired());
        assert_eq!(old_counter.0.load(Ordering::SeqCst), 0);
        assert_eq!(new_counter.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_worker_waker_wakes_once() {
        let worker_waker = Arc::new((AtomicBool::new(true), Mutex::new(()), Condvar::new()));

        assert!(wake_thread_waker(&worker_waker));
        assert!(!worker_waker.0.load(Ordering::SeqCst));
        assert!(!wake_thread_waker(&worker_waker));
    }

    #[test]
    fn test_worker_waker_wait_queue_skips_stale_and_wakes_one_sleeping_worker() {
        let waits = ArrayQueue::new(4);
        let stale = Arc::new((AtomicBool::new(false), Mutex::new(()), Condvar::new()));
        let sleeping = Arc::new((AtomicBool::new(true), Mutex::new(()), Condvar::new()));

        waits.push(stale).unwrap();
        waits.push(sleeping.clone()).unwrap();

        assert!(wake_waiting_worker(&waits));
        assert!(!sleeping.0.load(Ordering::SeqCst));
        assert!(!wake_waiting_worker(&waits));
    }

    #[test]
    fn test_worker_waker_register_and_prune_stale_waiter() {
        let waits = ArrayQueue::new(1);
        let stale = Arc::new((AtomicBool::new(false), Mutex::new(()), Condvar::new()));
        let current = Arc::new((AtomicBool::new(false), Mutex::new(()), Condvar::new()));

        waits.push(stale).unwrap();
        assert!(!register_waiting_worker(&waits, &current));
        assert_eq!(prune_stale_waiting_workers(&waits), 1);
        assert!(register_waiting_worker(&waits, &current));
    }

    #[test]
    fn test_worker_waker_prune_keeps_live_waiter() {
        let waits = ArrayQueue::new(1);
        let sleeping = Arc::new((AtomicBool::new(true), Mutex::new(()), Condvar::new()));

        waits.push(sleeping.clone()).unwrap();
        assert_eq!(prune_stale_waiting_workers(&waits), 0);
        assert!(wake_waiting_worker(&waits));
        assert!(!sleeping.0.load(Ordering::SeqCst));
    }
}

/*
* 运行时托管的 AsyncTask 调度状态。
*
* MANAGED 区分由本库单线程/多线程运行时驱动的任务，以及通过公开
* get_inner/set_inner 接口交给外部手工驱动的任务。SCHEDULED 表示一个尚未履行的
* 轮询义务，RUNNING 表示独占轮询所有权，COMPLETED 表示终态。
*/
const ASYNC_TASK_STATE_SCHEDULED: u8 = 0b0000_0001;
const ASYNC_TASK_STATE_RUNNING: u8 = 0b0000_0010;
const ASYNC_TASK_STATE_COMPLETED: u8 = 0b0000_0100;
const ASYNC_TASK_STATE_MANAGED: u8 = 0b1000_0000;
const ASYNC_TASK_STATE_INITIAL: u8 =
    ASYNC_TASK_STATE_MANAGED | ASYNC_TASK_STATE_SCHEDULED;

/// 运行时尝试认领一个已出队任务进行轮询时的结果。
///
/// 只有匹配的运行时驱动可以推进托管任务，因此本枚举仅在库内可见。`Legacy` 保留公开
/// 手工驱动路径，`Managed` 授予独占轮询所有权，`Discard` 表示陈旧或已完成的队列
/// 引用。生成本结果的时间复杂度为 O(1)，不阻塞、不分配、线程安全，也不会访问或
/// 轮询用户 Future。
pub(crate) enum AsyncTaskPollClaim {
    Legacy,
    Managed,
    Discard,
}

enum AsyncTaskWakeAction {
    LegacyEnqueue,
    ManagedEnqueue,
    Coalesced,
}

/// 单次托管 `Future::poll` 的异常安全所有者。
///
/// 本守卫不捕获或压制异常。若驱动在记录 `Pending` 或 `Ready` 前发生栈展开，
/// `Drop` 会发布已完成终态，防止仍被保留或陈旧的唤醒器让任务永久停留在 `RUNNING`。
/// 它不持锁、不分配、不阻塞、不唤醒工作线程，也不调用用户代码。创建、正常完成和
/// 栈展开清理均为 O(1)。它会推进任务状态，因此不是纯函数，也不幂等。
pub(crate) struct AsyncTaskPollGuard<
    'a,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
    O: Default + 'static = (),
> {
    task:  &'a AsyncTask<P, O>,
    armed: bool,
}

impl<
    'a,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
    O: Default + 'static,
> AsyncTaskPollGuard<'a, P, O> {
    /// 在 `try_begin_runtime_poll` 返回 `Managed` 后创建已启用的守卫。
    #[inline]
    pub(crate) fn new(task: &'a AsyncTask<P, O>) -> Self {
        AsyncTaskPollGuard {
            task,
            armed: true,
        }
    }

    /// 在 Future 已恢复到任务槽位后完成一次返回 `Pending` 的轮询。
    ///
    /// 仅当轮询期间发生过唤醒、必须生成一个后续队列项时返回 `true`。调用方必须在
    /// 释放其任务 `Arc` 前完成入队。时间复杂度 O(1)，无锁、无分配且不阻塞。
    #[inline]
    pub(crate) fn finish_pending(mut self) -> bool {
        let should_enqueue = self.task.finish_runtime_poll_pending();
        self.armed = false;
        should_enqueue
    }

    /// 为成功完成的 Future 发布终态。
    ///
    /// 调用后，迟到或陈旧的唤醒均为空操作。时间复杂度 O(1)，不分配、不阻塞；
    /// 不访问 Future、任务池、工作线程锁、回调、I/O 或 FFI。
    #[inline]
    pub(crate) fn finish_ready(mut self) {
        self.task.finish_runtime_poll_ready();
        self.armed = false;
    }
}

impl<
    'a,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
    O: Default + 'static,
> Drop for AsyncTaskPollGuard<'a, P, O> {
    fn drop(&mut self) {
        if self.armed {
            self.task.finish_runtime_poll_ready();
        }
    }
}

/// 异步任务及其运行时调度/生命周期状态。
///
/// # 运行时契约
///
/// 运行时托管任务是一次性的：其 Future 被持续轮询，首次返回 `Ready` 后永久完成。
/// 运行时任务通过私有原子状态合并重复唤醒、排除并发轮询，并拒绝迟到唤醒。公开
/// `get_inner`/`set_inner` 选择旧手工驱动契约，包括显式的低层替换/复用能力，因此
/// 既有自定义驱动无需新增特征方法。
///
/// 构造函数只创建一个逻辑上的首次轮询义务，并不执行物理入队。运行时或自定义任务池
/// 必须通过既有 `push*` 接口把新任务准确入队一次。`Waker` 用于在首次提交后安排后续
/// 轮询，不能替代首次入队。
///
/// # 示例
///
/// ```
/// use std::sync::Arc;
/// use futures::FutureExt;
/// use pi_async_rt::rt::{
///     AsyncRuntime, AsyncTask, AsyncTaskPool,
///     single_thread::SingleTaskRunner,
/// };
///
/// let runner = SingleTaskRunner::<()>::default();
/// let runtime = runner.startup().unwrap();
/// let task = Arc::new(AsyncTask::new(
///     runtime.alloc::<()>(),
///     runtime.shared_pool(),
///     0,
///     Some(async {}.boxed()),
/// ));
/// runtime.shared_pool().push(task).unwrap();
/// runner.run_once().unwrap();
/// ```
///
/// `future` 锁只覆盖取出或恢复装箱的 Future；绝不会跨越 `Future::poll`、任务池访问、
/// 工作线程通知、用户回调/析构、I/O 或 FFI。运行时状态操作为 O(1)、无分配且无锁，
/// 但比较并交换循环不具备无等待性，在竞争唤醒/轮询状态持续推进时可能重试。任务不是
/// `repr(C)`，不提供稳定的 FFI/Rust 布局 ABI。
///
/// # 布局与内存
///
/// 在已验收的 x86_64 目标上，加入内联调度状态后，
/// `AsyncTask<StealableTaskPool<()>, ()>` 为 96 字节，之前的实现为 80 字节。
/// 一字节状态跨过了该特化的 16 字节对齐边界，因此实际内联增量是 16 字节，而不是
/// 一字节。百万个同时存活的任务会增加 16,000,000 字节（约 15.26 MiB）任务本体
/// 存储。这是并发存活/保留成本，不会按历史累计执行过的任务数增长。
///
/// 状态不会新增独立堆分配。`Arc` 管理信息、装箱的 Future、任务句柄、context 负载和
/// 队列存储仍是独立的既有成本，不包含在 96 字节本体内。分配器尺寸分级和队列
/// 容量会使实际 RSS 与逻辑本体增量不同。若要把本体恢复到 80 字节，需要重新组织
/// context/state 表示；该优化会触及 V8 敏感的所有权表示，必须单独设计、审查和
/// 验证下游，因此本轮明确延期。
///
/// # 安全性
///
/// 托管轮询所有权由 `state` 同步，Future 所有权由 `future` 同步。既有 `TaskId` 和
/// context 安全要求不变。调用方不得并发手工轮询同一任务，也不得从任务池窃取
/// 运行时所有的任务。
pub struct AsyncTask<
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static = (),
> {
    uid:        TaskId,                                 //任务唯一标识
    future:     Mutex<Option<BoxFuture<'static, O>>>,   //异步任务
    pool:       Arc<P>,                                 //异步任务池
    priority:   usize,                                  //异步任务优先级
    context:    Option<UnsafeCell<Box<dyn Any>>>,       //异步任务上下文
    state:      AtomicU8,                               //内联调度状态；x86_64 上使当前特化由 80B 对齐至 96B
}

impl<
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> Drop for AsyncTask<P, O> {
    fn drop(&mut self) {
        let _ = unsafe { TaskHandle::<O>::from_raw((*self.uid.0.get() >> 64) as usize as *const ()) };
    }
}

unsafe impl<
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> Send for AsyncTask<P, O> {}
unsafe impl<
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> Sync for AsyncTask<P, O> {}

impl<
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
    O: Default + 'static,
> ArcWake for AsyncTask<P, O> {
    /// 发布一个可运行义务，并在需要时调度任务。
    ///
    /// 托管任务在已入队或运行时会合并重复唤醒。使空闲任务转为已调度状态的唤醒
    /// 准确执行一次 `Arc` 克隆、一次 `push_keep`，并且最多通知一个工作线程。运行中
    /// 的任务只记录延期调度；当前轮询所有者在恢复返回 `Pending` 的 Future 后入队。
    /// 对已完成任务的唤醒是空操作。
    ///
    /// 无竞争时的状态处理为 O(1) 且不新增分配；`push_keep` 继续服从具体任务池既有的
    /// 时间、容量和扩容成本。该路径不访问 Future 互斥锁，不阻塞等待，不执行用户回调、
    /// I/O 或 FFI。无锁比较并交换循环不具备无等待性，在竞争状态持续推进时可能重试。
    /// 与修改前相同，为保证运行时活性，任务池的 `push_keep` 必须能够接受可运行任务。
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let notify_on_push_error = match arc_self.prepare_wake() {
            AsyncTaskWakeAction::Coalesced => return,
            AsyncTaskWakeAction::LegacyEnqueue => true,
            AsyncTaskWakeAction::ManagedEnqueue => false,
        };

        let pool = arc_self.get_pool();
        let pushed = pool.push_keep(arc_self.clone()).is_ok();
        if pushed || notify_on_push_error {
            notify_runtime_task_pool(pool);
        }
    }
}

/// 在可运行任务已物理入队后，最多通知一个工作线程。
///
/// 多线程任务池使用 `wake_waiting_worker` 实现的有界陈旧等待者扫描；直接/单线程
/// 任务池使用既有受谓词保护的线程唤醒器。本辅助函数不入队也不轮询任务。直接
/// 唤醒器路径为 O(1)，既有 `waits` 注册表路径为有界 O(工作线程数量)；除既有短谓词锁
/// 外，不分配也不阻塞。托管路径必须只在成功入队后调用；兼容手工路径在
/// `push_keep` 返回错误时仍会调用，以保持修复前的通知语义。
#[inline]
pub(crate) fn notify_runtime_task_pool<
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
    O: Default + 'static,
>(pool: &P) {
    if let Some(waits) = pool.get_waits() {
        let _ = wake_waiting_worker(waits);
    } else if let Some(thread_waker) = pool.get_thread_waker() {
        let _ = wake_thread_waker(thread_waker);
    }
}

/// 把托管任务轮询期间观察到的一个延期唤醒入队。
///
/// 调用方必须先恢复 `Pending` Future，并完成 `RUNNING -> SCHEDULED` 转换。这里保留
/// 一次 `Arc` 克隆，因为公开任务池特征会消费队列参数，并且出错时不能返还所有权；
/// 其成本与修复前第一次唤醒相同，同时消除了全部重复唤醒克隆。入队成功后最多
/// 通知一个工作线程。复杂度为 O(1) 加所选任务池声明的队列复杂度；不会进入 Future
/// 锁、用户代码、I/O 或 FFI。
#[inline]
pub(crate) fn requeue_runtime_task<
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
    O: Default + 'static,
>(pool: &P, task: &Arc<AsyncTask<P, O>>) {
    if pool.push_keep(task.clone()).is_ok() {
        notify_runtime_task_pool(pool);
    }
}

impl<
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
    O: Default + 'static,
> AsyncTask<P, O> {
    /// 构造一个一次性异步任务。
    ///
    /// 任务初始带有一个托管轮询义务。若任务进入本库运行时，私有驱动会保证唤醒
    /// 合并和独占轮询。调用公开 `get_inner` 或 `set_inner` 会选择旧手工驱动模式，
    /// 但不改变这两个方法的签名或值语义。
    ///
    /// 构造时间复杂度为 O(1)，除参数已经拥有的值外不新增分配；不执行队列操作、
    /// 唤醒、轮询、加锁、I/O 或回调。每个值拥有一个 TaskId，因此构造不是幂等操作。
    pub fn new(uid: TaskId,
               pool: Arc<P>,
               priority: usize,
               future: Option<BoxFuture<'static, O>>) -> AsyncTask<P, O> {
        AsyncTask {
            uid,
            future: Mutex::new(future),
            pool,
            priority,
            context: None,
            state: AtomicU8::new(ASYNC_TASK_STATE_INITIAL),
        }
    }

    /// 构造一个带调用方 context 的一次性任务。
    ///
    /// 调度和唤醒语义与 `AsyncTask::new` 相同。本函数为 `context` 执行一次 `Box`
    /// 分配，因此构造的时间和空间复杂度均为 O(1)。它不入队、不轮询、不唤醒、
    /// 不获取运行时锁、不执行用户代码、不执行 I/O，也不接触 FFI。保存的 context
    /// 继续服从既有所有者线程/context 访问契约。
    pub fn with_context<C: 'static>(uid: TaskId,
                                    pool: Arc<P>,
                                    priority: usize,
                                    future: Option<BoxFuture<'static, O>>,
                                    context: C) -> AsyncTask<P, O> {
        let any = Box::new(context);

        AsyncTask {
            uid,
            future: Mutex::new(future),
            pool,
            priority,
            context: Some(UnsafeCell::new(any)),
            state: AtomicU8::new(ASYNC_TASK_STATE_INITIAL),
        }
    }

    /// 构造一个绑定到 `runtime` 且携带调用方 context 的任务。
    ///
    /// 返回值随后由本库运行时驱动消费时属于托管任务，这也包括
    /// `pi_v8::VmTaskPool` 等外部定时器适配器。公开 get/set 仍会选择旧手工驱动。
    /// 构造为 O(1)，执行既有 `runtime.alloc` TaskHandle 分配、一次 context `Box`
    /// 分配和一次共享任务池 `Arc` 克隆。分配失败继续保持这些既有操作的进程级行为。
    /// 本函数不入队、不轮询、不唤醒、不阻塞、不执行用户代码、I/O 或 FFI。
    pub fn with_runtime_and_context<RT, C>(runtime: &RT,
                                           priority: usize,
                                           future: Option<BoxFuture<'static, O>>,
                                           context: C) -> AsyncTask<P, O>
        where RT: AsyncRuntime<O, Pool = P>,
              C: Send + 'static {
        let any = Box::new(context);

        AsyncTask {
            uid: runtime.alloc::<O>(),
            future: Mutex::new(future),
            pool: runtime.shared_pool(),
            priority,
            context: Some(UnsafeCell::new(any)),
            state: AtomicU8::new(ASYNC_TASK_STATE_INITIAL),
        }
    }

    /// 判断一次唤醒是否需要一个物理队列项。
    ///
    /// 即使 `next == current`，成功的比较并交换也使用 `AcqRel`。该同值读改写会发布每次被合并
    /// 唤醒之前的写入；后续轮询认领在读取 Future 关联共享状态前获取最新原子
    /// 修改。弱比较并交换可能伪失败，因此必须使用重试循环。本操作无锁但不具备无等待性；
    /// 不涉及互斥锁、分配、克隆、队列或用户代码。
    #[inline]
    fn prepare_wake(&self) -> AsyncTaskWakeAction {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            if current & ASYNC_TASK_STATE_MANAGED == 0 {
                return AsyncTaskWakeAction::LegacyEnqueue;
            }
            if current & ASYNC_TASK_STATE_COMPLETED != 0 {
                return AsyncTaskWakeAction::Coalesced;
            }

            let next = current | ASYNC_TASK_STATE_SCHEDULED;
            match self.state.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if current & (ASYNC_TASK_STATE_SCHEDULED | ASYNC_TASK_STATE_RUNNING) != 0 {
                        return AsyncTaskWakeAction::Coalesced;
                    }
                    return AsyncTaskWakeAction::ManagedEnqueue;
                },
                Err(actual) => current = actual,
            }
        }
    }

    /// 认领一个尚未履行的运行时轮询义务。
    ///
    /// `Managed` 通过原子清除 `SCHEDULED` 并设置 `RUNNING` 授予独占所有权。
    /// `Discard` 表示物理队列项陈旧、重复或已完成，必须在不接触 Future 的情况下
    /// 丢弃。`Legacy` 委托给保持不变的公开取出/轮询/恢复行为。
    ///
    /// 无竞争路径为 O(1)，不分配、无锁且不阻塞。它不具备无等待性：弱比较并交换循环
    /// 可能因竞争或伪失败而重试。`AcqRel` 与唤醒发布同步；不接触用户代码、队列、
    /// 定时器、工作线程锁、I/O 或 FFI。
    #[inline]
    pub(crate) fn try_begin_runtime_poll(&self) -> AsyncTaskPollClaim {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            if current & ASYNC_TASK_STATE_MANAGED == 0 {
                return AsyncTaskPollClaim::Legacy;
            }
            if current & ASYNC_TASK_STATE_COMPLETED != 0
                || current & ASYNC_TASK_STATE_SCHEDULED == 0
                || current & ASYNC_TASK_STATE_RUNNING != 0
            {
                return AsyncTaskPollClaim::Discard;
            }

            let next =
                (current & !ASYNC_TASK_STATE_SCHEDULED) | ASYNC_TASK_STATE_RUNNING;
            match self.state.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return AsyncTaskPollClaim::Managed,
                Err(actual) => current = actual,
            }
        }
    }

    /// 在成功认领托管轮询后取出 Future。
    ///
    /// 只有本库运行时驱动可以调用本方法。互斥锁临界区只包含 `Option::take`，绝不与
    /// `Future::poll`、唤醒、任务池访问、工作线程通知或用户代码重叠。时间复杂度
    /// O(1)，不分配。
    #[inline]
    pub(crate) fn take_inner_for_runtime_poll(&self) -> Option<BoxFuture<'static, O>> {
        self.future.lock().take()
    }

    /// 恢复一个返回 `Pending` 的托管 Future。
    ///
    /// 必须在清除 `RUNNING` 前恢复，否则后续工作线程可能认领延期调度却观察到 Future
    /// 缺失。短互斥锁临界区只包含 `Option::replace`。若非法并发手工访问导致槽位中
    /// 已有值，该值只会在解锁后析构。时间复杂度 O(1)，不新增分配，不在锁内执行回调，
    /// 也不执行队列操作、I/O 或 FFI。
    #[inline]
    pub(crate) fn restore_inner_after_runtime_poll(
        &self,
        inner: BoxFuture<'static, O>,
    ) {
        let replaced = {
            let mut future = self.future.lock();
            future.replace(inner)
        };
        drop(replaced);
    }

    /// 完成一次托管 `Pending` 状态转换。
    ///
    /// 返回任务运行期间是否有唤醒设置了 `SCHEDULED`；调用方随后准确创建一个队列项。
    /// Future 恢复和互斥锁解锁先行发生于本次 `AcqRel` 转换；下一次认领通过
    /// `Acquire` 获取前述写入。
    #[inline]
    fn finish_runtime_poll_pending(&self) -> bool {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            if current & ASYNC_TASK_STATE_MANAGED == 0
                || current & ASYNC_TASK_STATE_COMPLETED != 0
                || current & ASYNC_TASK_STATE_RUNNING == 0
            {
                return false;
            }

            let next = current & !ASYNC_TASK_STATE_RUNNING;
            match self.state.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return current & ASYNC_TASK_STATE_SCHEDULED != 0,
                Err(actual) => current = actual,
            }
        }
    }

    /// 发布托管任务终态。
    ///
    /// 原子存储会有意同时清除 `RUNNING` 和并发延期的 `SCHEDULED` 位。竞争唤醒要么先于
    /// 本次 `Release` 存储发布并被完成状态吸收，要么观察到 `COMPLETED` 后成为空操作。
    /// 本操作为 O(1)，不分配、无锁且不阻塞。
    #[inline]
    fn finish_runtime_poll_ready(&self) {
        self.state.store(
            ASYNC_TASK_STATE_MANAGED | ASYNC_TASK_STATE_COMPLETED,
            Ordering::Release,
        );
    }

    /// 选择既有公开手工驱动行为。
    ///
    /// 历史上，公开 get/set 调用方拥有取出/轮询/恢复协议，无法调用新的私有完成
    /// 钩子。因此暴露 Future 前会把空闲或已入队的托管任务原子切换为兼容手工模式。已在
    /// 正在轮询的运行时任务不会降级。`allow_completed` 只供公开 `set_inner` 使用，以
    /// 保留显式低层任务复用。
    ///
    /// 无竞争时为 O(1)，不分配、无锁且不阻塞；竞争状态转换下不具备无等待性。不访问
    /// 互斥锁、任务池或用户代码。
    #[inline]
    fn select_legacy_manual_driver(&self, allow_completed: bool) {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            if current & ASYNC_TASK_STATE_MANAGED == 0
                || current & ASYNC_TASK_STATE_RUNNING != 0
                || (!allow_completed && current & ASYNC_TASK_STATE_COMPLETED != 0)
            {
                return;
            }

            match self.state.compare_exchange_weak(
                current,
                0,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    /// 检查是否允许唤醒
    pub fn is_enable_wakeup(&self) -> bool {
        self.uid.exist_waker::<O>()
    }

    /// 为外部/手工任务驱动取出装箱的 Future。
    ///
    /// 本方法在取出 Future 前选择兼容手工调度，因此既有自定义驱动无需新增特征
    /// 方法即可保留原有唤醒后调用 `push_keep` 的行为。本方法不是运行时驱动入口。
    ///
    /// 当其它驱动拥有 Future 或任务已完成时返回 `None`。时间复杂度 O(1)，不分配；
    /// 只获取任务的短 Future 互斥锁，可能与其它 get/set 短暂竞争。不得与本库运行时驱动
    /// 并发调用，也不得用于并发轮询同一任务。本操作非纯、非幂等，并转移 Future
    /// 所有权。
    pub fn get_inner(&self) -> Option<BoxFuture<'static, O>> {
        self.select_legacy_manual_driver(false);
        self.future.lock().take()
    }

    /// 为外部/手工任务驱动替换装箱的 Future。
    ///
    /// 本方法选择兼容手工调度，也允许显式复用已完成的低层任务，从而保留旧公开 get/set
    /// 能力。运行时所有的 `Pending` 恢复使用私有辅助函数，仍保持托管。复用已完成任务
    /// 前，手工驱动必须回收全部旧唤醒器；在保留的兼容手工契约下，旧唤醒器否则可能
    /// 把替换后的 Future 入队。
    ///
    /// 时间复杂度 O(1)，除调用方拥有的装箱值外不分配；只获取短 Future 互斥锁，不轮询、
    /// 不入队也不唤醒。本操作非纯且非幂等。被替换的 Future 会先移出互斥锁临界区，
    /// 再在调用线程析构，因此其析构函数不能在持有 Future 锁时重入本任务。
    pub fn set_inner(&self, inner: Option<BoxFuture<'static, O>>) {
        self.select_legacy_manual_driver(true);
        let replaced = {
            let mut future = self.future.lock();
            std::mem::replace(&mut *future, inner)
        };
        drop(replaced);
    }

    /// 获取任务的所有者
    #[inline]
    pub fn owner(&self) -> usize {
        unsafe {
            *self.uid.0.get() as usize
        }
    }

    /// 获取异步任务优先级
    #[inline]
    pub fn priority(&self) -> usize {
        self.priority
    }

    //判断异步任务是否有上下文
    pub fn exist_context(&self) -> bool {
        self.context.is_some()
    }

    //获取异步任务上下文的只读引用
    pub fn get_context<C: Send + 'static>(&self) -> Option<&C> {
        if let Some(context) = &self.context {
            //存在上下文
            let any = unsafe { &*context.get() };
            return <dyn Any>::downcast_ref::<C>(&**any);
        }

        None
    }

    //获取异步任务上下文的可写引用
    pub fn get_context_mut<C: Send + 'static>(&self) -> Option<&mut C> {
        if let Some(context) = &self.context {
            //存在上下文
            let any = unsafe { &mut *context.get() };
            return <dyn Any>::downcast_mut::<C>(&mut **any);
        }

        None
    }

    //设置异步任务上下文，返回上一个异步任务上下文
    pub fn set_context<C: Send + 'static>(&self, new: C) {
        if let Some(context) = &self.context {
            //存在上一个上下文，则释放上一个上下文
            let _ = unsafe { &*context.get() };

            //设置新的上下文
            let any: Box<dyn Any + 'static> = Box::new(new);
            unsafe { *context.get() = any; }
        }
    }

    //获取异步任务的任务池
    pub fn get_pool(&self) -> &P {
        self.pool.as_ref()
    }
}

///
/// 异步任务池
///
pub trait AsyncTaskPool<O: Default + 'static = ()>: Default + Send + Sync + 'static {
    type Pool: AsyncTaskPoolExt<O> + AsyncTaskPool<O>;

    /// 获取绑定的线程唯一id
    fn get_thread_id(&self) -> usize;

    /// 获取当前异步任务池内任务数量
    fn len(&self) -> usize;

    /// 将异步任务加入异步任务池
    fn push(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()>;

    /// 将异步任务加入本地异步任务池
    fn push_local(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()>;

    /// 将指定了优先级的异步任务加入任务池
    fn push_priority(&self,
                     priority: usize,
                     task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()>;

    /// 将一次唤醒或托管延期唤醒产生的一个可运行任务入队。
    ///
    /// 每次成功调用准确拥有一个物理队列项。托管 `AsyncTask` 状态会在调用本方法前
    /// 合并重复唤醒；自定义任务池不得自行增加轮询或重复队列项。返回 `Err` 表示
    /// 运行时无法保证该次唤醒的进度，因为本特征会消费任务 `Arc`，不能返还所有权。
    ///
    /// 供运行时使用的实现必须线程安全，不得与 `Future::poll` 重入，并应在唤醒热路径
    /// 上保持不阻塞/O(1)。实现不得调用用户代码或通知多个工作线程；工作线程通知由
    /// 运行时在成功入队后执行。
    fn push_keep(&self, task: Arc<AsyncTask<Self::Pool, O>>) -> Result<()>;

    /// 尝试从异步任务池中弹出一个异步任务
    fn try_pop(&self) -> Option<Arc<AsyncTask<Self::Pool, O>>>;

    /// 尝试从异步任务池中弹出所有异步任务
    fn try_pop_all(&self) -> IntoIter<Arc<AsyncTask<Self::Pool, O>>>;

    /// 获取本地线程的唤醒器
    fn get_thread_waker(&self) -> Option<&Arc<(AtomicBool, Mutex<()>, Condvar)>>;
}

///
/// 异步任务池扩展
///
pub trait AsyncTaskPoolExt<O: Default + 'static = ()>: Send + Sync + 'static {
    /// 设置待唤醒的工作者唤醒器队列
    fn set_waits(&mut self,
                 _waits: Arc<ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>>) {}

    /// 获取待唤醒的工作者唤醒器队列
    fn get_waits(&self) -> Option<&Arc<ArrayQueue<Arc<(AtomicBool, Mutex<()>, Condvar)>>>> {
        //默认没有待唤醒的工作者唤醒器队列
        None
    }

    /// 获取空闲的工作者的数量，这个数量大于0，表示可以新开线程来运行可分派的工作者
    fn idler_len(&self) -> usize {
        //默认不分派
        0
    }

    /// 分派一个空闲的工作者
    fn spawn_worker(&self) -> Option<usize> {
        //默认不分派
        None
    }

    /// 获取工作者的数量
    fn worker_len(&self) -> usize {
        //默认工作者数量和本机逻辑核数相同
        #[cfg(not(target_arch = "wasm32"))]
        return num_cpus::get();
        #[cfg(target_arch = "wasm32")]
        return 1;
    }

    /// 获取缓冲区的任务数量，缓冲区任务是未分配给工作者的任务
    fn buffer_len(&self) -> usize {
        //默认没有缓冲区
        0
    }

    /// 设置当前绑定本地线程的唤醒器
    fn set_thread_waker(&mut self, _thread_waker: Arc<(AtomicBool, Mutex<()>, Condvar)>) {
        //默认不设置
    }

    /// 复制当前绑定本地线程的唤醒器
    fn clone_thread_waker(&self) -> Option<Arc<(AtomicBool, Mutex<()>, Condvar)>> {
        //默认不复制
        None
    }

    /// 关闭当前工作者
    fn close_worker(&self) {
        //默认不允许关闭工作者
    }
}

///
/// 异步运行时
///
pub trait AsyncRuntime<O: Default + 'static = ()>: Clone + Send + Sync + 'static {
    type Pool: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = Self::Pool>;

    /// 共享运行时内部任务池
    fn shared_pool(&self) -> Arc<Self::Pool>;

    /// 获取当前异步运行时的唯一id
    fn get_id(&self) -> usize;

    /// 获取当前异步运行时待处理任务数量
    fn wait_len(&self) -> usize;

    /// 获取当前异步运行时任务数量
    fn len(&self) -> usize;

    /// 分配异步任务的唯一id
    fn alloc<R: 'static>(&self) -> TaskId;

    /// 派发一个指定的异步任务到异步运行时
    fn spawn<F>(&self, future: F) -> Result<TaskId>
        where F: Future<Output = O> + Send + 'static;

    /// 派发一个异步任务到本地异步运行时，如果本地没有本异步运行时，则会派发到当前运行时中
    fn spawn_local<F>(&self, future: F) -> Result<TaskId>
        where F: Future<Output = O> + Send + 'static;

    /// 派发一个指定优先级的异步任务到异步运行时
    fn spawn_priority<F>(&self, priority: usize, future: F) -> Result<TaskId>
        where F: Future<Output = O> + Send + 'static;

    /// 派发一个异步任务到异步运行时，并立即让出任务的当前运行
    fn spawn_yield<F>(&self, future: F) -> Result<TaskId>
        where F: Future<Output = O> + Send + 'static;

    /// 派发一个在指定时间后执行的异步任务到异步运行时，时间单位ms
    fn spawn_timing<F>(&self, future: F, time: usize) -> Result<TaskId>
        where F: Future<Output = O> + Send + 'static;

    /// 派发一个指定任务唯一id的异步任务到异步运行时
    fn spawn_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where F: Future<Output = O> + Send + 'static;

    /// 派发一个指定任务唯一id的异步任务到本地异步运行时，如果本地没有本异步运行时，则会派发到当前运行时中
    fn spawn_local_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where F: Future<Output = O> + Send + 'static;

    /// 派发一个指定任务唯一id和任务优先级的异步任务到异步运行时
    fn spawn_priority_by_id<F>(&self,
                               task_id: TaskId,
                               priority: usize,
                               future: F) -> Result<()>
        where F: Future<Output = O> + Send + 'static;

    /// 派发一个指定任务唯一id的异步任务到异步运行时，并立即让出任务的当前运行
    fn spawn_yield_by_id<F>(&self, task_id: TaskId, future: F) -> Result<()>
        where F: Future<Output = O> + Send + 'static;

    /// 派发一个指定任务唯一id和在指定时间后执行的异步任务到异步运行时，时间单位ms
    fn spawn_timing_by_id<F>(&self,
                             task_id: TaskId,
                             future: F,
                             time: usize) -> Result<()>
        where F: Future<Output = O> + Send + 'static;

    /// 挂起指定唯一id的异步任务
    fn pending<Output: 'static>(&self, task_id: &TaskId, waker: Waker) -> Poll<Output>;

    /// 唤醒指定唯一id的异步任务
    fn wakeup<Output: 'static>(&self, task_id: &TaskId);

    /// 挂起当前异步运行时的当前任务，并在指定的其它运行时上派发一个指定的异步任务，等待其它运行时上的异步任务完成后，唤醒当前运行时的当前任务，并返回其它运行时上的异步任务的值
    fn wait<V: Send + 'static>(&self) -> AsyncWait<V>;

    /// 挂起当前异步运行时的当前任务，并在多个其它运行时上执行多个其它任务，其中任意一个任务完成，则唤醒当前运行时的当前任务，并返回这个已完成任务的值，而其它未完成的任务的值将被忽略
    fn wait_any<V: Send + 'static>(&self, capacity: usize) -> AsyncWaitAny<V>;

    /// 挂起当前异步运行时的当前任务，并在多个其它运行时上执行多个其它任务，任务返回后需要通过用户指定的检查回调进行检查，其中任意一个任务检查通过，则唤醒当前运行时的当前任务，并返回这个已完成任务的值，而其它未完成或未检查通过的任务的值将被忽略，如果所有任务都未检查通过，则强制唤醒当前运行时的当前任务
    fn wait_any_callback<V: Send + 'static>(&self, capacity: usize) -> AsyncWaitAnyCallback<V>;

    /// 构建用于派发多个异步任务到指定运行时的映射归并，需要指定映射归并的容量
    fn map_reduce<V: Send + 'static>(&self, capacity: usize) -> AsyncMapReduce<V>;

    /// 挂起当前异步运行时的当前任务，等待指定的时间后唤醒当前任务
    fn timeout(&self, timeout: usize) -> BoxFuture<'static, ()>;

    /// 立即让出当前任务的执行
    fn yield_now(&self) -> BoxFuture<'static, ()>;

    /// 生成一个异步管道，输入指定流，输入流的每个值通过过滤器生成输出流的值
    fn pipeline<S, SO, F, FO>(&self, input: S, filter: F) -> BoxStream<'static, FO>
        where S: Stream<Item = SO> + Send + 'static,
              SO: Send + 'static,
              F: FnMut(SO) -> AsyncPipelineResult<FO> + Send + 'static,
              FO: Send + 'static;

    /// 关闭异步运行时，返回请求关闭是否成功
    fn close(&self) -> bool;
}

///
/// 异步运行时扩展
///
pub trait AsyncRuntimeExt<O: Default + 'static = ()> {
    /// 派发一个指定的异步任务到异步运行时，并指定异步任务的初始化上下文
    fn spawn_with_context<F, C>(&self,
                                task_id: TaskId,
                                future: F,
                                context: C) -> Result<()>
        where F: Future<Output = O> + Send + 'static,
              C: 'static;

    /// 派发一个在指定时间后执行的异步任务到异步运行时，并指定异步任务的初始化上下文，时间单位ms
    fn spawn_timing_with_context<F, C>(&self,
                                       task_id: TaskId,
                                       future: F,
                                       context: C,
                                       time: usize) -> Result<()>
        where F: Future<Output = O> + Send + 'static,
              C: Send + 'static;

    /// 立即创建一个指定任务池的异步运行时，并执行指定的异步任务，阻塞当前线程，等待异步任务完成后返回
    fn block_on<F>(&self, future: F) -> Result<F::Output>
        where F: Future + Send + 'static,
              <F as Future>::Output: Default + Send + 'static;
}

///
/// 异步运行时构建器
///
pub struct AsyncRuntimeBuilder<O: Default + 'static = ()>(PhantomData<O>);

impl<O: Default + 'static> AsyncRuntimeBuilder<O> {
    /// 构建默认的工作者异步运行时
    pub fn default_worker_thread(worker_name: Option<&str>,
                                 worker_stack_size: Option<usize>,
                                 worker_sleep_timeout: Option<u64>,
                                 worker_loop_interval: Option<Option<u64>>) -> WorkerRuntime<O> {
        let runner = WorkerTaskRunner::default();

        let thread_name = if let Some(name) = worker_name {
            name
        } else {
            //默认的线程名称
            "Default-Single-Worker"
        };
        let thread_stack_size = if let Some(size) = worker_stack_size {
            size
        } else {
            //默认的线程堆栈大小
            2 * 1024 * 1024
        };
        let sleep_timeout = if let Some(timeout) = worker_sleep_timeout {
            timeout
        } else {
            //默认的线程休眠时长
            1
        };
        let loop_interval = if let Some(interval) = worker_loop_interval {
            interval
        } else {
            //默认的线程循环间隔时长
            None
        };

        //创建线程并在线程中执行异步运行时
        let clock = Clock::new();
        let runner_copy = runner.clone();
        let rt_copy = runner.get_runtime();
        let rt = runner.startup(
            thread_name,
            thread_stack_size,
            sleep_timeout,
            loop_interval,
            move || {
                let last = clock.recent();
                match runner_copy.run_once() {
                    Err(e) => {
                        panic!("Run runner failed, reason: {:?}", e);
                    },
                    Ok(len) => {
                        (len == 0,
                         clock
                             .recent()
                             .duration_since(last))
                    },
                }
            },
            move || {
                rt_copy.wait_len() + rt_copy.len()
            },
        );

        rt
    }

    /// 构建自定义的工作者异步运行时
    pub fn custom_worker_thread<P, F0, F1>(pool: P,
                                           worker_handle: Arc<AtomicBool>,
                                           worker_condvar: Arc<(AtomicBool, Mutex<()>, Condvar)>,
                                           thread_name: &str,
                                           thread_stack_size: usize,
                                           sleep_timeout: u64,
                                           loop_interval: Option<u64>,
                                           loop_func: F0,
                                           get_queue_len: F1) -> WorkerRuntime<O, P>
        where P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>,
              F0: Fn() -> (bool, Duration) + Send + 'static,
              F1: Fn() -> usize + Send + 'static {
        let runner = WorkerTaskRunner::new(pool,
                                           worker_handle,
                                           worker_condvar);

        //创建线程并在线程中执行异步运行时
        let rt_copy = runner.get_runtime();
        let rt = runner.startup(
            thread_name,
            thread_stack_size,
            sleep_timeout,
            loop_interval,
            loop_func,
            move || {
                rt_copy.wait_len() + get_queue_len()
            },
        );

        rt
    }

    /// 构建默认的多线程异步运行时。
    ///
    /// 说明：
    /// - 这是 `AsyncRuntimeBuilder` 对外提供的默认多线程 runtime 构建入口。
    /// - 本轮保持函数签名、返回类型和既有启动语义不变。
    /// - 当调用方显式传入大于 0 的 `worker_size` 时，会同时创建相同 worker slot 数量
    ///   的 `StealableTaskPool`，避免启动 worker 数量大于 pool 内实际 worker slot 时，
    ///   worker 线程中 `clone_thread_waker().unwrap()` panic。
    /// - `worker_size=Some(0)` 保留旧语义：仍通过 builder 的 `init_worker_size(0)` 和
    ///   `set_worker_limit(0, 0)` 兜底，不会把 0 直接传给 `StealableTaskPool::with`。
    ///
    /// 参数：
    /// - `worker_prefix`：worker 线程名前缀；`None` 使用 builder 默认值。
    /// - `worker_stack_size`：worker 栈大小；`None` 使用默认 2 MiB。
    /// - `worker_size`：固定 worker 数量；`None` 使用默认 builder 和默认 pool 尺寸；
    ///   `Some(0)` 使用 builder 原有的默认初始 worker 兜底语义。
    /// - `worker_sleep_timeout`：worker 空闲休眠最长时长，单位 ms；`None` 使用默认值。
    ///
    /// 返回：
    /// - 已启动的 `MultiTaskRuntime<O>`。
    ///
    /// 边界条件：
    /// - `worker_size=Some(size > 0)` 时，实际 worker 数和 pool worker slot 数保持一致。
    /// - `worker_size=Some(0)` 时不创建 0 worker pool，避免兼容性回归。
    /// - `worker_size=None` 时不改变原默认构建路径。
    ///
    /// 性能：
    /// - 构建时间 O(W)，W 为 worker 数；空间 O(W)。
    /// - 该函数不是任务调度热路径。
    ///
    /// 副作用与安全性：
    /// - 非纯函数，会创建任务池、runtime 和 worker 线程。
    /// - 不阻塞等待 worker 完成；不执行用户 future。
    /// - 线程安全由 `MultiTaskRuntimeBuilder::build` 和底层任务池保证。
    pub fn default_multi_thread(worker_prefix: Option<&str>,
                                worker_stack_size: Option<usize>,
                                worker_size: Option<usize>,
                                worker_sleep_timeout: Option<u64>) -> MultiTaskRuntime<O> {
        let mut builder = if let Some(size) = worker_size.filter(|size| *size > 0) {
            let pool = StealableTaskPool::with(size,
                                               65535,
                                               [1, 1],
                                               3000);
            MultiTaskRuntimeBuilder::new(pool)
                .thread_stack_size(2 * 1024 * 1024)
                .set_timer_interval(1)
        } else {
            MultiTaskRuntimeBuilder::default()
        };

        if let Some(size) = worker_size {
            builder = builder
                .init_worker_size(size)
                .set_worker_limit(size, size);
        }
        if let Some(thread_prefix) = worker_prefix {
            builder = builder.thread_prefix(thread_prefix);
        }
        if let Some(thread_stack_size) = worker_stack_size {
            builder = builder.thread_stack_size(thread_stack_size);
        }
        if let Some(sleep_timeout) = worker_sleep_timeout {
            builder = builder.set_timeout(sleep_timeout);
        }

        builder.build()
    }

    /// 构建自定义的多线程异步运行时
    pub fn custom_multi_thread<P>(pool: P,
                                  worker_prefix: &str,
                                  worker_stack_size: usize,
                                  worker_size: usize,
                                  worker_sleep_timeout: u64,
                                  worker_timer_interval: usize) -> MultiTaskRuntime<O, P>
        where P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P> {
        MultiTaskRuntimeBuilder::new(pool)
            .thread_prefix(worker_prefix)
            .thread_stack_size(worker_stack_size)
            .init_worker_size(worker_size)
            .set_worker_limit(worker_size, worker_size)
            .set_timeout(worker_sleep_timeout)
            .set_timer_interval(worker_timer_interval)
            .build()
    }
}

/// 绑定指定异步运行时到本地线程
pub fn bind_local_thread<O: Default + 'static>(runtime: LocalAsyncRuntime<O>) {
    match PI_ASYNC_LOCAL_THREAD_ASYNC_RUNTIME.try_with(move |rt| {
        let raw = Arc::into_raw(Arc::new(runtime)) as *mut LocalAsyncRuntime<O> as *mut ();
        rt.store(raw, Ordering::Relaxed);
    }) {
        Err(e) => {
            panic!("Bind single runtime to local thread failed, reason: {:?}", e);
        },
        Ok(_) => (),
    }
}

/// 从本地线程解绑单线程异步任务执行器
pub fn unbind_local_thread() {
    let _ = PI_ASYNC_LOCAL_THREAD_ASYNC_RUNTIME.try_with(move |rt| {
        rt.store(null_mut(), Ordering::Relaxed);
    });
}

///
/// 本地线程绑定的异步运行时
///
pub struct LocalAsyncRuntime<O: Default + 'static> {
    inner:              *const (),                                                  //内部运行时指针
    get_id_func:        fn(*const ()) -> usize,                                     //获取本地运行时的id的函数
    spawn_func:         fn(*const (), BoxFuture<'static, O>) -> Result<()>,         //派发函数
    spawn_local_func:   fn(*const (), BoxFuture<'static, O>) -> Result<()>,         //本地派发函数
    spawn_timing_func:  fn(*const (), BoxFuture<'static, O>, usize) -> Result<()>,  //定时派发函数
    timeout_func:       fn(*const (), usize) -> BoxFuture<'static, ()>,             //超时函数
}

unsafe impl<O: Default + 'static> Send for LocalAsyncRuntime<O> {}
unsafe impl<O: Default + 'static> Sync for LocalAsyncRuntime<O> {}

impl<O: Default + 'static> LocalAsyncRuntime<O> {
    /// 创建本地线程绑定的异步运行时
    pub fn new(inner: *const (),
               get_id_func: fn(*const ()) -> usize,
               spawn_func: fn(*const (), BoxFuture<'static, O>) -> Result<()>,
               spawn_local_func: fn(*const (), BoxFuture<'static, O>) -> Result<()>,
               spawn_timing_func: fn(*const (), BoxFuture<'static, O>, usize) -> Result<()>,
               timeout_func: fn(*const (), usize) -> BoxFuture<'static, ()>) -> Self {
        LocalAsyncRuntime {
            inner,
            get_id_func,
            spawn_func,
            spawn_local_func,
            spawn_timing_func,
            timeout_func,
        }
    }

    /// 获取本地运行时的id
    #[inline]
    pub fn get_id(&self) -> usize {
        (self.get_id_func)(self.inner)
    }

    /// 派发一个指定的异步任务到异步运行时
    #[inline]
    pub fn spawn<F>(&self, future: F) -> Result<()>
        where F: Future<Output = O> + Send + 'static {
        (self.spawn_func)(self.inner, async move {
            future.await
        }.boxed())
    }

    /// 派发一个指定的异步任务到本地线程绑定的异步运行时
    #[inline]
    pub fn spawn_local<F>(&self, future: F) -> Result<()>
    where F: Future<Output = O> + Send + 'static {
        (self.spawn_local_func)(self.inner, async move {
            future.await
        }.boxed())
    }

    /// 定时派发一个指定的异步任务到本地线程绑定的异步运行时
    #[inline]
    pub fn sapwn_timing_func<F>(&self, future: F, timeout: usize) -> Result<()>
        where F: Future<Output = O> + Send + 'static {
        (self.spawn_timing_func)(self.inner,
                                 async move {
                                     future.await
                                 }.boxed(),
                                 timeout)
    }

    /// 挂起本地线程绑定的异步运行时的当前任务，等待指定的时间后唤醒当前任务
    #[inline]
    pub fn timeout(&self, timeout: usize) -> BoxFuture<'static, ()> {
        (self.timeout_func)(self.inner, timeout)
    }
}

///
/// 获取本地线程绑定的异步运行时
/// 注意：O如果与本地线程绑定的运行时的O不相同，则无法获取本地线程绑定的运行时
///
pub fn local_async_runtime<O: Default + 'static>() -> Option<Arc<LocalAsyncRuntime<O>>> {
    match PI_ASYNC_LOCAL_THREAD_ASYNC_RUNTIME.try_with(move |ptr| {
        let raw = ptr.load(Ordering::Relaxed) as *const LocalAsyncRuntime<O>;
        unsafe {
            if raw.is_null() {
                //本地线程未绑定异步运行时
                None
            } else {
                //本地线程已绑定异步运行时
                let shared: Arc<LocalAsyncRuntime<O>> = unsafe { Arc::from_raw(raw) };
                let result = shared.clone();
                Arc::into_raw(shared); //避免提前释放
                Some(result)
            }
        }
    }) {
        Err(_) => None, //本地线程没有绑定异步运行时
        Ok(rt) => rt,
    }
}

///
/// 派发任务到本地线程绑定的异步运行时，如果本地线程没有异步运行时，则返回错误
/// 注意：F::Output如果与本地线程绑定的运行时的O不相同，则无法执行指定任务
///
pub fn spawn_local<O, F>(future: F) -> Result<()>
    where O: Default + 'static,
          F: Future<Output = O> + Send + 'static {
    if let Some(rt) = local_async_runtime::<O>() {
        rt.spawn(future)
    } else {
        Err(Error::new(ErrorKind::Other, format!("Spawn task to local thread failed, reason: runtime not exist")))
    }
}

///
/// 从本地线程绑定的字典中获取指定类型的值的只读引用
///
pub fn get_local_dict<T: 'static>() -> Option<&'static T> {
    match PI_ASYNC_LOCAL_THREAD_ASYNC_RUNTIME_DICT.try_with(move |dict| {
        unsafe {
            if let Some(any) = (&*dict.get()).get(&TypeId::of::<T>()) {
                //指定类型的值存在
                <dyn Any>::downcast_ref::<T>(&**any)
            } else {
                //指定类型的值不存在
                None
            }
        }
    }) {
        Err(_) => {
            None
        },
        Ok(result) => {
            result
        }
    }
}

///
/// 从本地线程绑定的字典中获取指定类型的值的可写引用
///
pub fn get_local_dict_mut<T: 'static>() -> Option<&'static mut T> {
    match PI_ASYNC_LOCAL_THREAD_ASYNC_RUNTIME_DICT.try_with(move |dict| {
        unsafe {
            if let Some(any) = (&mut *dict.get()).get_mut(&TypeId::of::<T>()) {
                //指定类型的值存在
                <dyn Any>::downcast_mut::<T>(&mut **any)
            } else {
                //指定类型的值不存在
                None
            }
        }
    }) {
        Err(_) => {
            None
        },
        Ok(result) => {
            result
        }
    }
}

///
/// 在本地线程绑定的字典中设置指定类型的值，返回上一个设置的值
///
pub fn set_local_dict<T: 'static>(value: T) -> Option<T> {
    match PI_ASYNC_LOCAL_THREAD_ASYNC_RUNTIME_DICT.try_with(move |dict| {
        unsafe {
            let result = if let Some(any) = (&mut *dict.get()).remove(&TypeId::of::<T>()) {
                //指定类型的上一个值存在
                if let Ok(r) = any.downcast() {
                    //造型成功，则返回
                    Some(*r)
                } else {
                    None
                }
            } else {
                //指定类型的上一个值不存在
                None
            };

            //设置指定类型的新值
            (&mut *dict.get()).insert(TypeId::of::<T>(), Box::new(value) as Box<dyn Any>);

            result
        }
    }) {
        Err(_) => {
            None
        },
        Ok(result) => {
            result
        }
    }
}

///
/// 在本地线程绑定的字典中移除指定类型的值，并返回移除的值
///
pub fn remove_local_dict<T: 'static>() -> Option<T> {
    match PI_ASYNC_LOCAL_THREAD_ASYNC_RUNTIME_DICT.try_with(move |dict| {
        unsafe {
            if let Some(any) = (&mut *dict.get()).remove(&TypeId::of::<T>()) {
                //指定类型的上一个值存在
                if let Ok(r) = any.downcast() {
                    //造型成功，则返回
                    Some(*r)
                } else {
                    None
                }
            } else {
                //指定类型的上一个值不存在
                None
            }
        }
    }) {
        Err(_) => {
            None
        },
        Ok(result) => {
            result
        }
    }
}

///
/// 清空本地线程绑定的字典
///
pub fn clear_local_dict() -> Result<()> {
    match PI_ASYNC_LOCAL_THREAD_ASYNC_RUNTIME_DICT.try_with(move |dict| {
        unsafe {
            (&mut *dict.get()).clear();
        }
    }) {
        Err(e) => {
            Err(Error::new(ErrorKind::Other, format!("Clear local dict failed, reason: {:?}", e)))
        },
        Ok(_) => {
            Ok(())
        }
    }
}

const ASYNC_VALUE_EMPTY: u8 = 0;
const ASYNC_VALUE_WAITING: u8 = 1;
const ASYNC_VALUE_SETTING: u8 = 2;
const ASYNC_VALUE_READY: u8 = 3;
const ASYNC_VALUE_TAKING: u8 = 4;
const ASYNC_VALUE_CONSUMED: u8 = 5;

/// 同步非阻塞的异步值，只允许被同步非阻塞设置一次值。
///
/// 说明：
/// - `AsyncValue` 是一个 single-shot future，`set()` 成功一次后，等待方可通过
///   `Future::poll` 取出该值。
/// - 本类型允许 pending 后重复 poll；重复 poll 会更新最新 waker，并继续返回
///   `Poll::Pending`。
/// - `set()` 保持旧语义：首次设置成功，后续设置静默失败且不会覆盖已设置的值。
/// - 当前 API 不表达 sender/receiver 拆分、关闭或取消语义；never set 的 future 会继续
///   pending。
/// - poll after ready 属于调用方违反 Future 契约，可能 panic。
///
/// 性能：
/// - `poll` 和 `set` 均为 O(1)，只在 CAS 竞争或极短取值窗口内有限重试。
/// - 不执行阻塞等待，不持有互斥锁，不在热路径分配队列节点。
///
/// 安全性：
/// - 内部 value 使用 `UnsafeCell<Option<V>>` 保存；只有成功进入 `SETTING` 的 setter
///   可以写入，只有成功进入 `TAKING` 的 receiver 可以取出。
/// - 状态转换使用原子 Acquire/Release/AcqRel 保证跨线程可见性。
pub struct AsyncValue<V: Send + 'static>(Arc<InnerAsyncValue<V>>);

unsafe impl<V: Send + 'static> Send for AsyncValue<V> {}
unsafe impl<V: Send + 'static> Sync for AsyncValue<V> {}

impl<V: Send + 'static> Clone for AsyncValue<V> {
    fn clone(&self) -> Self {
        AsyncValue(self.0.clone())
    }
}

impl<V: Send + 'static> Debug for AsyncValue<V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f,
               "AsyncValue[status = {}]",
               self.0.status.load(Ordering::Acquire))
    }
}

impl<V: Send + 'static> Future for AsyncValue<V> {
    type Output = V;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut spin_len = 1;
        loop {
            match self.0.status.load(Ordering::Acquire) {
                ASYNC_VALUE_EMPTY => {
                    self.0.waker.register(cx.waker());
                    match self.0.status.compare_exchange(ASYNC_VALUE_EMPTY,
                                                         ASYNC_VALUE_WAITING,
                                                         Ordering::AcqRel,
                                                         Ordering::Acquire) {
                        Ok(_) => {
                            return Poll::Pending;
                        },
                        Err(ASYNC_VALUE_EMPTY) => {
                            continue;
                        },
                        Err(ASYNC_VALUE_WAITING) | Err(ASYNC_VALUE_SETTING) => {
                            return Poll::Pending;
                        },
                        Err(ASYNC_VALUE_READY) => {
                            continue;
                        },
                        Err(ASYNC_VALUE_TAKING) => {
                            spin_len = spin(spin_len);
                            continue;
                        },
                        Err(ASYNC_VALUE_CONSUMED) => {
                            panic!("AsyncValue polled after completion");
                        },
                        Err(_) => {
                            panic!("AsyncValue entered invalid state");
                        },
                    }
                },
                ASYNC_VALUE_WAITING | ASYNC_VALUE_SETTING => {
                    self.0.waker.register(cx.waker());
                    match self.0.status.load(Ordering::Acquire) {
                        ASYNC_VALUE_READY => {
                            continue;
                        },
                        ASYNC_VALUE_TAKING => {
                            spin_len = spin(spin_len);
                            continue;
                        },
                        ASYNC_VALUE_CONSUMED => {
                            panic!("AsyncValue polled after completion");
                        },
                        _ => {
                            return Poll::Pending;
                        },
                    }
                },
                ASYNC_VALUE_READY => {
                    match self.0.status.compare_exchange(ASYNC_VALUE_READY,
                                                         ASYNC_VALUE_TAKING,
                                                         Ordering::AcqRel,
                                                         Ordering::Acquire) {
                        Ok(_) => {
                            let value = unsafe { (*self.0.value.get()).take().unwrap() };
                            self.0.status.store(ASYNC_VALUE_CONSUMED, Ordering::Release);
                            return Poll::Ready(value);
                        },
                        Err(ASYNC_VALUE_TAKING) => {
                            spin_len = spin(spin_len);
                            continue;
                        },
                        Err(ASYNC_VALUE_CONSUMED) => {
                            panic!("AsyncValue polled after completion");
                        },
                        Err(_) => {
                            continue;
                        },
                    }
                },
                ASYNC_VALUE_TAKING => {
                    //其它 clone 已经获得取值权，等待其完成状态推进，避免同时访问 value。
                    spin_len = spin(spin_len);
                    continue;
                },
                ASYNC_VALUE_CONSUMED => {
                    panic!("AsyncValue polled after completion");
                },
                _ => {
                    panic!("AsyncValue entered invalid state");
                },
            }
        }
    }
}

/*
* 同步非阻塞的异步值同步方法
*/
impl<V: Send + 'static> AsyncValue<V> {
    /// 构建异步值，默认值为未就绪
    pub fn new() -> Self {
        let inner = InnerAsyncValue {
            value: UnsafeCell::new(None),
            waker: AtomicWaker::new(),
            status: AtomicU8::new(ASYNC_VALUE_EMPTY),
        };

        AsyncValue(Arc::new(inner))
    }

    /// 判断异步值是否已完成设置
    pub fn is_complete(&self) -> bool {
        match self.0.status.load(Ordering::Acquire) {
            ASYNC_VALUE_READY | ASYNC_VALUE_TAKING | ASYNC_VALUE_CONSUMED => true,
            _ => false,
        }
    }

    /// 设置异步值
    pub fn set(self, value: V) {
        let mut value = Some(value);
        loop {
            match self.0.status.load(Ordering::Acquire) {
                ASYNC_VALUE_EMPTY => {
                    match self.0.status.compare_exchange(ASYNC_VALUE_EMPTY,
                                                         ASYNC_VALUE_SETTING,
                                                         Ordering::AcqRel,
                                                         Ordering::Acquire) {
                        Ok(_) => {
                            unsafe { *self.0.value.get() = value.take(); }
                            self.0.status.store(ASYNC_VALUE_READY, Ordering::Release);
                            self.0.waker.wake();
                            return;
                        },
                        Err(_) => {
                            continue;
                        },
                    }
                },
                ASYNC_VALUE_WAITING => {
                    match self.0.status.compare_exchange(ASYNC_VALUE_WAITING,
                                                         ASYNC_VALUE_SETTING,
                                                         Ordering::AcqRel,
                                                         Ordering::Acquire) {
                        Ok(_) => {
                            unsafe { *self.0.value.get() = value.take(); }
                            self.0.status.store(ASYNC_VALUE_READY, Ordering::Release);
                            self.0.waker.wake();
                            return;
                        },
                        Err(_) => {
                            continue;
                        },
                    }
                },
                _ => {
                    //异步值正在设置、已设置或已消费，则保持旧语义：重复 set 静默失败。
                    return;
                }
            }
        }
    }
}

// 同步非阻塞的内部异步值，只允许被同步非阻塞的设置一次值
pub struct InnerAsyncValue<V: Send + 'static> {
    value:  UnsafeCell<Option<V>>,  // 值，访问权由 status 的 SETTING/TAKING 状态独占保护。
    waker:  AtomicWaker,            // 最近一次 pending poll 注册的唤醒器。
    status: AtomicU8,               // 状态机，见 ASYNC_VALUE_* 常量。
}

///
/// 异步非阻塞可变值的守护者
///
pub struct AsyncVariableGuard<'a, V: Send + 'static> {
    value:  &'a UnsafeCell<Option<V>>,      //值
    waker:  &'a UnsafeCell<Option<Waker>>,  //唤醒器
    status: &'a AtomicU8,                   //值状态
}

unsafe impl<V: Send + 'static> Send for AsyncVariableGuard<'_, V> {}

impl<V: Send + 'static> Drop for AsyncVariableGuard<'_, V> {
    fn drop(&mut self) {
        //当前异步可变值已锁定，则解除锁定
        //当前异步可变值的状态为2或6，表示当前异步可变值的唤醒器未就绪并已锁定，或当前异步可变值不需要唤醒并已完成所有修改
        //当前异步可变值的状态为3或7，表示当前异步可变值的唤醒器已就绪并已锁定，或当前异步可变值已唤醒并已完成所有修改
        self.status.fetch_sub(2, Ordering::Relaxed);
    }
}

impl<V: Send + 'static> Deref for AsyncVariableGuard<'_, V> {
    type Target = Option<V>;

    fn deref(&self) -> &Self::Target {
        unsafe {
            &*self.value.get()
        }
    }
}

impl<V: Send + 'static> DerefMut for AsyncVariableGuard<'_, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            &mut *self.value.get()
        }
    }
}

impl<V: Send + 'static> AsyncVariableGuard<'_, V> {
    /// 完成异步可变值的修改
    pub fn finish(self) {
        //设置异步可变值的状态为已完成修改
        if self.status.fetch_add(4, Ordering::Relaxed) == 3 {
            if let Some(waker) = unsafe { (&mut *self.waker.get()).take() } {
                //当前异步可变值需要唤醒，则立即唤醒异步可变值
                waker.wake();
            }
        }
    }
}

///
/// 异步非阻塞可变值，在完成前允许被同步非阻塞的修改多次
///
pub struct AsyncVariable<V: Send + 'static>(Arc<InnerAsyncVariable<V>>);

unsafe impl<V: Send + 'static> Send for AsyncVariable<V> {}
unsafe impl<V: Send + 'static> Sync for AsyncVariable<V> {}

impl<V: Send + 'static> Clone for AsyncVariable<V> {
    fn clone(&self) -> Self {
        AsyncVariable(self.0.clone())
    }
}

impl<V: Send + 'static> Future for AsyncVariable<V> {
    type Output = V;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe {
            *self.0.waker.get() = Some(cx.waker().clone()); //设置异步可变值的唤醒器准备就绪
        }

        let mut spin_len = 1;
        loop {
            match self.0.status.compare_exchange(0,
                                                 1,
                                                 Ordering::Acquire,
                                                 Ordering::Relaxed) {
                Err(current) if current & 4 != 0 => {
                    //异步可变值已完成所有修改，则立即返回
                    unsafe {
                        let _ = (&mut *self.0.waker.get()).take(); //释放异步可变值的唤醒器
                        return Poll::Ready((&mut *(&self).0.value.get()).take().unwrap());
                    }
                },
                Err(_) => {
                    //还未完成值修改，则自旋等待
                    spin_len = spin(spin_len);
                },
                Ok(_) => {
                    //异步可变值已挂起
                    return Poll::Pending;
                },
            }
        }
    }
}

impl<V: Send + 'static> AsyncVariable<V> {
    /// 构建异步可变值，默认值为未就绪
    pub fn new() -> Self {
        let inner = InnerAsyncVariable {
            value: UnsafeCell::new(None),
            waker: UnsafeCell::new(None),
            status: AtomicU8::new(0),
        };

        AsyncVariable(Arc::new(inner))
    }

    /// 判断异步可变值是否已完成设置
    pub fn is_complete(&self) -> bool {
        self
            .0
            .status
            .load(Ordering::Acquire) & 4 != 0
    }

    /// 锁住待修改的异步可变值，并返回当前异步可变值的守护者，如果异步可变值已完成修改则返回空
    pub fn lock(&self) -> Option<AsyncVariableGuard<V>> {
        let mut spin_len = 1;
        loop {
            match self
                .0
                .status
                .compare_exchange(1,
                                  3,
                                  Ordering::Acquire,
                                  Ordering::Relaxed) {
                Err(0) => {
                    //异步可变值还未就绪，则自旋等待
                    match self
                        .0
                        .status
                        .compare_exchange(0,
                                          2,
                                          Ordering::Acquire,
                                          Ordering::Relaxed) {
                        Err(1) => {
                            //异步可变值已就绪，则继续尝试获取锁
                            continue;
                        },
                        Err(2) => {
                            //异步可变值的唤醒器未就绪且已锁，但未获取到锁，则自旋等待
                            spin_len = spin(spin_len);
                        },
                        Err(3) => {
                            //异步可变值的唤醒器已就绪且已锁，但未获取到锁，则自旋等待
                            spin_len = spin(spin_len);
                        },
                        Err(_) => {
                            //已完成，则返回空
                            return None;
                        },
                        Ok(_) => {
                            //异步可变值的唤醒器未就绪且获取到锁，则返回异步可变值的守护者
                            let guard = AsyncVariableGuard {
                                value: &self.0.value,
                                waker: &self.0.waker,
                                status: &self.0.status,
                            };

                            return Some(guard)
                        },
                    }
                },
                Err(2) => {
                    //异步可变值的唤醒器未就绪且已锁，但未获取到锁，则自旋等待
                    spin_len = spin(spin_len);
                },
                Err(3) => {
                    //异步可变值的唤醒器已就绪且已锁，但未获取到锁，则自旋等待
                    spin_len = spin(spin_len);
                },
                Err(_) => {
                    //已完成，则返回空
                    return None;
                }
                Ok(_) => {
                    //异步可变值的唤醒器已就绪且获取到锁，则返回异步可变值的守护者
                    let guard = AsyncVariableGuard {
                        value: &self.0.value,
                        waker: &self.0.waker,
                        status: &self.0.status,
                    };

                    return Some(guard)
                },
            }
        }
    }
}

// 内部异步非阻塞可变值，在完成前允许被同步非阻塞的修改多次
pub struct InnerAsyncVariable<V: Send + 'static> {
    value:  UnsafeCell<Option<V>>,      //值
    waker:  UnsafeCell<Option<Waker>>,  //唤醒器
    status: AtomicU8,                   //状态
}

///
/// 等待异步任务运行的结果
///
pub struct AsyncWaitResult<V: Send + 'static>(pub Arc<RefCell<Option<Result<V>>>>);

unsafe impl<V: Send + 'static> Send for AsyncWaitResult<V> {}
unsafe impl<V: Send + 'static> Sync for AsyncWaitResult<V> {}

impl<V: Send + 'static> Clone for AsyncWaitResult<V> {
    fn clone(&self) -> Self {
        AsyncWaitResult(self.0.clone())
    }
}

///
/// 等待异步任务运行的结果集
///
pub struct AsyncWaitResults<V: Send + 'static>(pub Arc<RefCell<Option<Vec<Result<V>>>>>);

unsafe impl<V: Send + 'static> Send for AsyncWaitResults<V> {}
unsafe impl<V: Send + 'static> Sync for AsyncWaitResults<V> {}

impl<V: Send + 'static> Clone for AsyncWaitResults<V> {
    fn clone(&self) -> Self {
        AsyncWaitResults(self.0.clone())
    }
}

///
/// 异步定时器任务
///
pub enum AsyncTimingTask<
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static = (),
> {
    Pended(TaskId),                     //已挂起的定时任务
    WaitRun(Arc<AsyncTask<P, O>>),      //等待执行的定时任务
    TimeoutWake(Arc<TimeoutWaiter>),    //等待timeout到期的唤醒句柄
}

///
/// 异步任务本地定时器
///
pub struct AsyncTaskTimer<
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static = (),
> {
    producor:   Sender<(usize, AsyncTimingTask<P, O>)>,                     //定时任务生产者
    consumer:   Receiver<(usize, AsyncTimingTask<P, O>)>,                   //定时任务消费者
    timer:      Arc<RefCell<Timer<AsyncTimingTask<P, O>, 1000, 60, 3>>>,    //定时器
    clock:      Clock,                                                      //定时器时钟
    now:        QInstant,                                                   //当前时间
}

unsafe impl<
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> Send for AsyncTaskTimer<P, O> {}
unsafe impl<
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> Sync for AsyncTaskTimer<P, O> {}

impl<
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> AsyncTaskTimer<P, O> {
    /// 构建异步任务本地定时器
    pub fn new() -> Self {
        let (producor, consumer) = unbounded();
        let clock = Clock::new();
        let now = clock.recent();

        AsyncTaskTimer {
            producor,
            consumer,
            timer: Arc::new(RefCell::new(Timer::<AsyncTimingTask<P, O>, 1000, 60, 3>::default())),
            clock,
            now,
        }
    }

    /// 获取定时任务生产者
    #[inline]
    pub fn get_producor(&self) -> &Sender<(usize, AsyncTimingTask<P, O>)> {
        &self.producor
    }

    /// 获取剩余未到期的定时器任务数量
    #[inline]
    pub fn len(&self) -> usize {
        let timer = self.timer.as_ref().borrow();
        timer.add_count() - timer.remove_count()
    }

    /// 设置定时器
    pub fn set_timer(&self, task: AsyncTimingTask<P, O>, timeout: usize) -> usize {
        let current_time = self
            .clock
            .recent()
            .duration_since(self.now)
            .as_millis() as u64;
        self
            .timer
            .borrow_mut()
            .push_time(current_time + timeout as u64, task)
            .data()
            .as_ffi() as usize
    }

    /// 取消定时器
    pub fn cancel_timer(&self, timer_ref: usize) -> Option<AsyncTimingTask<P, O>> {
        if let Some(item) = self
            .timer
            .borrow_mut()
            .cancel(KeyData::from_ffi(timer_ref as u64).into()) {
            Some(item)
        } else {
            None
        }
    }

    /// 消费所有定时任务，返回定时任务数量
    pub fn consume(&self) -> usize {
        let timer_tasks = self.consumer.try_iter().collect::<Vec<(usize, AsyncTimingTask<P, O>)>>();
        let len = timer_tasks.len();
        for (timeout, task) in timer_tasks {
            self.set_timer(task, timeout);
        }

        len
    }

    /// 判断当前时间是否有可以弹出的任务，如果有可以弹出的任务，则返回当前时间，否则返回空
    pub fn is_require_pop(&self) -> Option<u64> {
        let current_time = self
            .clock
            .recent()
            .duration_since(self.now)
            .as_millis() as u64;
        if self.timer.borrow_mut().is_ok(current_time) {
            Some(current_time)
        } else {
            None
        }
    }

    /// 从定时器中弹出指定时间的一个到期任务
    pub fn pop(&self, current_time: u64) -> Option<(usize, AsyncTimingTask<P, O>)> {
        if let Some((key, item)) = self.timer.borrow_mut().pop_kv(current_time) {
            Some((key.data().as_ffi() as usize, item))
        } else {
            None
        }
    }
}

///
/// 异步任务本地定时器，不支持取消定时任务
///
pub struct AsyncTaskTimerByNotCancel<
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static = (),
> {
    producor:   Sender<(usize, AsyncTimingTask<P, O>)>,                             //定时任务生产者
    consumer:   Receiver<(usize, AsyncTimingTask<P, O>)>,                           //定时任务消费者
    timer:      Arc<RefCell<NotCancelTimer<AsyncTimingTask<P, O>, 1000, 60, 3>>>,   //定时器
    clock:      Clock,                                                              //定时器时钟
    now:        QInstant,                                                           //当前时间
}

unsafe impl<
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> Send for AsyncTaskTimerByNotCancel<P, O> {}
unsafe impl<
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> Sync for AsyncTaskTimerByNotCancel<P, O> {}

impl<
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> AsyncTaskTimerByNotCancel<P, O> {
    /// 构建异步任务本地定时器
    pub fn new() -> Self {
        let (producor, consumer) = unbounded();
        let clock = Clock::new();
        let now = clock.recent();

        AsyncTaskTimerByNotCancel {
            producor,
            consumer,
            timer: Arc::new(RefCell::new(NotCancelTimer::<AsyncTimingTask<P, O>, 1000, 60, 3>::default())),
            clock,
            now,
        }
    }

    /// 获取定时任务生产者
    #[inline]
    pub fn get_producor(&self) -> &Sender<(usize, AsyncTimingTask<P, O>)> {
        &self.producor
    }

    /// 获取剩余未到期的定时器任务数量
    #[inline]
    pub fn len(&self) -> usize {
        let timer = self.timer.as_ref().borrow();
        timer.add_count() - timer.remove_count()
    }

    /// 设置定时器
    pub fn set_timer(&self, task: AsyncTimingTask<P, O>, timeout: usize) {
        self
            .timer
            .borrow_mut()
            .push(timeout, task);
    }

    /// 消费所有定时任务，返回定时任务数量
    pub fn consume(&self) -> usize {
        let timer_tasks = self.consumer.try_iter().collect::<Vec<(usize, AsyncTimingTask<P, O>)>>();
        let len = timer_tasks.len();
        for (timeout, task) in timer_tasks {
            self.set_timer(task, timeout);
        }

        len
    }

    /// 判断当前时间是否有可以弹出的任务，如果有可以弹出的任务，则返回当前时间，否则返回空
    pub fn is_require_pop(&self) -> Option<u64> {
        let current_time = self
            .clock
            .recent()
            .duration_since(self.now)
            .as_millis() as u64;
        if self.timer.borrow_mut().is_ok(current_time) {
            Some(current_time)
        } else {
            None
        }
    }

    /// 从定时器中弹出指定时间的一个到期任务
    pub fn pop(&self, current_time: u64) -> Option<AsyncTimingTask<P, O>> {
        if let Some(item) = self.timer.borrow_mut().pop(current_time) {
            Some(item)
        } else {
            None
        }
    }
}

///
/// 等待指定超时
///
pub struct AsyncWaitTimeout<
    RT: AsyncRuntime<O>,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static = (),
> {
    rt:         RT,                                     //当前运行时
    producor:   Sender<(usize, AsyncTimingTask<P, O>)>, //超时请求生产者
    timeout:    usize,                                  //超时时长，单位ms
    registered: AtomicBool,                             //是否已注册到定时器
    waiter:     Arc<TimeoutWaiter>,                     //timeout专用等待句柄
}

unsafe impl<
    RT: AsyncRuntime<O>,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> Send for AsyncWaitTimeout<RT, P, O> {}
unsafe impl<
    RT: AsyncRuntime<O>,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> Sync for AsyncWaitTimeout<RT, P, O> {}

impl<
    RT: AsyncRuntime<O>,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> Future for AsyncWaitTimeout<RT, P, O> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.waiter.is_fired() {
            //已到期，则返回
            return Poll::Ready(());
        }

        self.waiter.register(cx.waker());

        if !self.registered.swap(true, Ordering::AcqRel) {
            //发送超时请求，并返回
            let _ = self
                .producor
                .send((self.timeout, AsyncTimingTask::TimeoutWake(self.waiter.clone())));
        }

        if self.waiter.is_fired() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl<
    RT: AsyncRuntime<O>,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> Drop for AsyncWaitTimeout<RT, P, O> {
    fn drop(&mut self) {
        self.waiter.clear_waker();
    }
}

impl<
    RT: AsyncRuntime<O>,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> AsyncWaitTimeout<RT, P, O> {
    /// 构建等待指定超时任务的方法
    pub fn new(rt: RT,
               producor: Sender<(usize, AsyncTimingTask<P, O>)>,
               timeout: usize) -> Self {
        AsyncWaitTimeout {
            rt,
            producor,
            timeout,
            registered: AtomicBool::new(false), //设置初始值
            waiter: Arc::new(TimeoutWaiter::new()),
        }
    }
}

///
/// 本地等待指定超时
///
pub struct LocalAsyncWaitTimeout<
    RT: AsyncRuntime<O>,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static = (),
> {
    rt:         RT,                                     //当前运行时
    timer:      Arc<AsyncTaskTimerByNotCancel<P, O>>,   //定时器
    timeout:    usize,                                  //超时时长，单位ms
    registered: AtomicBool,                             //是否已注册到定时器
    waiter:     Arc<TimeoutWaiter>,                     //timeout专用等待句柄
}

unsafe impl<
    RT: AsyncRuntime<O>,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> Send for LocalAsyncWaitTimeout<RT, P, O> {}
unsafe impl<
    RT: AsyncRuntime<O>,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> Sync for LocalAsyncWaitTimeout<RT, P, O> {}

impl<
    RT: AsyncRuntime<O>,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> Future for LocalAsyncWaitTimeout<RT, P, O> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.waiter.is_fired() {
            //已到期，则返回
            return Poll::Ready(());
        }

        self.waiter.register(cx.waker());

        if !self.registered.swap(true, Ordering::AcqRel) {
            //设置本地超时请求，并返回
            self
                .timer
                .set_timer(AsyncTimingTask::TimeoutWake(self.waiter.clone()),
                           self.timeout);
        }

        if self.waiter.is_fired() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl<
    RT: AsyncRuntime<O>,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> Drop for LocalAsyncWaitTimeout<RT, P, O> {
    fn drop(&mut self) {
        self.waiter.clear_waker();
    }
}

impl<
    RT: AsyncRuntime<O>,
    P: AsyncTaskPoolExt<O> + AsyncTaskPool<O>,
    O: Default + 'static,
> LocalAsyncWaitTimeout<RT, P, O> {
    /// 构建等待指定超时任务的方法
    pub fn new(rt: RT,
               timer: Arc<AsyncTaskTimerByNotCancel<P, O>>,
               timeout: usize) -> Self {
        LocalAsyncWaitTimeout {
            rt,
            timer,
            timeout,
            registered: AtomicBool::new(false), //设置初始值
            waiter: Arc::new(TimeoutWaiter::new()),
        }
    }
}

///
/// 等待异步任务执行完成
///
pub struct AsyncWait<V: Send + 'static>(AsyncWaitAny<V>);

unsafe impl<V: Send + 'static> Send for AsyncWait<V> {}
unsafe impl<V: Send + 'static> Sync for AsyncWait<V> {}

/*
* 等待异步任务执行完成同步方法
*/
impl<V: Send + 'static> AsyncWait<V> {
    /// 派发指定超时时间的指定任务到指定的运行时，并返回派发是否成功
    pub fn spawn<RT, O, F>(&self,
                           rt: RT,
                           timeout: Option<usize>,
                           future: F) -> Result<()>
        where RT: AsyncRuntime<O>,
              O: Default + 'static,
              F: Future<Output = Result<V>> + Send + 'static {
        self.0.spawn(rt.clone(), future)?;

        if let Some(timeout) = timeout {
            //设置了超时时间
            let rt_copy = rt.clone();
            self.0.spawn(rt, async move {
                rt_copy.timeout(timeout).await;

                //返回超时错误
                Err(Error::new(ErrorKind::TimedOut, format!("Time out")))
            })
        } else {
            //未设置超时时间
            Ok(())
        }
    }

    /// 派发指定超时时间的指定任务到本地运行时，并返回派发是否成功
    pub fn spawn_local<O, F>(&self,
                             timeout: Option<usize>,
                             future: F) -> Result<()>
        where O: Default + 'static,
              F: Future<Output = Result<V>> + Send + 'static {
        if let Some(rt) = local_async_runtime::<O>() {
            //当前线程有绑定运行时
            self.0.spawn_local(future)?;

            if let Some(timeout) = timeout {
                //设置了超时时间
                let rt_copy = rt.clone();
                self.0.spawn_local(async move {
                    rt_copy.timeout(timeout).await;

                    //返回超时错误
                    Err(Error::new(ErrorKind::TimedOut, format!("Time out")))
                })
            } else {
                //未设置超时时间
                Ok(())
            }
        } else {
            //当前线程未绑定运行时
            Err(Error::new(ErrorKind::Other, format!("Spawn wait task failed, reason: local async runtime not exist")))
        }
    }
}

/*
* 等待异步任务执行完成异步方法
*/
impl<V: Send + 'static> AsyncWait<V> {
    /// 异步等待已派发任务的结果
    pub async fn wait_result(self) -> Result<V> {
        self.0.wait_result().await
    }
}

///
/// 等待任意异步任务执行完成
///
pub struct AsyncWaitAny<V: Send + 'static> {
    capacity:       usize,                      //派发任务的容量
    producor:       AsyncSender<Result<V>>,     //异步返回值生成器
    consumer:       AsyncReceiver<Result<V>>,   //异步返回值接收器
}

unsafe impl<V: Send + 'static> Send for AsyncWaitAny<V> {}
unsafe impl<V: Send + 'static> Sync for AsyncWaitAny<V> {}

/*
* 等待任意异步任务执行完成同步方法
*/
impl<V: Send + 'static> AsyncWaitAny<V> {
    /// 派发指定任务到指定的运行时，并返回派发是否成功
    pub fn spawn<RT, O, F>(&self,
                           rt: RT,
                           future: F) -> Result<()>
        where RT: AsyncRuntime<O>,
              O: Default + 'static,
              F: Future<Output = Result<V>> + Send + 'static {
        let producor = self.producor.clone();
        rt.spawn_by_id(rt.alloc::<O>(), async move {
            let value = future.await;
            producor.into_send_async(value).await;

            //返回异步任务的默认值
            Default::default()
        })
    }

    /// 派发指定任务到本地运行时，并返回派发是否成功
    pub fn spawn_local<F>(&self,
                          future: F) -> Result<()>
        where F: Future<Output = Result<V>> + Send + 'static {
        if let Some(rt) = local_async_runtime() {
            //本地线程有绑定运行时
            let producor = self.producor.clone();
            rt.spawn(async move {
                let value = future.await;
                producor.into_send_async(value).await;
            })
        } else {
            //本地线程未绑定运行时
            Err(Error::new(ErrorKind::Other, format!("Spawn wait any task failed, reason: local async runtime not exist")))
        }
    }
}

/*
* 等待任意异步任务执行完成异步方法
*/
impl<V: Send + 'static> AsyncWaitAny<V> {
    /// 异步等待任意已派发任务的结果
    pub async fn wait_result(self) -> Result<V> {
        match self.consumer.recv_async().await {
            Err(e) => {
                //接收错误，则立即返回
                Err(Error::new(ErrorKind::Other, format!("Wait any result failed, reason: {:?}", e)))
            },
            Ok(result) => {
                //接收成功，则立即返回
                result
            },
        }
    }
}

///
/// 等待任意异步任务执行完成
///
pub struct AsyncWaitAnyCallback<V: Send + 'static> {
    capacity:   usize,                      //派发任务的容量
    producor:   AsyncSender<Result<V>>,     //异步返回值生成器
    consumer:   AsyncReceiver<Result<V>>,   //异步返回值接收器
}

unsafe impl<V: Send + 'static> Send for AsyncWaitAnyCallback<V> {}
unsafe impl<V: Send + 'static> Sync for AsyncWaitAnyCallback<V> {}

/*
* 等待任意异步任务执行完成同步方法
*/
impl<V: Send + 'static> AsyncWaitAnyCallback<V> {
    /// 派发指定任务到指定的运行时，并返回派发是否成功
    pub fn spawn<RT, O, F>(&self,
                           rt: RT,
                           future: F) -> Result<()>
        where RT: AsyncRuntime<O>,
              O: Default + 'static,
              F: Future<Output = Result<V>> + Send + 'static {
        let producor = self.producor.clone();
        rt.spawn_by_id(rt.alloc::<O>(), async move {
            let value = future.await;
            producor.into_send_async(value).await;

            //返回异步任务的默认值
            Default::default()
        })
    }

    /// 派发指定任务到本地运行时，并返回派发是否成功
    pub fn spawn_local<F>(&self,
                          future: F) -> Result<()>
        where F: Future<Output = Result<V>> + Send + 'static {
        if let Some(rt) = local_async_runtime() {
            //当前线程有绑定运行时
            let producor = self.producor.clone();
            rt.spawn(async move {
                let value = future.await;
                producor.into_send_async(value).await;
            })
        } else {
            //当前线程未绑定运行时
            Err(Error::new(ErrorKind::Other, format!("Spawn wait any task failed by callback, reason: current async runtime not exist")))
        }
    }
}

/*
* 等待任意异步任务执行完成异步方法
*/
impl<V: Send + 'static> AsyncWaitAnyCallback<V> {
    /// 异步等待满足用户回调需求的已派发任务的结果
    pub async fn wait_result(mut self,
                             callback: impl Fn(&Result<V>) -> bool + Send + Sync + 'static) -> Result<V> {
        let checker = create_checker(self.capacity, callback);
        loop {
            match self.consumer.recv_async().await {
                Err(e) => {
                    //接收错误，则立即返回
                    return Err(Error::new(ErrorKind::Other, format!("Wait any result failed by callback, reason: {:?}", e)));
                },
                Ok(result) => {
                    //接收成功，则检查是否立即返回
                    if checker(&result) {
                        //检查通过，则立即唤醒等待的任务，否则等待其它任务唤醒
                        return result;
                    }
                },
            }
        }
    }
}

// 根据用户提供的回调，生成检查器
fn create_checker<V, F>(len: usize,
                        callback: F) -> Arc<dyn Fn(&Result<V>) -> bool + Send + Sync + 'static>
    where V: Send + 'static,
          F: Fn(&Result<V>) -> bool + Send + Sync + 'static {
    let mut check_counter = AtomicUsize::new(len); //初始化检查计数器
    Arc::new(move |result| {
        if check_counter.fetch_sub(1, Ordering::SeqCst) == 1 {
            //最后一个任务的检查，则忽略用户回调，并立即返回成功
            true
        } else {
            //不是最后一个任务的检查，则调用用户回调，并根据用户回调确定是否成功
            callback(result)
        }
    })
}

///
/// 异步映射归并
///
pub struct AsyncMapReduce<V: Send + 'static> {
    count:          usize,                              //派发的任务数量
    capacity:       usize,                              //派发任务的容量
    producor:       AsyncSender<(usize, Result<V>)>,    //异步返回值生成器
    consumer:       AsyncReceiver<(usize, Result<V>)>,  //异步返回值接收器
}

unsafe impl<V: Send + 'static> Send for AsyncMapReduce<V> {}

/*
* 异步映射归并同步方法
*/
impl<V: Send + 'static> AsyncMapReduce<V> {
    /// 映射指定任务到指定的运行时，并返回任务序号
    pub fn map<RT, O, F>(&mut self, rt: RT, future: F) -> Result<usize>
        where RT: AsyncRuntime<O>,
              O: Default + 'static,
              F: Future<Output = Result<V>> + Send + 'static {
        if self.count >= self.capacity {
            //已派发任务已达可派发任务的限制，则返回错误
            return Err(Error::new(ErrorKind::Other, format!("Map task to runtime failed, capacity: {}, reason: out of capacity", self.capacity)));
        }

        let index = self.count;
        let producor = self.producor.clone();
        rt.spawn_by_id(rt.alloc::<O>(), async move {
            let value = future.await;
            producor.into_send_async((index, value)).await;

            //返回异步任务的默认值
            Default::default()
        })?;

        self.count += 1; //派发任务成功，则计数
        Ok(index)
    }
}

/*
* 异步映射归并异步方法
*/
impl<V: Send + 'static> AsyncMapReduce<V> {
    /// 归并所有派发的任务
    pub async fn reduce(self, order: bool) -> Result<Vec<Result<V>>> {
        let mut count = self.count;
        let mut results = Vec::with_capacity(count);
        while count > 0 {
            match self.consumer.recv_async().await {
                Err(e) => {
                    //接收错误，则立即返回
                    return Err(Error::new(ErrorKind::Other, format!("Reduce result failed, reason: {:?}", e)));
                },
                Ok((index, result)) => {
                    //接收成功，则继续
                    results.push((index, result));
                    count -= 1;
                },
            }
        }

        if order {
            //需要对结果集进行排序
            results.sort_by_key(|(key, _value)| {
                key.clone()
            });
        }
        let (_, values) = results
            .into_iter()
            .unzip::<usize, Result<V>, Vec<usize>, Vec<Result<V>>>();

        Ok(values)
    }
}

///
/// 异步管道过滤器结果
///
pub enum AsyncPipelineResult<O: 'static> {
    Disconnect,     //关闭管道
    Filtered(O),    //过滤后的值
}

///
/// 派发一个工作线程
/// 返回线程的句柄，可以通过句柄关闭线程
/// 线程在没有任务可以执行时会休眠，当派发任务或唤醒任务时会自动唤醒线程
///
pub fn spawn_worker_thread<F0, F1>(thread_name: &str,
                                   thread_stack_size: usize,
                                   thread_handler: Arc<AtomicBool>,
                                   thread_waker: Arc<(AtomicBool, Mutex<()>, Condvar)>, //用于唤醒运行时所在线程的条件变量
                                   sleep_timeout: u64,                                  //休眠超时时长，单位毫秒
                                   loop_interval: Option<u64>,                          //工作者线程循环的间隔时长，None为无间隔，单位毫秒
                                   loop_func: F0,
                                   get_queue_len: F1) -> Arc<AtomicBool>
    where F0: Fn() -> (bool, Duration) + Send + 'static,
          F1: Fn() -> usize + Send + 'static {
    let thread_status_copy = thread_handler.clone();

    thread::Builder::new()
        .name(thread_name.to_string())
        .stack_size(thread_stack_size).spawn(move || {
        let mut sleep_count = 0;

        while thread_handler.load(Ordering::Relaxed) {
            let (is_no_task, run_time) = loop_func();

            if is_no_task {
                //当前没有任务
                if sleep_count > 1 {
                    //当前没有任务连续达到2次，则休眠线程
                    sleep_count = 0; //重置休眠计数
                    let (is_sleep, lock, condvar) = &*thread_waker;
                    if get_queue_len() > 0 {
                        //当前有任务，则继续工作
                        continue;
                    }

                    {
                        let _locked = lock.lock();
                        if !is_sleep.load(Ordering::Acquire) {
                            //发布休眠状态，外部唤醒端会在同一把锁内确认后再notify
                            is_sleep.store(true, Ordering::Release);
                        }
                    }

                    if get_queue_len() > 0 {
                        //发布休眠后再次检查任务，避免外部唤醒落在发布窗口内
                        is_sleep.store(false, Ordering::Release);
                        continue;
                    }

                    let mut locked = lock.lock();
                    if is_sleep.load(Ordering::Acquire) {
                        let _ = condvar.wait_for(
                            &mut locked,
                            Duration::from_millis(sleep_timeout),
                        );
                    }
                    is_sleep.store(false, Ordering::Release);

                    continue; //唤醒后立即尝试执行任务
                }

                sleep_count += 1; //休眠计数
                if let Some(interval) = &loop_interval {
                    //设置了循环间隔时长
                    if let Some(remaining_interval) = Duration::from_millis(*interval).checked_sub(run_time){
                        //本次运行少于循环间隔，则休眠剩余的循环间隔，并继续执行任务
                        thread::sleep(remaining_interval);
                    }
                }
            } else {
                //当前有任务
                sleep_count = 0; //重置休眠计数
                if let Some(interval) = &loop_interval {
                    //设置了循环间隔时长
                    if let Some(remaining_interval) = Duration::from_millis(*interval).checked_sub(run_time){
                        //本次运行少于循环间隔，则休眠剩余的循环间隔，并继续执行任务
                        thread::sleep(remaining_interval);
                    }
                }
            }
        }
    });

    thread_status_copy
}

/// 唤醒工作者所在线程，如果线程当前正在运行，则忽略
pub fn wakeup_worker_thread<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>(worker_waker: &Arc<(AtomicBool, Mutex<()>, Condvar)>, rt: &SingleTaskRuntime<O, P>) {
    //检查工作者所在线程是否需要唤醒
    if worker_waker.0.load(Ordering::Relaxed) && rt.len() > 0 {
        let _ = wake_thread_waker(worker_waker);
    }
}

/// 注册全局异常处理器，会替换当前全局异常处理器
pub fn register_global_panic_handler<Handler>(handler: Handler)
    where Handler: Fn(thread::Thread, String, Option<String>, Option<(String, u32, u32)>) -> Option<i32> + Send + Sync + 'static {
    set_hook(Box::new(move |panic_info| {
        let thread_info = thread::current();

        let payload = panic_info.payload();
        let payload_info = match payload.downcast_ref::<&str>() {
            None => {
                //不是String
                match payload.downcast_ref::<String>() {
                    None => {
                        //不是&'static str，则返回未知异常
                        "Unknow panic".to_string()
                    },
                    Some(info) => {
                        info.clone()
                    }
                }
            },
            Some(info) => {
                info.to_string()
            }
        };

        let other_info = if let Some(arg) = panic_info.payload_as_str() {
            Some(arg.to_string())
        } else {
            None
        };

        let location = if let Some(location) = panic_info.location() {
            Some((location.file().to_string(), location.line(), location.column()))
        } else {
            None
        };

        if let Some(exit_code) = handler(thread_info, payload_info, other_info, location) {
            //需要关闭当前进程
            std::process::exit(exit_code);
        }
    }));
}

/// 替换全局内存分配错误处理器
pub fn replace_global_alloc_error_handler() {
    set_alloc_error_hook(global_alloc_error_handle);
}

fn global_alloc_error_handle(layout: Layout) {
    let bt = Backtrace::new();
    eprintln!("[UTC: {}][Thread: {}]Global memory allocation of {:?} bytes failed, stacktrace: \n{:?}",
              SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis(),
              thread::current().name().unwrap_or(""),
              layout.size(),
              bt);
}

// 立即异步让出当前任务执行
pub(crate) struct YieldNow(bool);

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
