//! 运行时（runtime）构造时分别选用三类队列（queue）。
use crate::instance::Timer;
use pi_async_executor::{DefaultTimerDriver, ExecutorClock, ExecutorTimer, ManualClock, ThreadNotifier};
use pi_async_runtime_core::RuntimeId;
use pi_async_task_queue::{
    CancelTimerOptions, ExternalQueueManager, IdleQueueManager, MpscIdleTaskQueue,
    MpscQueueOptions, MpscTaskQueue,
};
use pi_clock_manual::global::GlobalManualInstant;
use std::marker::PhantomData;

pub(crate) enum QueueSelection<Q> {
    Default(fn(MpscQueueOptions) -> pi_result::Result<Q>),
    Specified(Q),
}

type MakeDefaultTimer<C> =
    fn(RuntimeId, &C, ThreadNotifier, CancelTimerOptions) -> pi_result::Result<Timer<C>>;

/// 分别选择外部任务队列（external task queue）、空闲任务队列（idle task queue）和定时任务队列（timer task queue）。
/// 缺项使用原默认。构造运行时（runtime）时移交同一实例，执行器（executor）绑定对应队列。
pub struct RuntimeQueues<
    C: ExecutorClock<Instant = GlobalManualInstant> = ManualClock,
    E = MpscTaskQueue,
    I = MpscIdleTaskQueue,
    F = MakeDefaultTimer<C>,
> {
    pub(crate) external: QueueSelection<E>,
    pub(crate) idle: QueueSelection<I>,
    pub(crate) timer: F,
    clock: PhantomData<fn() -> C>,
}

impl<C: ExecutorClock<Instant = GlobalManualInstant>> Default for RuntimeQueues<C> {
    fn default() -> Self {
        Self {
            external: QueueSelection::Default(MpscTaskQueue::new),
            idle: QueueSelection::Default(MpscIdleTaskQueue::new),
            timer: make_default_timer::<C>,
            clock: PhantomData,
        }
    }
}

fn make_default_timer<C: ExecutorClock<Instant = GlobalManualInstant>>(
    runtime: RuntimeId,
    clock: &C,
    notifier: ThreadNotifier,
    options: CancelTimerOptions,
) -> pi_result::Result<Timer<C>> {
    DefaultTimerDriver::new(runtime, clock.clone(), notifier, options)
}

impl<C: ExecutorClock<Instant = GlobalManualInstant>, E, I, F> RuntimeQueues<C, E, I, F> {
    /// 移交外部任务队列（external task queue）的管理者。已有共享访问按队列原约定使用。
    /// 经运行时（runtime）派发时由运行时核对任务归属并安排通知（notification）。
    pub fn with_external<Q: ExternalQueueManager>(self, queue: Q) -> RuntimeQueues<C, Q, I, F> {
        RuntimeQueues {
            external: QueueSelection::Specified(queue),
            idle: self.idle,
            timer: self.timer,
            clock: PhantomData,
        }
    }

    /// 移交空闲任务队列（idle task queue）的管理者。空闲判定仍由原执行器（executor）负责。
    /// 已有共享访问按队列原约定使用，经运行时派发时由运行时负责归属和通知（notification）。
    pub fn with_idle<Q: IdleQueueManager>(self, queue: Q) -> RuntimeQueues<C, E, Q, F> {
        RuntimeQueues {
            external: self.external,
            idle: QueueSelection::Specified(queue),
            timer: self.timer,
            clock: PhantomData,
        }
    }

    /// 使用一次原计时构造回调准备所选实例。回调使用本次实际身份、时钟（clock）和通知状态。
    /// 回调返回的拥有者在创建线程（thread）交给原执行器。失败时保留原报告（Report）。
    pub fn with_timer<T, B>(
        self,
        make: B,
    ) -> RuntimeQueues<C, E, I, impl FnOnce(RuntimeId, &C, ThreadNotifier, CancelTimerOptions) -> pi_result::Result<T>>
    where
        T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
        B: FnOnce(RuntimeId, &C, ThreadNotifier) -> pi_result::Result<T>,
    {
        RuntimeQueues {
            external: self.external,
            idle: self.idle,
            timer: move |runtime, clock: &C, notifier, _options| make(runtime, clock, notifier),
            clock: PhantomData,
        }
    }
}
