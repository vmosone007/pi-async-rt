//! 计时、只读查询与关闭控制的直接组合，不复制状态或启动后台服务。
use crate::{instance::*, mode::RuntimeMode};
use pi_async_executor::{ExecutorClock, ExecutorHooks, IdlePolicy};
use pi_async_runtime_core::{
    CloseRequestStatus, CloseState, IdleState, RuntimeControl, RuntimeId, RuntimeQuery,
    RuntimeStatus, RuntimeTime, RuntimeWait, Sleep, Timeout, TimeoutOn,
};
use pi_clock_manual::global::GlobalManualInstant;
use std::{future::Future, time::Duration};
impl<C> RuntimeTime for RuntimeHandle<C>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
{
    type Timer = RuntimeTimerAccess<C>;
    fn timeout(&self, duration: Duration) -> Timeout<Self::Timer> {
        Timeout::new(self.shared().inner.timer_access(), duration)
    }
    fn timeout_on<F: Future>(&self, future: F, duration: Duration) -> TimeoutOn<Self::Timer, F> {
        TimeoutOn::new(self.shared().inner.timer_access(), future, duration)
    }
    fn sleep(&self, duration: Duration) -> Sleep<Self::Timer> {
        Sleep::new(self.shared().inner.timer_access(), duration)
    }
}
impl<C> RuntimeWait for RuntimeHandle<C> where C: ExecutorClock<Instant = GlobalManualInstant> {}
impl<C> RuntimeQuery for RuntimeHandle<C>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
{
    fn id(&self) -> RuntimeId {
        self.shared().inner.id()
    }
    fn status(&self) -> RuntimeStatus {
        self.shared().inner.status()
    }
    fn is_driving(&self) -> bool {
        self.shared().inner.is_driving()
    }
    fn last_idle_state(&self) -> IdleState {
        self.shared().inner.last_idle_state()
    }
    fn close_state(&self) -> CloseState {
        self.shared().inner.close_state()
    }
    fn external_task_count(&self) -> usize {
        self.shared().inner.external_task_count()
    }
    fn internal_task_count(&self) -> usize {
        self.shared().inner.internal_task_count()
    }
    fn timer_registration_count(&self) -> usize {
        self.shared().inner.timer_registration_count()
    }
    fn idle_task_count(&self) -> usize {
        self.shared().inner.idle_task_count()
    }
    fn in_flight_task_count(&self) -> usize {
        self.shared().inner.in_flight_task_count()
    }
    fn waiting_for_wake_count(&self) -> usize {
        self.shared().inner.waiting_for_wake_count()
    }
    fn pending_cancel_task_count(&self) -> usize {
        self.shared().inner.pending_cancel_task_count()
    }
}
impl<C> RuntimeControl for RuntimeHandle<C>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
{
    fn request_close(&self) -> CloseRequestStatus {
        self.shared().inner.request_close()
    }
}
impl<M, C, P, H> RuntimeTime for ConfiguredRuntime<M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    type Timer = RuntimeTimerAccess<C>;
    fn timeout(&self, duration: Duration) -> Timeout<Self::Timer> {
        Timeout::new(self.shared().inner.timer_access(), duration)
    }
    fn timeout_on<F: Future>(&self, future: F, duration: Duration) -> TimeoutOn<Self::Timer, F> {
        TimeoutOn::new(self.shared().inner.timer_access(), future, duration)
    }
    fn sleep(&self, duration: Duration) -> Sleep<Self::Timer> {
        Sleep::new(self.shared().inner.timer_access(), duration)
    }
}
impl<M, C, P, H> RuntimeWait for ConfiguredRuntime<M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
}
impl<M, C, P, H> RuntimeQuery for ConfiguredRuntime<M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    fn id(&self) -> RuntimeId {
        self.shared().inner.id()
    }
    fn status(&self) -> RuntimeStatus {
        self.shared().inner.status()
    }
    fn is_driving(&self) -> bool {
        self.shared().inner.is_driving()
    }
    fn last_idle_state(&self) -> IdleState {
        self.shared().inner.last_idle_state()
    }
    fn close_state(&self) -> CloseState {
        self.shared().inner.close_state()
    }
    fn external_task_count(&self) -> usize {
        self.shared().inner.external_task_count()
    }
    fn internal_task_count(&self) -> usize {
        self.shared().inner.internal_task_count()
    }
    fn timer_registration_count(&self) -> usize {
        self.shared().inner.timer_registration_count()
    }
    fn idle_task_count(&self) -> usize {
        self.shared().inner.idle_task_count()
    }
    fn in_flight_task_count(&self) -> usize {
        self.shared().inner.in_flight_task_count()
    }
    fn waiting_for_wake_count(&self) -> usize {
        self.shared().inner.waiting_for_wake_count()
    }
    fn pending_cancel_task_count(&self) -> usize {
        self.shared().inner.pending_cancel_task_count()
    }
}
impl<M, C, P, H> RuntimeControl for ConfiguredRuntime<M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    fn request_close(&self) -> CloseRequestStatus {
        self.shared().inner.request_close()
    }
}
