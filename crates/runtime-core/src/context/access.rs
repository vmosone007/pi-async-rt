use super::record;
use crate::{
    block_on::RuntimeBlockOn, control::RuntimeControl, definitions::CurrentRuntimeAccessError,
    dispatch::*, time::RuntimeTime, wait::RuntimeWait,
};

/// 公共访问的目标与本地推进权限不同；显式enter只选择原服务。
pub trait RuntimeContext: TaskDispatch + RuntimeTime {
    type Handle: Runtime<Notifier = Self::Notifier, Timer = Self::Timer, Handle = Self::Handle>
        + Clone
        + Send
        + Sync
        + 'static;
    /// 取得原服务的共享句柄（shared handle）；Clone不复制服务，普通释放不自动关闭。
    fn handle(&self) -> Self::Handle;
    /// 取得并克隆（Clone）当前所选原服务的共享句柄（shared handle），不产生本地或推进权限。
    /// 无当前目标、句柄类型（type）不匹配或线程局部存储（TLS）不可访问时返回对应错误。
    /// 有效公共选择在原服务关闭后仍可读取关闭事实，不会重新开放服务。
    fn try_current() -> pi_result::RawResult<Self::Handle, CurrentRuntimeAccessError> {
        record::try_current::<Self::Handle>()
    }
    /// 同步借用不能逃逸或跨await；公共环境关闭后仍可读原关闭事实。
    fn try_with_current<R>(
        action: impl FnOnce(&Self::Handle) -> R,
    ) -> pi_result::RawResult<R, CurrentRuntimeAccessError> {
        record::try_with_current::<Self::Handle, R>(action)
    }
    /// 同目标允许嵌套，不授予本地权限；闭包返回Future不会延长本次选择范围。
    fn enter<R>(
        &self,
        action: impl FnOnce() -> R,
    ) -> pi_result::RawResult<R, CurrentRuntimeAccessError> {
        record::with_selected(self.handle(), action)
    }
}

/// 运行时（runtime）的公共能力聚合，不包含执行器（executor）的run／run_once。
pub trait Runtime:
    TaskDispatch
    + LocalTaskDispatch
    + IdleDispatch
    + TimedTaskDispatch
    + RuntimeWait
    + RuntimeControl
    + RuntimeContext
    + RuntimeBlockOn
{
}
impl<T> Runtime for T where
    T: TaskDispatch
        + LocalTaskDispatch
        + IdleDispatch
        + TimedTaskDispatch
        + RuntimeWait
        + RuntimeControl
        + RuntimeContext
        + RuntimeBlockOn
{
}
