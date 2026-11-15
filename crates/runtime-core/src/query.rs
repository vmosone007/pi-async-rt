use crate::definitions::*;
/// 各项查询独立取样；不推进、不取出、不调用用户策略。
pub trait RuntimeQuery {
    /// 读取原服务或任务（Task）的固定身份；不创建新身份或权限。
    fn id(&self) -> RuntimeId;
    /// 只读采样原服务Open／Closing／Closed；不推进服务，不保证下一次提交成功。
    fn status(&self) -> RuntimeStatus;
    /// 查询原服务推进权当前是否实际持有；包括停放与同步钩子（hook），根专用推进不计入。
    fn is_driving(&self) -> bool;
    /// 读取最近一次成功策略判断的Unknown／Idle／NotIdle，不重新调用策略。
    fn last_idle_state(&self) -> IdleState;
    /// 读取自洽的关闭进度样本；Completed不代表最后共享存储已释放。
    fn close_state(&self) -> CloseState;
    /// 读取已被本外部来源接收、尚未交付或合法清理的数量；与其它项独立取样。
    fn external_task_count(&self) -> usize;
    /// 读取原内部可执行来源的数量样本；不包括纯挂起任务（Task）。
    fn internal_task_count(&self) -> usize;
    /// 包括纯计时信号，不把每项都称为业务任务（Task）。
    fn timer_registration_count(&self) -> usize;
    /// 读取空闲来源尚保管的原任务（Task）数量；不调用空闲策略。
    fn idle_task_count(&self) -> usize;
    /// 读取已接纳且自身退出责任未结束的任务（Task）数量；不以队列数相减推导。
    fn in_flight_task_count(&self) -> usize;
    /// 仅已接纳、根Pending交接完成且无唤醒／取消／退出需求者。
    fn waiting_for_wake_count(&self) -> usize;
    /// 原取消已记录但最终控制结果尚未确定；重复请求不重复计数。
    fn pending_cancel_task_count(&self) -> usize;
}
