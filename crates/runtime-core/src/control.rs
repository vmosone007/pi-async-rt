use crate::{definitions::CloseRequestStatus, query::RuntimeQuery};
/// 原服务控制；仅提出请求，不等待所有来源、任务（Task）或线程退出。
pub trait RuntimeControl: RuntimeQuery {
    /// 请求一旦记录不因通知异常回滚，无人推进也不拒绝合法请求。
    fn request_close(&self) -> CloseRequestStatus;
}
