//! 唯一结果领取，不把释放结果句柄等同取消（cancellation）。
use crate::definitions::*;
use crate::task::TaskRef;
pub use crate::task::{PanicRecord, TaskError};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

// SendOutput只由F/输出满足Send的创建入口产生，LocalOutput不满足Send/Sync。
// T/M/K的PhantomData把输出与元数据的真实类型约束带到句柄；不能只凭裸TaskRef宣称Send。
// 唯一句柄不Clone，poll &mut维持唯一领取；放弃时仅撤销结果兴趣，F继续原执行规则。
// 完成发布不把非Send输出交远端drop；线程关闭及最后持有须与原值清理分别处理。
// 主要调用链：poll -> TaskRef.take_result/登记结果Waker/复核；Drop -> abandon_result。

mod output_sealed {
    pub trait Sealed {}
}

/// 结果领取线程模式；封闭实现，调用方不能伪造跨线程能力。
// @trace 100
pub trait OutputMode: output_sealed::Sealed + 'static {}

/// 输出满足Send的创建模式；仍不赋予原异步计算（Future）访问权。
// @trace 101
pub struct SendOutput;
impl output_sealed::Sealed for SendOutput {}
impl OutputMode for SendOutput {}

/// 本地输出模式；即使T碰巧满足Send也默认保持本地领取。
// @trace 102
pub struct LocalOutput(PhantomData<Rc<()>>);
impl output_sealed::Sealed for LocalOutput {}
impl OutputMode for LocalOutput {}

/// 唯一结果句柄（result handle），不支持Clone或复制（Copy）。
///
/// 单次领取Result<T,TaskError>；业务T可以自身是Result，不据此决定控制完成分类。
/// 本地模式不能跨线程；共享ControlHandle不获得本句柄的权限。
#[must_use = "保留以领取结果，或释放以明确放弃；释放不会自动取消任务"]
// @trace 103
pub struct JoinHandle<T, M: Send + Sync + 'static = (), K: OutputMode = SendOutput> {
    pub(crate) task: TaskRef,
    pub(crate) consumed: bool,
    pub(crate) marker: PhantomData<(T, M, K)>,
}

// 句柄不内嵌T或M；移动句柄不移动固定原值或输出，故不要求它们满足Unpin。
impl<T, M: Send + Sync + 'static, K: OutputMode> Unpin for JoinHandle<T, M, K> {}

// @trace 1
impl<T, M: Send + Sync + 'static, K: OutputMode> JoinHandle<T, M, K> {
    /// 取得原任务（Task）固定身份，不消耗领取权。
    pub fn id(&self) -> TaskId {
        join_id_inner(self)
    }
}

// @trace 2
impl<T, M: Send + Sync + 'static, K: OutputMode> JoinHandle<T, M, K> {
    /// 线程安全（thread-safe）地观察当前任务（Task）基本事实，不等同结果已领取。
    pub fn snapshot(&self) -> TaskSnapshot {
        join_snapshot_inner(self)
    }
}

// @trace 3
impl<T, M: Send + Sync + 'static, K: OutputMode> JoinHandle<T, M, K> {
    /// 借用创建时同类型附加数据；引用不超过本句柄借用期。
    pub fn metadata(&self) -> &M {
        join_metadata_inner(self)
    }
}

// @trace 4
impl<T, M: Send + Sync + 'static, K: OutputMode> Future for JoinHandle<T, M, K> {
    type Output = Result<T, TaskError>;
    /// 等待并唯一领取输出（output）；未就绪登记当前唤醒器（Waker）。
    /// 领取后再次poll返回AlreadyTaken，不重复领取或制造默认T。
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        join_poll_inner(self, cx)
    }
}

// @trace 5
impl<T, M: Send + Sync + 'static, K: OutputMode> Drop for JoinHandle<T, M, K> {
    /// 放弃尚未领取的结果，不默认取消原任务（Task）。
    fn drop(&mut self) {
        join_drop_inner(self)
    }
}

include!("join_inner.rs");
