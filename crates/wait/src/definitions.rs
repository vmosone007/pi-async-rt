//! 等待结束和交值失败的错误类型（error types）。

use pi_result::prelude::*;
use std::fmt;

// @trace 1
/// 等待未取得值时的明确原因。
///
/// 放弃等待本身不会生成此错误；此错误表达有效交值方无值结束。
#[derive(Clone, Copy, Debug, Eq, PartialEq, ThisError)]
pub enum WaitError {
    /// 对应交值句柄（sender handle）已无值释放。
    ///
    /// 对任一等待（wait_any），表示全部有效交值方均无值退出；
    /// 对全部等待（wait_all），只表示结果向量（Vec）中的当前登记位置。
    #[error("交值句柄已无值释放")]
    SenderDropped,
}

// @trace 2
/// 交值未被接受的原因。
///
/// 原因描述本次交值许可，不推断放弃等待的更深层业务原因。
#[derive(Clone, Copy, Debug, Eq, PartialEq, ThisError)]
pub enum SendErrorKind {
    /// 接收关闭先于本次交值许可；原值尚未转交。
    #[error("等待方已停止接收")]
    ReceiverClosed,
    /// 任一等待（wait_any）已有胜出交值方，即使其值尚未完成保存。
    ///
    /// 此原因只用于任一等待；后续关闭不会改写已成立的胜出事实。
    #[error("已有其它交值被接受")]
    ValueAlreadyAccepted,
}

// @trace 3
/// 携带原值的交值错误（send error）。
///
/// 使用 [`Self::kind`] 读取原因，或用 [`Self::into_parts`] 取回同一个原值。
/// 显示（Display）和调试（Debug）均只描述原因，不读取或格式化原值。
/// 普通交值不因本错误而要求值实现 `Debug`、`Display`、`Clone` 或 `'static`。
/// 将本错误装入报告（Report）时，仍须满足报告自身的线程和生命周期约束。
#[derive(ThisError)]
#[error("交值失败：{kind}")]
pub struct SendError<T> {
    /// 固定、可复制的失败原因，也是错误来源（source）。
    #[source]
    kind: SendErrorKind,
    /// 本次没有被接受的原值；不会被复制或替换。
    value: T,
}

impl<T> SendError<T> {
    /// 读取本次交值失败的固定原因，不消费原值。
    pub fn kind(&self) -> SendErrorKind {
        self.kind
    }

    /// 消耗本错误，一并取得失败原因和同一个原值。
    ///
    /// 原值归调用者后，其使用和析构（drop）由调用者负责。
    pub fn into_parts(self) -> (SendErrorKind, T) {
        (self.kind, self.value)
    }

    // 仅建立失败载体，不改变等待状态或处理原值。
    pub(crate) fn new(kind: SendErrorKind, value: T) -> Self {
        Self { kind, value }
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("SendError").field("kind", &self.kind).finish_non_exhaustive()
    }
}
