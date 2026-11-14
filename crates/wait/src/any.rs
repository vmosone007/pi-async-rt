//! 多个交值方竞争同一份结果，接受最先获得许可的原值。

use crate::definitions::{SendError, SendErrorKind, WaitError};
use atomic_waker::AtomicWaker;
use pi_result::prelude::*;
use std::cell::UnsafeCell;
use std::future::Future;
use std::mem::{self, MaybeUninit};
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::task::{Context, Poll};

#[path = "any_inner.rs"]
mod inner;

// 内部设计：多个有效 AnySender 共享一个 AnyShared，不引入 all 的槽或列表。
// state 低五位与唯一交值相同，高位为尚未结束的有效发送责任；初始一份。
// VALUE_CLAIMED 由首个条件原子更新（CAS）胜出者设置；它尚未写完 T 也已排除后继。
// send 的拒绝原因先保留已有胜出事实，再区分普通接收关闭。
// Active→Consumed 仅移出包装，不等于有效责任已经减数；私有操作仍负责恰减一次。
// Rejected 只保存固定原因，Clone 不再读取共享状态、分配或增加责任。
// 有效 Clone 必须在开放且无胜出许可时条件增加高位数量；不从零恢复。
// checked_increment 是保留低位的纯算术入口；最终 CAS 仍须重新核状态和前态。
// 保存原值后，用发布 VALUE_READY 的同字 AcqRel 读改写（RMW）返回前态确认关闭。
// 竞争者未取得 VALUE_TAKEN 不得读 T；已关闭且尚在写入时由真实发送者负责。
// 成功／失败 send 及无值 Drop 各自只结束自己的有效责任；通知前必须完成减数。
// 最后无值退出在同一 CAS 中确认没有胜出者并发布 NO_VALUE，不能先减数再猜测。
// WaitAny 不等所有发送端释放：见胜出值即可领取；全部无值才返回 SenderDropped。
// poll 检查→登记实际 Context→复查；完成后移出 shared，剩余发送者只按关闭路径退出。
// 常规 send 不分配，正常有效 Clone 只增加 Arc 和条件计数；不加锁，不使用 SeqCst。
// 条件重试仅对应真实并发修改；不等待胜出写者，回调异常不撤销已确认接受事实。
// 预定行为验证入口：tests/wait_any_test.rs；构造与所有退出必须配齐后再运行持有测试。
// 本段不表示相应测试或生产行为已经具备。

// @trace 1
/// 等待多个交值方中最先被接受的值。
///
/// 使用 [`wait_any`] 构造；完成结果为 `RawResult<T, WaitError>`。
/// 第一个被接受的值即可完成，不等待其余句柄（handle）释放。
/// 只有全部有效交值方均无值结束且没有胜出者，才返回 [`WaitError::SenderDropped`]。
///
/// 放弃等待停止接收，不等待其它线程；同时只允许一个轮询（poll）者。
/// 完成后再次轮询会恐慌（panic）。不要求 `T: Unpin`、`Sync` 或 `'static`；
/// 跨线程移动仍须满足 `T: Send`。通知回调异常后不保证可继续轮询，仍可安全放弃。
pub struct WaitAny<T> {
    // 唯一接收责任；None 表示已完成或已经移出关闭责任。
    shared: Option<Arc<AnyShared<T>>>,
}

// @trace 2
/// 可克隆（Clone）的任一交值句柄（sender handle）。
///
/// 每份有效句柄可消耗自身交值一次；只有首个获得许可的交值被接受。
/// 无值释放仅结束本份，不会提前结束其它交值方。
/// `T: Send` 时可跨线程使用，不额外要求 `T: Sync`。
pub struct AnySender<T> {
    // 包装是否仍承载有效责任；Consumed 不代表私有操作已完成减数。
    state: AnySenderState<T>,
}

// @trace 3
// 包装状态（wrapper state）：拒绝份不保有共享记录或参与数量。
enum AnySenderState<T> {
    // 一份真实共享持有及恰好一次结束的有效发送责任。
    Active(Arc<AnyShared<T>>),
    // 关闭或已有胜出时固定的拒绝原因；再次 Clone 原样复制。
    Rejected(SendErrorKind),
    // send／Drop 已把责任移交局部值，包装自身不可再退出一次。
    Consumed,
}

// @trace 4
// 多发送端共享记录（shared record）；原值只有一个，不按句柄数分配槽。
struct AnyShared<T> {
    // 低五位是接收／原值事实，高位是未结束的有效责任数。
    state: AtomicUsize,
    // 获胜写者唯一保存；成功取得取走位的读者唯一移出。
    value: UnsafeCell<MaybeUninit<T>>,
    // 等待方实际 Context 的最新通知目标。
    notify: AtomicWaker,
}

impl<T> AnyShared<T> {
    const RECEIVER_CLOSED: usize = 1;
    const VALUE_CLAIMED: usize = 2;
    const VALUE_READY: usize = 4;
    const VALUE_TAKEN: usize = 8;
    const NO_VALUE: usize = 16;
    const COUNT_ONE: usize = 32;
    const MAX_SENDERS: usize = usize::MAX >> 5;

    // 纯算术，不接触共享字段；调用方仍须条件确认当前字和开放许可。
    fn checked_increment(word: usize) -> Option<usize> {
        inner::any_checked_increment_inner(word)
    }
}

// SAFETY: 原值仅按单一获胜写许可和单一取走许可移动；不对外提供共享 T 引用。
// Arc 保持记录存活，状态发布及 AtomicWaker 协调线程交接；跨线程转交要求 T: Send。
unsafe impl<T: Send> Send for AnyShared<T> {}
// SAFETY: 并发共享访问限于原子状态和通知协议，UnsafeCell 受唯一许可保护；
// 不会并发借出 &T，因此不要求 T: Sync。计数不是原值消费权限。
unsafe impl<T: Send> Sync for AnyShared<T> {}

// @trace 5
/// 同步建立任一等待及一份初始有效交值句柄（sender handle）。
///
/// 通过克隆（Clone）交值句柄增加参与方，不需要先轮询（poll）等待端。
/// 一次共享分配（Arc allocation），不创建任务或访问运行时；分配失败沿标准库行为。
pub fn wait_any<T>() -> (WaitAny<T>, AnySender<T>) {
    inner::any_create_inner()
}

impl<T> Future for WaitAny<T> {
    type Output = RawResult<T, WaitError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        inner::any_poll_inner(self.get_mut(), context)
    }
}

impl<T> Drop for WaitAny<T> {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.take() {
            inner::any_close_inner(&shared);
        }
    }
}

impl<T> AnySender<T> {
    /// 同步尝试交值并消耗本句柄（handle）。
    ///
    /// 成功只表示本值胜出并被接受，不承诺等待端已观察到它。
    /// 失败通过 [`SendError`] 返还同一个原值：已有胜出许可为
    /// [`SendErrorKind::ValueAlreadyAccepted`]，否则接收关闭为
    /// [`SendErrorKind::ReceiverClosed`]。已有胜出事实不被后续关闭改写。
    ///
    /// 不分配，不等待接收者或其它交值方；条件原子更新（CAS）可能因真实竞争重试。
    /// 通知或清理中的用户恐慌（panic）传播，不回退已确认接受事实；
    /// 用户回调耗时不属于非阻塞保证。
    pub fn send(mut self, value: T) -> RawResult<(), SendError<T>> {
        match mem::replace(&mut self.state, AnySenderState::Consumed) {
            AnySenderState::Active(shared) => inner::any_send_inner(&shared, value),
            AnySenderState::Rejected(kind) => Err(SendError::new(kind, value)),
            AnySenderState::Consumed => panic!("交值句柄已经消耗"),
        }
    }
}

impl<T> Clone for AnySender<T> {
    /// 增加一份交值能力；关闭或已有胜出时仅保留固定拒绝原因。
    ///
    /// 有效克隆（Clone）不分配；本库有效数量越界在发布新份之前恐慌（panic）。
    /// 标准共享持有（Arc）自身上限仍沿标准库规则；拒绝份不会增加有效数量。
    fn clone(&self) -> Self {
        inner::any_clone_inner(self)
    }
}

impl<T> Drop for AnySender<T> {
    fn drop(&mut self) {
        if let AnySenderState::Active(shared) =
            mem::replace(&mut self.state, AnySenderState::Consumed)
        {
            inner::any_sender_drop_inner(&shared);
        }
    }
}

impl<T> Drop for AnyShared<T> {
    fn drop(&mut self) {
        inner::any_shared_drop_inner(self);
    }
}
