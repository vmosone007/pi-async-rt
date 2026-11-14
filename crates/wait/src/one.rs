//! 唯一交值方与等待方之间的一次原值交接。

use crate::definitions::{SendError, WaitError};
use atomic_waker::AtomicWaker;
use pi_result::prelude::*;
use std::cell::UnsafeCell;
use std::future::Future;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::task::{Context, Poll};

#[path = "one_inner.rs"]
mod inner;

// 内部设计：一个共享存储（shared storage）只服务一个等待端和一个发送端。
// WaitOne.shared 与 OneSender.shared 各持有一份 Arc；没有额外任务或队列。
// 公开 send／Drop 先 take 出自身责任，再进入私有操作；局部 Arc 在回调异常时仍可释放。
// OneShared 只含状态字、原值存储、通知目标；初始原值未初始化。
// 低五位：关闭1、接受许可2、保存完成4、已经取走8、无值结束16。
// 关闭和发布保存完成都在同一状态字上作 AcqRel 读改写（RMW）。
// 发送者用发布返回的前态确认关闭；后发生的一方据此承担已初始化原值。
// 看见保存完成仍必须先竞争取走位，再读取 MaybeUninit<T>；失败者不得读。
// 关闭遇尚在写入的发送者不等待；发送者仍持 Arc，完成保存后负责必要清理。
// poll 完成检查→登记实际 Context→再查；完成时先移出 shared，再返回原值。
// 原值先取走／责任先结束，最后才调用可能异常的 Waker 回调或 T::drop。
// 最后 Arc 的 OneShared::drop 只收尾未被取走的初始化值，不恢复通知。
// one 的构造、轮询、交值、关闭、无值退出与最终存储释放分别委托同名芯入口。
// 每次构造一次 Arc 分配，正常 send 不分配；不使用锁（lock）、SeqCst 或额外线程。
// CAS 只因真实状态竞争重试，不等待特定参与者；用户析构和通知没有墙钟上界。
// 预定行为验证入口：tests/wait_one_test.rs；需先配齐构造与析构，再运行持有测试。
// 本段表示设计关系，不代表该测试文件或相应生产实现已经存在。

// @trace 1
/// 等待唯一交值方交来的值。
///
/// 通过 [`wait_one`] 获得，完成结果为 `RawResult<T, WaitError>`。
/// 值可在首次轮询（poll）前到达；唯一交值句柄无值释放时返回
/// [`WaitError::SenderDropped`]。放弃本值立即停止接收，不要求发送端同时释放。
///
/// 同时只允许一个轮询者；完成后再次轮询会恐慌（panic）。
/// 不要求 `T: Unpin`、`Sync` 或 `'static`；跨线程移动仍需 `T: Send`。
/// 通知回调发生恐慌后，不保证可继续轮询；仍可放弃等待并清理自身原值责任。
pub struct WaitOne<T> {
    // Some 表示本等待端仍负责接收／关闭；None 表示责任已经移出。
    shared: Option<Arc<OneShared<T>>>,
}

// @trace 2
/// 唯一交值句柄（sender handle）。
///
/// 不可克隆（Clone）；[`Self::send`] 消耗自身，表达本份只能交值一次。
/// 不交值而直接释放，会使等待端取得 [`WaitError::SenderDropped`]。
/// 在满足 `T: Send` 时可转交其它线程，不要求 `T: Sync`。
pub struct OneSender<T> {
    // send／Drop 先移出 Arc；即使通知异常，也不能再按未交值路径收尾。
    shared: Option<Arc<OneShared<T>>>,
}

// @trace 3
// 唯一等待的共享记录（shared record）；字段不向其它模块或调用者开放。
struct OneShared<T> {
    // 原值许可和结束事实的唯一归属；不保存引用数量。
    state: AtomicUsize,
    // 只有获许可写者／取得 VALUE_TAKEN 的读者可访问，未就绪不可读。
    value: UnsafeCell<MaybeUninit<T>>,
    // 只管理实际 Context 的通知目标；不保存或推进任务。
    notify: AtomicWaker,
}

impl<T> OneShared<T> {
    const RECEIVER_CLOSED: usize = 1;
    const VALUE_CLAIMED: usize = 2;
    const VALUE_READY: usize = 4;
    const VALUE_TAKEN: usize = 8;
    const NO_VALUE: usize = 16;
}

// SAFETY: 原值只在单一写入许可及单一消费许可下移动，不向外借出 T 的引用。
// 共享记录的跨线程转交只在 T: Send 时允许；存储地址由 Arc 保持，通知使用 AtomicWaker。
unsafe impl<T: Send> Send for OneShared<T> {}
// SAFETY: 共享访问只执行状态原子操作；UnsafeCell 访问受上述许可及发布／获取顺序保护。
// T 不要求 Sync，因为不会并发提供 &T；这不授予绕过状态协议读取原值的权限。
unsafe impl<T: Send> Sync for OneShared<T> {}

// @trace 4
/// 同步建立一份唯一等待及其唯一交值句柄（sender handle）。
///
/// 构造后即可交值，无需先轮询（poll）等待端；不创建任务（Task），不访问运行时。
/// 需要一次共享分配（Arc allocation）；分配失败沿标准库行为。
pub fn wait_one<T>() -> (WaitOne<T>, OneSender<T>) {
    inner::one_create_inner()
}

impl<T> Future for WaitOne<T> {
    type Output = RawResult<T, WaitError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        inner::one_poll_inner(self.get_mut(), context)
    }
}

impl<T> Drop for WaitOne<T> {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.take() {
            inner::one_close_inner(&shared);
        }
    }
}

impl<T> OneSender<T> {
    /// 同步交出原值，消耗本句柄（handle）。
    ///
    /// 成功表示值已被接受，不承诺等待端已观察到值。
    /// 接收已关闭则返回 [`SendError`]，可拆出同一个原值及
    /// [`crate::SendErrorKind::ReceiverClosed`]；不复制或隐式丢弃原值。
    ///
    /// 不等待接收方、调度或锁；条件原子更新（CAS）可因实际并发修改重试。
    /// 原值正常交接不新增分配。通知及原值清理可执行用户代码，其恐慌（panic）会传播；
    /// 已成立的接受事实不会因此回退。回调本身的耗时不属于非阻塞保证。
    pub fn send(mut self, value: T) -> RawResult<(), SendError<T>> {
        let shared = self.shared.take().expect("交值句柄已经消耗");
        inner::one_send_inner(&shared, value)
    }
}

impl<T> Drop for OneSender<T> {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.take() {
            inner::one_sender_drop_inner(&shared);
        }
    }
}

impl<T> Drop for OneShared<T> {
    fn drop(&mut self) {
        inner::one_shared_drop_inner(self);
    }
}
