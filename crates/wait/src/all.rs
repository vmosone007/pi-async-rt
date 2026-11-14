//! 按登记顺序收集全部有效交值方各自的结果。

use crate::definitions::{SendError, SendErrorKind, WaitError};
use atomic_waker::AtomicWaker;
use pi_result::prelude::*;
use std::cell::UnsafeCell;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::{self, MaybeUninit};
use std::pin::Pin;
use std::sync::atomic::{AtomicPtr, AtomicUsize};
use std::sync::Arc;
use std::task::{Context, Poll};

#[path = "all_inner.rs"]
mod inner;

// 内部设计：AllShared 管组关闭、有效责任数、累计位置数及列表持有；AllSlot 管一份结果。
// 每份有效 AllSender 各持 group Arc 和 slot Arc；组的列表另持每槽一份 Arc::into_raw。
// AllSlot 不反向持有组；next 原始连接不增加强引用，不形成内部 Arc 环。
// 初始两次 Arc 分配；每次有效 Clone 一次槽分配，普通 send 不额外分配。
// state 最高位是永久关闭，低位是未结束有效份；registered 是已发布及临时预留位置。
// head 成功 CAS 的顺序即登记顺序；next 只在本槽发布前写，发布后只读。
// 正常收集沿栈链读出再 reverse；原份在向量首位，不按 send 到达时间排序。
// 关闭先设置 state 关闭位，再把 head 换为固定只读 static 地址；此后不再恢复空链。
// 原始列表持有仅按真实已交出地址逐项 Arc::from_raw 一次；关闭标记永不解引用或恢复。
// 有效 Clone：准备 Arc→条件预留活跃份→预留累计位置→发布列表原始 Arc。
// 观察关闭标记时收回未发布列表 Arc，再撤销两类预留，返回固定 ReceiverClosed。
// 逻辑越界不发布新份；已取得的预留先撤销再 panic。关闭不把计数强行归零。
// send 固定顺序：槽许可→同字条件确认组开放→保存 T→发布 VALUE_READY→
// group.state.fetch_or(0, AcqRel) 确认关闭，必要时先取走 T 到局部→retire→用户析构／通知。
// 最后确认不可改普通 load；同字 RMW 确立关闭与发布先后，避免双方错过彼此。
// 关闭检查／必要取走不可延迟到 retire 后；否则正常零份收集可能丢失原值。
// retire 之前不得调用 T::drop 或通知，以免异常留下未结束的虚假责任。
// 接收关闭不等 WRITING；该写者仍持槽 Arc，随后按同字关闭确认负责原值。
// AllDrainGuard 独占一条固定余链；每项先推进 next 再读取结果／执行用户析构。
// 守卫自己的 Drop 非空时先交给栈上后继守卫、清空自己，再调用 drain；
// 这样首次用户异常仍有活着的余链守卫。空守卫立即返回，不按节点递归，不加 catch_unwind。
// AllShared 最后释放接续剩余链，不虚构额外组 Arc；AllSlot 最后释放只清理未取走原值。
// all 的普通关闭／收集为 O(n)，不追逐关闭后的新登记；不使用锁或 SeqCst。
// 预定行为验证入口：tests/wait_all_test.rs，需先配齐构造、槽释放及余链守卫的全部路径。
// 此处没有已经执行相应测试的含义。

// @trace 1
/// 等待全部有效交值方分别结束，按登记顺序返回逐项结果。
///
/// 输出为 `Vec<RawResult<T, WaitError>>`。最初交值方占第一项，每次有效克隆（Clone）
/// 增加一项；并发克隆按登记成功先后排序，不按交值到达顺序排序。
/// 某份句柄（handle）无值释放，只在自身位置产生 [`WaitError::SenderDropped`]。
///
/// 放弃等待停止新接收与登记，并处理自己已取得的结果；不等待尚在交值的其它线程。
/// 尚存句柄可释放，后续交值返原值和关闭错误。只允许一个轮询（poll）者；
/// 完成后再次轮询恐慌（panic）。不要求 `T: Unpin`、`Sync` 或 `'static`；
/// 跨线程移动要求 `T: Send`。通知回调异常后不保证继续轮询，仍可放弃等待。
pub struct WaitAll<T> {
    // 唯一接收责任；None 表示已完成或已移出关闭责任。
    shared: Option<Arc<AllShared<T>>>,
}

// @trace 2
/// 可克隆（Clone）的全部交值句柄（sender handle）。
///
/// 每份有效句柄占一个结果位置，消耗自身交值或无值释放均只结束本份。
/// 接收关闭后的克隆只保留关闭错误，不新增位置。
/// `T: Send` 时可跨线程使用，不额外要求 `T: Sync`。
pub struct AllSender<T> {
    // 包装状态不替代槽中的结果许可，也不把移出责任误作已经减数。
    state: AllSenderState<T>,
}

// @trace 3
// 每份有效能力分别保有组与本槽；拒绝份不保留它们。
enum AllSenderState<T> {
    // 组 Arc 保持计数／通知可用，本槽 Arc 保持尚在交值的原值存储可用。
    Active {
        group: Arc<AllShared<T>>,
        slot: Arc<AllSlot<T>>,
    },
    // 固定关闭原因；Clone 原样复制，send 原值返还。
    Rejected(SendErrorKind),
    // 原责任已经移到局部；不得再次结束同一份。
    Consumed,
}

// @trace 4
// 组记录（group record）的字段只在本模块壳及配对芯内访问。
struct AllShared<T> {
    // 最高位关闭，低位未结束有效份；所有合法读改写保持关闭位。
    state: AtomicUsize,
    // 已发布累计位置加尚未发布预留；不随正常交值结束而减数。
    registered: AtomicUsize,
    // 每个真实地址代表一份由组列表保有的原始 Arc，或固定关闭标记。
    head: AtomicPtr<AllSlot<T>>,
    // 唯一等待端实际 Context 的最新通知目标。
    notify: AtomicWaker,
    // 组拥有列表结果的类型／释放关系，无额外运行时字段或分配。
    owns_slots: PhantomData<T>,
}

// @trace 5
// 独立结果槽（result slot）；自己不持 group，防止组与槽相互强持有。
struct AllSlot<T> {
    // 发布前唯一写，发布后只读；连接到已发布的较早槽。
    next: UnsafeCell<*mut AllSlot<T>>,
    // 写入、就绪、无值和已经取走四种许可／事实。
    state: AtomicUsize,
    // 获得写入许可者保存，获得取走许可者移出；不向外借出 &T。
    value: UnsafeCell<MaybeUninit<T>>,
}

// @trace 6
// 独占余链的清理守卫（cleanup guard）；只在当前同步栈上传递列表持有。
struct AllDrainGuard<T> {
    // 尚未 Arc::from_raw 恢复的真实列表后缀；null 表示不再承担余项。
    next: *mut AllSlot<T>,
}

// @trace 8
// 非零大小、不可变的独立对象；仅取地址作为关闭标记，不读取其值表示状态。
static CLOSED_LIST_MARKER: usize = 0;

impl<T> AllShared<T> {
    const RECEIVER_CLOSED: usize = 1usize << (usize::BITS - 1);
    const MAX_ACTIVE: usize = usize::MAX >> 1;

    // 实际结果元素大小和两个计数表示共同限制累计结果容量。
    fn max_results() -> usize {
        inner::all_max_results_inner::<T>()
    }

    // 纯算术，只判断累计位置能否增加，不改变任何共享字段。
    fn checked_registered_increment(registered: usize) -> Option<usize> {
        inner::all_checked_registered_increment_inner::<T>(registered)
    }

    // 标记仅比较，不得传入 Arc::from_raw 或解引用。
    fn closed_marker() -> *mut AllSlot<T> {
        inner::all_closed_marker_inner::<T>()
    }

    // 永久停收并一次交出当前真实列表；返回守卫立即承担其余项。
    fn detach(&self) -> AllDrainGuard<T> {
        inner::all_detach_inner(self)
    }

    // 正常结果、关闭拒绝或未发布预留的责任分别安排后，恰减本份一次再按条件通知。
    fn retire(&self) {
        inner::all_retire_inner(self);
    }
}

impl<T> AllSlot<T> {
    const WRITING: usize = 1;
    const VALUE_READY: usize = 2;
    const NO_VALUE: usize = 4;
    const VALUE_TAKEN: usize = 8;

    // 准备未发布槽，本方法不分配 Arc。
    fn new() -> Self {
        inner::all_slot_new_inner()
    }

    // 先取得消费许可；未就绪、仅在写或已取走时不读取原值。
    fn take_ready(&self) -> Option<RawResult<T, WaitError>> {
        inner::all_slot_take_ready_inner(self)
    }
}

impl<T> AllDrainGuard<T> {
    // 先推进余项责任，再交出当前恢复的列表 Arc。
    fn take_next(&mut self) -> Option<Arc<AllSlot<T>>> {
        inner::all_drain_take_next_inner(self)
    }

    // 有限处理本守卫当前余链，不追逐其它新增来源。
    fn drain(&mut self) {
        inner::all_drain_inner(self);
    }
}

// SAFETY: 组通过原子字和发布链管理持有，不并发借出 T 的引用；列表每个原始 Arc
// 只由取得固定链的守卫恢复一次。T: Send 允许最终结果移至取得责任的合法线程。
unsafe impl<T: Send> Send for AllShared<T> {}
// SAFETY: 共享状态和链首都按发布／获取协议访问；槽 next 发布后不再修改，
// 对槽原值的读写另由唯一许可保护，不因为 &AllShared 存在而获得 &T。
unsafe impl<T: Send> Sync for AllShared<T> {}
// SAFETY: 原值只按槽写入／取走许可移动，next 在发布前唯一写，发布后只读。
// 跨线程移动槽或最终清理要求 T: Send，不要求 T: Sync。
unsafe impl<T: Send> Send for AllSlot<T> {}
// SAFETY: UnsafeCell 的访问均受上述许可和链发布顺序保护；不会并发借出 &T。
// 列表只保存 Arc 强引用的原始地址，不授予任意裸指针恢复持有的能力。
unsafe impl<T: Send> Sync for AllSlot<T> {}

// @trace 7
/// 同步建立全部等待及一份初始有效交值句柄（sender handle）。
///
/// 初始结果位置为第一项。有效克隆（Clone）增加位置；没有固定外部参与数量。
/// 构造需要组和原份结果槽各一次共享分配（Arc allocation），不创建任务或访问运行时。
/// 系统分配失败沿标准库规则；最终结果向量（Vec）的容量受实际元素大小限制。
pub fn wait_all<T>() -> (WaitAll<T>, AllSender<T>) {
    inner::all_create_inner()
}

impl<T> Future for WaitAll<T> {
    type Output = Vec<RawResult<T, WaitError>>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        inner::all_poll_inner(self.get_mut(), context)
    }
}

impl<T> Drop for WaitAll<T> {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.take() {
            inner::all_close_inner(&shared);
        }
    }
}

impl<T> AllSender<T> {
    /// 同步交出本份位置的原值并消耗句柄（handle）。
    ///
    /// 成功只表示本位置已经接受原值，不表示其它参与方已结束或等待端已观察结果。
    /// 接收先关闭时，以 [`SendErrorKind::ReceiverClosed`] 返还同一个原值。
    /// 不等待其它发送方，不额外分配；条件原子更新（CAS）可因真实竞争重试。
    ///
    /// 通知和原值清理可以执行用户代码；其恐慌（panic）传播，
    /// 已接受事实不因此回退，用户回调耗时不属于非阻塞保证。
    pub fn send(mut self, value: T) -> RawResult<(), SendError<T>> {
        match mem::replace(&mut self.state, AllSenderState::Consumed) {
            AllSenderState::Active { group, slot } => inner::all_send_inner(&group, &slot, value),
            AllSenderState::Rejected(kind) => Err(SendError::new(kind, value)),
            AllSenderState::Consumed => panic!("交值句柄已经消耗"),
        }
    }
}

impl<T> Clone for AllSender<T> {
    /// 登记一份独立交值位置，顺序由并发登记成功点决定。
    ///
    /// 有效克隆（Clone）分配一个新结果槽；先观察到关闭则只复制关闭原因，
    /// 不分配、不增加位置。关闭竞争中尚未发布的预留会撤销。
    /// 本库数量或容量越界在新份发布前回滚预留并恐慌（panic）；
    /// 标准共享分配及引用数量的失败仍沿标准库行为。
    fn clone(&self) -> Self {
        inner::all_clone_inner(self)
    }
}

impl<T> Drop for AllSender<T> {
    fn drop(&mut self) {
        if let AllSenderState::Active { group, slot } =
            mem::replace(&mut self.state, AllSenderState::Consumed)
        {
            inner::all_sender_drop_inner(&group, &slot);
        }
    }
}

impl<T> Drop for AllShared<T> {
    fn drop(&mut self) {
        inner::all_shared_drop_inner(self);
    }
}

impl<T> Drop for AllSlot<T> {
    fn drop(&mut self) {
        inner::all_slot_drop_inner(self);
    }
}

impl<T> Drop for AllDrainGuard<T> {
    fn drop(&mut self) {
        inner::all_drain_drop_inner(self);
    }
}
