//! 默认定时来源的身份、选项及内部编号。

use std::num::{NonZeroU32, NonZeroU64, NonZeroU8, NonZeroUsize};

/// 唯一描述原定时登记的不透明键（opaque key）。
///
/// 复制不保活原值（original value）、不取消登记，也不授予查询内部数据的权限。
/// 键（key）由来源分配，包含来源及登记身份；不会因清理、移动或编号复用而命中另一项。
/// 调用取消接口（cancellation interface）只提交请求，不证明此键（key）仍有效。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
// @trace 1
pub struct CancelTimerKey {
    // 来源编号不复用；不是指针（pointer）或线程编号（thread identifier）。
    pub(super) queue_id: NonZeroU64,
    // 来源内只增、耗尽即拒绝的编号；不代表排队顺序或固定清理边界（boundary）。
    pub(super) registration_id: NonZeroU64,
}

/// 默认定时队列（timer queue）的三个独立工作额度。
///
/// 构造后不变。额度限制一次普通操作的尝试数量，不保证固定墙钟耗时，
/// 不限制已确认完整的到期结果向量（vector）长度，也不允许放弃析构（drop）余责。
#[derive(Clone, Copy, Debug)]
// @trace 2
pub struct CancelTimerOptions {
    /// 请求采用、原生取出、实际清理各自最多尝试的项数。默认64，互不借用额度。
    /// 来源仍接受请求且其它完成条件均成立时，`process_cleanup` 可在请求额度
    /// 用完后再尝试一次非等待取出，所获请求只暂存（staging）并留待下轮采用。
    pub work_batch_size: NonZeroUsize,
    /// 非空原生定时器（native timer）一次普通推进的最大毫秒跨度；默认64。
    /// 原生滚轮一次可能处理多项，不能据此承诺固定时间开销。
    pub advance_ticks: NonZeroU32,
    /// 接收前受检原子比较交换（CAS）的最多尝试次数；默认3。
    /// 只限制可明确拒绝的准备阶段，不截断已接收的发布和清理责任。
    pub atomic_attempts: NonZeroU8,
}

impl Default for CancelTimerOptions {
    fn default() -> Self {
        timer_options_default_inner()
    }
}

// 两组仅为同一来源内部的固定清理范围；false对应0，true对应1。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct TaggedRegistrationId {
    pub(super) group: bool,
    pub(super) registration_id: NonZeroU64,
}

// 连接只保存稳定编号（stable identifier），不保存IndexMap的密集位置或元素地址。
#[derive(Clone, Copy, Default)]
pub(super) struct TimerLinks {
    pub(super) prev: Option<TaggedRegistrationId>,
    pub(super) next: Option<TaggedRegistrationId>,
}

#[derive(Clone, Copy, Default)]
pub(super) struct TimerListEnds {
    pub(super) head: Option<TaggedRegistrationId>,
    pub(super) tail: Option<TaggedRegistrationId>,
}

// 一个权威来源数量，最高五位分别记录独立且不可混同的事实。
pub(super) const STOPPED: usize = 1usize << (usize::BITS - 1);
pub(super) const OWNER_EXITED: usize = 1usize << (usize::BITS - 2);
pub(super) const CLEANUP_REQUESTED: usize = 1usize << (usize::BITS - 3);
pub(super) const EXIT_CONFIRMED: usize = 1usize << (usize::BITS - 4);
pub(super) const CURRENT_GROUP: usize = 1usize << (usize::BITS - 5);
pub(super) const COUNT_MASK: usize = usize::MAX >> 5;

// 各组发布参与计数（publisher count）低 63 位为实际数量，最高位保留新登记事实。
pub(super) const PUBLISHER_COUNT_MASK: u64 = u64::MAX >> 1;
pub(super) const PUBLISHER_REGISTERED: u64 = !PUBLISHER_COUNT_MASK;

// 原生槽保留哨兵。所有未交出的原值保守占用额度，不只计算当前原生槽。
pub(super) const NATIVE_CAPACITY: usize = 4_294_967_294;
pub(super) const MAX_DEADLINE_NANOS: u64 = 18_446_744_073_709_000_000;

include!("definitions_inner.rs");
