//! 预先分配的取消接续节点；只在本模块（module）消费原生队列，不保存异步计算（Future）。
use crate::parking::ThreadNotifier;
use cordyceps::{
    mpsc_queue::{Consumer, Links},
    Linked,
};
use pi_async_task_queue::{management::QueueCleanupProgress, notify::QueueNotifier};
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU8, AtomicUsize},
        Arc,
    },
};

// 静态回返协议；具体节点保存核心登记，本模块只负责排队和责任接续。
// Handle不Clone；原登记可通过另一Arc包装共享，绝不由此复制已入队资格。
// on_drop是原生指针往返可重建的同一函数地址，只用于冷路径单位结算；不指向用户回调。
// 成功出队后先结束Consumer/links引用，再处理核心取消；允许节点安全重用和原址存储。
// state仅AUTO_HELP／REQUEST，不设置永久CLOSED，迟到真实发布仍须帮助，不重开业务来源。
// units是已登记发布、队列保管及临时处理的真实接续持有数，不是业务定时登记数量。
// units用于限制复查时刻，不能证明当前可取。
// Consumer内确认清旧请求，归还后实际处理/单位结算，再AcqRel核新请求；独立出口在
// AUTO_HELP时进入帮助登记一次，内部空重查不制造请求。完整Empty＋units零优先于请求。
// 只创建时分配stub/共享对象；publish及每次重试只移动原Handle，原生Linked不调用用户代码。
// 用户Report清理和核心resume在Consumer外；Owner退出前实际定时来源已停止或已由真实本地活动者接续。

pub(super) trait RetryWork:
    Send + Sync + Sized + 'static + Linked<Links<Self>, Handle = RetryHandle<Self>>
{
    fn resume(this: &Arc<Self>);
}
pub(super) struct RetryHandle<W> {
    pub(super) value: Arc<W>,
    pub(super) on_drop: Option<fn(&W)>,
}
pub(super) struct RetryShared<W: Linked<Links<W>, Handle = RetryHandle<W>>> {
    queue: cordyceps::MpscQueue<W>,
    state: AtomicU8,
    units: AtomicUsize,
    notifier: ThreadNotifier,
    batch: NonZeroUsize,
}
pub(super) struct RetryOwner<W: RetryWork> {
    shared: Arc<RetryShared<W>>,
}
struct RetryConsumerGuard<'a, W: RetryWork> {
    consumer: Option<Consumer<'a, W>>,
}
struct RetryCompletionGuard<'a, W: RetryWork> {
    shared: &'a RetryShared<W>,
    armed: bool,
}
struct RetryCleanupGuard<'a, W: RetryWork> {
    shared: &'a RetryShared<W>,
    current: Option<RetryHandle<W>>,
    resume_tail: bool,
}
const RETRY_AUTO_HELP: u8 = 1;
const RETRY_REQUEST: u8 = 2;
enum RetryStep {
    Complete,
    MoreWork,
    // 普通步骤实际取出、归还 Consumer、处理并结清了每次尝试，恰好用尽本步额度。
    BatchProcessed,
    Deferred,
    Busy,
}
impl<W> Drop for RetryHandle<W> {
    fn drop(&mut self) {
        retry_handle_drop_inner(self)
    }
}
impl<W: RetryWork> RetryOwner<W> {
    pub(super) fn new(stub: RetryHandle<W>, notifier: ThreadNotifier, batch: NonZeroUsize) -> Self {
        retry_owner_new_inner(stub, notifier, batch)
    }
    pub(super) fn shared(&self) -> Arc<RetryShared<W>> {
        retry_owner_shared_inner(self)
    }
    // default_timer_lifecycle_test 的 test_retry_notifies_only_after_processing_the_full_batch
    // 核对两次普通推进后的通知和实际空结果。
    pub(super) fn process(&mut self) -> QueueCleanupProgress {
        let step = self.shared.step(false);
        // 确认守卫已结算。只在真实处理满额后用原通知安排复查，同一待处理通知可合并。
        if matches!(step, RetryStep::BatchProcessed) {
            self.shared.notifier.notify();
        }
        retry_owner_process_inner(step)
    }
}
impl<W: RetryWork> Drop for RetryOwner<W> {
    fn drop(&mut self) {
        retry_owner_drop_inner(self)
    }
}
impl<W: RetryWork> RetryShared<W> {
    // 同一原节点（node）已独占链接资格。先登记本次单位，再取得带结算责任的持有。
    // confirm只调用已核对同一登记的一次性确认，随后才发布节点并通知（notification）。
    pub(super) fn publish(
        &self,
        node: Arc<W>,
        return_unit: fn(&W),
        confirm: impl FnOnce(&W),
    ) {
        let mut completion = RetryCompletionGuard { shared: self, armed: false };
        retry_publish_inner(self, &mut completion);
        let value = RetryHandle { value: node, on_drop: Some(return_unit) };
        confirm(&value.value);
        self.queue.enqueue(value);
        self.notifier.notify();
    }
    // 原Handle在发布登记前不带单位，into_ptr后仍保相同结算回返；Stub永不计单位。
    fn confirm(&self) {
        if retry_confirm_inner(self) {
            self.assist();
        }
    }
    fn assist(&self) -> RetryStep {
        retry_request_inner(self);
        loop {
            match self.step(true) {
                RetryStep::MoreWork | RetryStep::BatchProcessed => continue,
                result @ (RetryStep::Complete | RetryStep::Deferred | RetryStep::Busy) => {
                    return result
                }
            }
        }
    }
    fn step(&self, helping: bool) -> RetryStep {
        let mut completion = if helping {
            None
        } else {
            Some(RetryCompletionGuard { shared: self, armed: false })
        };
        retry_step_inner(self, helping, completion.as_mut())
    }
    pub(super) fn begin_close(&self) -> QueueCleanupProgress {
        retry_begin_close_inner(self)
    }
    pub(super) fn has_work(&self) -> bool {
        retry_has_work_inner(self)
    }
}
impl<W: Linked<Links<W>, Handle = RetryHandle<W>>> RetryShared<W> {
    pub(super) fn return_unit(&self) {
        retry_return_unit_inner(self)
    }
}
impl<W: RetryWork> Drop for RetryConsumerGuard<'_, W> {
    fn drop(&mut self) {
        retry_consumer_drop_inner(self)
    }
}
impl<W: RetryWork> Drop for RetryCompletionGuard<'_, W> {
    fn drop(&mut self) {
        if retry_completion_drop_inner(self) {
            self.shared.confirm();
        }
    }
}
impl<W: RetryWork> Drop for RetryCleanupGuard<'_, W> {
    fn drop(&mut self) {
        retry_cleanup_drop_inner(self)
    }
}
include!("retry_inner.rs");
