//! 当前任务附加数据（current task metadata）的公开借用（public borrowing）约定。
//!
//! 只经公开入口（public API）创建和推进真实任务（Task）；不安装线程局部身份（TLS identity），
//! 不操作私有状态（private state），不由辅助代码（test helper）代补清理（cleanup）。

use pi_async_task::{
    create, create_local, try_current_task_id, try_with_current_metadata, CurrentTaskAccessError,
    Runnable, Schedule, TaskOptions, TaskThread,
};
use pi_async_task::definitions::{RunOutcome, TerminationKind};
use std::cell::Cell;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// 这些就绪任务（ready tasks）没有后续调度（scheduling）需求；意外交付（delivery）立即失败。
struct NoSchedule;
impl Schedule for NoSchedule {
    fn schedule(&self, _task: Runnable) {
        panic!("本场景没有后续调度（scheduling）需求");
    }
}

/// 新原生编号（native numbering）T379；没有当前任务（Task）时必须准确拒绝，且不调用读取闭包（closure）。
/// 直接查询及普通异步计算（Future）的裸轮询（poll）都不能自行建立任务（Task）身份。
#[test]
fn test_no_current_task_rejects_without_calling_reader() {
    let calls = Cell::new(0);
    assert_eq!(try_current_task_id(), None);
    let result = try_with_current_metadata::<usize, _>(|_| {
        calls.set(calls.get() + 1);
        17
    });
    assert_eq!(result, Err(CurrentTaskAccessError::NoCurrentTask));
    assert_eq!(calls.get(), 0);
    let mut bare = std::pin::pin!(poll_fn(|_| {
        Poll::Ready(try_with_current_metadata::<usize, _>(|_| calls.set(1)))
    }));
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(bare.as_mut().poll(&mut cx), Poll::Ready(Err(CurrentTaskAccessError::NoCurrentTask)));
    assert_eq!(calls.get(), 0);
    assert_eq!(try_current_task_id(), None);
}

/// T379：真实本地任务（local task）内借用同一附加数据（metadata），安全更新原值。
/// 返回值（return value）同时携带非Send的Rc及调用者短借用（short borrow），不借出所给&M。
/// 错误类型（type mismatch）不得调用读取闭包（closure）；任务（Task）退出后身份恢复为空。
#[test]
fn test_local_task_reads_original_metadata_without_restricting_result() {
    let owner = TaskThread::new();
    let metadata = Arc::new(AtomicUsize::new(41));
    let expected = Arc::clone(&metadata);
    let local = Rc::new(Cell::new(0));
    let expected_local = Rc::clone(&local);
    let future = poll_fn(move |_| {
        assert!(try_current_task_id().is_some());
        let calls = Cell::new(0);
        assert_eq!(
            try_with_current_metadata::<usize, _>(|_| {
                calls.set(calls.get() + 1);
                1
            }),
            Err(CurrentTaskAccessError::MetadataTypeMismatch)
        );
        assert_eq!(calls.get(), 0);
        let borrowed = String::from("caller-owned");
        let (returned, text) = try_with_current_metadata::<Arc<AtomicUsize>, _>(|value| {
            assert!(Arc::ptr_eq(value, &expected));
            calls.set(calls.get() + 1);
            assert_eq!(value.fetch_add(1, Ordering::SeqCst), 41);
            (Rc::clone(&local), borrowed.as_str())
        }).unwrap();
        assert_eq!(calls.get(), 1);
        assert_eq!(text, "caller-owned");
        assert!(Rc::ptr_eq(&returned, &local));
        returned.set(7);
        Poll::Ready(73usize)
    });
    let mut parts = create_local(&owner, future, NoSchedule, Arc::clone(&metadata), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("本地创建（local creation）应成功"));
    assert!(matches!(parts.runnable.run(&owner), Ok(RunOutcome::Finished(TerminationKind::Completed))));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Ok(73))));
    assert_eq!(metadata.load(Ordering::SeqCst), 42);
    assert_eq!(expected_local.get(), 7);
    assert_eq!(owner.live_tasks(), 0);
    assert_eq!(try_current_task_id(), None);
    drop(parts.join);
    drop(parts.control);
    assert_eq!(Arc::strong_count(&metadata), 1);
}

/// T379：同步嵌套（synchronous nesting）必须读取实际内层任务（Task），返回后恢复外层。
/// 内层读取闭包（closure）的恐慌（panic）须由既有轮询（poll）边界保存原载荷（payload）。
#[test]
fn test_nested_task_and_reader_panic_restore_outer_metadata() {
    let owner = Rc::new(TaskThread::new());
    let nested_owner = Rc::clone(&owner);
    let outer_future = poll_fn(move |_| {
        let outer_id = try_current_task_id().unwrap();
        assert_eq!(try_with_current_metadata::<usize, _>(|value| *value), Ok(101));
        let nested = try_with_current_metadata::<usize, _>(|value| {
            assert_eq!(*value, 101);
            let inner_future = poll_fn(move |_| {
                assert_ne!(try_current_task_id(), Some(outer_id));
                let _ = try_with_current_metadata::<usize, ()>(|inner| {
                    assert_eq!(*inner, 202);
                    std::panic::panic_any(812usize);
                });
                Poll::Ready(())
            });
            let mut inner = create(inner_future, NoSchedule, 202usize, TaskOptions::plain())
                .unwrap_or_else(|_| panic!("内层创建（inner creation）应成功"));
            assert!(matches!(inner.runnable.run(&nested_owner), Ok(RunOutcome::Finished(TerminationKind::Panicked))));
            let mut cx = Context::from_waker(Waker::noop());
            match Pin::new(&mut inner.join).poll(&mut cx) {
                Poll::Ready(Err(pi_async_task::TaskError::Panicked(record))) => {
                    assert_eq!(record.payload.downcast_ref::<usize>(), Some(&812));
                }
                _ => panic!("须保留原恐慌载荷（panic payload）"),
            }
            assert_eq!(try_current_task_id(), Some(outer_id));
            assert_eq!(try_with_current_metadata::<usize, _>(|restored| *restored), Ok(101));
            *value
        });
        assert_eq!(nested, Ok(101));
        assert_eq!(try_current_task_id(), Some(outer_id));
        Poll::Ready(303usize)
    });
    let mut outer = create_local(&owner, outer_future, NoSchedule, 101usize, TaskOptions::plain())
        .unwrap_or_else(|_| panic!("外层创建（outer creation）应成功"));
    assert!(matches!(outer.runnable.run(&owner), Ok(RunOutcome::Finished(TerminationKind::Completed))));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(Pin::new(&mut outer.join).poll(&mut cx), Poll::Ready(Ok(303))));
    assert_eq!(try_current_task_id(), None);
    assert_eq!(owner.live_tasks(), 0);

    let following_future = poll_fn(|_| {
        Poll::Ready(try_with_current_metadata::<usize, _>(|value| *value).unwrap())
    });
    let mut following = create(following_future, NoSchedule, 404usize, TaskOptions::plain())
        .unwrap_or_else(|_| panic!("后续创建（following creation）应成功"));
    assert!(matches!(following.runnable.run(&owner), Ok(RunOutcome::Finished(TerminationKind::Completed))));
    assert!(matches!(Pin::new(&mut following.join).poll(&mut cx), Poll::Ready(Ok(404))));
    assert_eq!(try_current_task_id(), None);
    assert_eq!(owner.live_tasks(), 0);
}

/// T379：本任务（Task）提出取消（cancellation）时，读取闭包（closure）的原共享借用（shared borrow）仍然有效。
/// 原异步计算（Future）返回挂起（Pending）后才由生产清理（production cleanup）落实取消（cancellation）；析构（drop）时已不在当前轮询（poll）范围。
#[test]
fn test_self_cancel_keeps_metadata_borrow_valid_until_reader_returns() {
    let owner = TaskThread::new();
    let control_slot = Rc::new(std::cell::RefCell::new(None::<pi_async_task::ControlHandle>));
    let metadata = Arc::new(AtomicUsize::new(19));
    let drops = Arc::new(AtomicUsize::new(0));
    let future = CancelDuringRead {
        control: Rc::clone(&control_slot),
        drops: Arc::clone(&drops),
    };
    let mut parts = create_local(&owner, future, NoSchedule, Arc::clone(&metadata), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("本地创建（local creation）应成功"));
    *control_slot.borrow_mut() = Some(parts.control.clone());
    assert!(matches!(parts.runnable.run(&owner), Ok(RunOutcome::Finished(TerminationKind::Cancelled))));
    assert_eq!(metadata.load(Ordering::SeqCst), 23);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(try_current_task_id(), None);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Err(pi_async_task::TaskError::Cancelled))));
    control_slot.borrow_mut().take();
    drop(parts.join);
    drop(parts.control);
    assert_eq!(owner.live_tasks(), 0);
    assert_eq!(Arc::strong_count(&metadata), 1);
}

/// T379：四种合法存储布局（storage layouts）均由创建时的虚函数表（vtable）投影同一附加数据（metadata）。
/// 只经公开选项（public options）选择布局（layout）；不进入队列（queue），不借助私有地址（private addresses）。
#[test]
fn test_current_metadata_uses_original_value_in_each_supported_layout() {
    let owner = TaskThread::new();
    for options in [
        TaskOptions::plain(),
        TaskOptions::intrusive(),
        TaskOptions::intrusive_mpsc(),
        TaskOptions::intrusive_local_and_mpsc(),
    ] {
        let metadata = Arc::new(String::from("original"));
        let expected = Arc::clone(&metadata);
        let future = poll_fn(move |_| {
            let calls = Cell::new(0);
            let result = try_with_current_metadata::<Arc<String>, _>(|value| {
                calls.set(calls.get() + 1);
                assert!(Arc::ptr_eq(value, &expected));
                value.len()
            });
            assert_eq!(calls.get(), 1);
            Poll::Ready(result)
        });
        let mut parts = create(future, NoSchedule, Arc::clone(&metadata), options)
            .unwrap_or_else(|_| panic!("各布局创建（creation for each layout）应成功"));
        assert!(matches!(parts.runnable.run(&owner), Ok(RunOutcome::Finished(TerminationKind::Completed))));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Ok(Ok(8)))));
        assert_eq!(try_current_task_id(), None);
        assert_eq!(owner.live_tasks(), 0);
        drop(parts.join);
        drop(parts.control);
        assert_eq!(Arc::strong_count(&metadata), 1);
    }
}

/// 业务原值（business Future）通过自身公开控制句柄（control handle）提出取消（cancellation），不修改任务（Task）私有状态（private state）。
struct CancelDuringRead {
    control: Rc<std::cell::RefCell<Option<pi_async_task::ControlHandle>>>,
    drops: Arc<AtomicUsize>,
}
impl Future for CancelDuringRead {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        let control = self.control.borrow().as_ref().unwrap().clone();
        assert_eq!(try_current_task_id(), Some(control.id()));
        let result = try_with_current_metadata::<Arc<AtomicUsize>, _>(|value| {
            assert_eq!(value.load(Ordering::SeqCst), 19);
            control.cancel();
            assert_eq!(try_current_task_id(), Some(control.id()));
            value.store(23, Ordering::SeqCst);
            31
        });
        assert_eq!(result, Ok(31));
        Poll::Pending
    }
}
impl Drop for CancelDuringRead {
    fn drop(&mut self) {
        let calls = Cell::new(0);
        assert_eq!(try_with_current_metadata::<Arc<AtomicUsize>, _>(|_| calls.set(1)), Err(CurrentTaskAccessError::NoCurrentTask));
        assert_eq!(calls.get(), 0);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
