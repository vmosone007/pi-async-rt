//! 通过公共入口（public API）复现独立审查发现，并补齐直接行为证据。
//! 危险退出（abort）和全局恐慌钩子（panic hook）在有界子进程（subprocess）内隔离。

use pi_async_task::definitions::{AdmissionError, FutureDropStatus, RunOutcome, TerminationKind};
use pi_async_task::{create, create_local, Runnable, Schedule, TaskError, TaskOptions, TaskThread};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::io::{Read, Write};
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// 只暂停指定线程（thread）的下一次真实分配（allocation）；不访问任务（Task）私有状态。
struct AdmissionAllocator;

#[global_allocator]
static ALLOCATOR: AdmissionAllocator = AdmissionAllocator;
static ALLOCATION_ENTERED: AtomicBool = AtomicBool::new(false);
static ALLOCATION_RELEASED: AtomicBool = AtomicBool::new(false);
static ALLOCATION_TIMED_OUT: AtomicBool = AtomicBool::new(false);
thread_local! {
    static PAUSE_NEXT_ALLOCATION: Cell<bool> = const { Cell::new(false) };
}

/// 分配器（allocator）内不分配、不恐慌（panic）；超时（timeout）只留下证据。
fn allocation_gate() {
    if !PAUSE_NEXT_ALLOCATION.try_with(|armed| armed.replace(false)).unwrap_or(false) {
        return;
    }
    ALLOCATION_ENTERED.store(true, Ordering::SeqCst);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ALLOCATION_RELEASED.load(Ordering::SeqCst) {
        if Instant::now() >= deadline {
            ALLOCATION_TIMED_OUT.store(true, Ordering::SeqCst);
            break;
        }
        std::hint::spin_loop();
        std::thread::yield_now();
    }
}

// SAFETY: 只在真实分配（allocation）前插入无恐慌（panic）的有界暂停；原布局（layout）原样交给System。
unsafe impl GlobalAlloc for AdmissionAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocation_gate();
        // SAFETY: 调用方的分配（allocation）前提原样转交。
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocation_gate();
        // SAFETY: 调用方的分配（allocation）前提原样转交。
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocation_gate();
        // SAFETY: 原指针（pointer）、布局（layout）和新大小原样转交。
        unsafe { System.realloc(pointer, layout, size) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: 只归还System产生的同一分配（allocation）。
        unsafe { System.dealloc(pointer, layout) }
    }
}

/// 子进程（subprocess）分支标识；不影响生产配置（configuration）。
fn child_scenario() -> Option<String> {
    std::env::var("PI_ASYNC_REVIEW_CHILD").ok()
}

/// 精确执行当前独立测试（test），15秒截止并完整收集两个输出流（streams）。
fn run_child(test: &str, scenario: &str) -> Output {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture", "--test-threads=1"])
        .env("PI_ASYNC_REVIEW_CHILD", scenario)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn().unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let out = std::thread::spawn(move || {
        let mut data = Vec::new();
        stdout.read_to_end(&mut data).unwrap();
        data
    });
    let err = std::thread::spawn(move || {
        let mut data = Vec::new();
        stderr.read_to_end(&mut data).unwrap();
        data
    });
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut timed_out = false;
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            timed_out = true;
            child.kill().unwrap();
            break child.wait().unwrap();
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    let output = Output { status, stdout: out.join().unwrap(), stderr: err.join().unwrap() };
    println!("CHILD scenario={scenario} status={status} timeout={timed_out}\nSTDOUT:\n{}\nSTDERR:\n{}",
        String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
    assert!(!timed_out, "子进程（subprocess）超时不能作为目标反例（counterexample）");
    output
}

/// 从子进程（subprocess）的公开观测结果取得整数，不推断缺失证据。
fn fact(output: &Output, key: &str) -> usize {
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout.split_whitespace().find_map(|word| word.strip_prefix(key))
        .unwrap_or_else(|| panic!("缺少观测事实（observation fact）：{key}"))
        .parse().unwrap()
}

/// 不应发生调度（scheduling）的场景；误触发保留为真实失败。
struct NoSchedule;
impl Schedule for NoSchedule {
    fn schedule(&self, _task: Runnable) {
        panic!("unexpected-schedule");
    }
}

/// 合法保存后继执行持有（Runnable），不在交付（delivery）内轮询（poll）或清理原值。
struct Delivery(mpsc::Sender<Runnable>);
impl Schedule for Delivery {
    fn schedule(&self, task: Runnable) {
        self.0.send(task).unwrap_or_else(|_| panic!("delivery-disconnected"));
    }
}

/// 接纳（admission）反例只记录原值是否被错误推进或清理。
struct PendingProbe {
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl Future for PendingProbe {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}
impl Drop for PendingProbe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// 只保存公共结果（public result）和所属域（owner domain）计数。
#[derive(Debug)]
struct AdmissionReport {
    owner: u64,
    success: bool,
    error: Option<AdmissionError>,
    roots: usize,
}

/// 首次分配（allocation）暂停后，让另一所属域（owner domain）完成接纳（admission）。
/// 观测后直接退出子进程（subprocess），避免已证双绑定导致并发析构（concurrent drop）。
fn admission_child(close_after: bool) -> ! {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let parts = create(PendingProbe { polls: polls.clone(), drops: drops.clone() },
        NoSchedule, (), TaskOptions::plain()).unwrap_or_else(|_| panic!("creation-failed"));
    let runnable = Arc::new(parts.runnable);
    let first = runnable.clone();
    let (report_tx, reports) = mpsc::channel();
    let first_report = report_tx.clone();
    let (first_command, first_park) = mpsc::channel::<()>();
    let (second_command, second_park) = mpsc::channel::<()>();
    let (close_tx, closes) = mpsc::channel();
    let first_close = close_tx.clone();
    let _first = std::thread::spawn(move || {
        let owner = TaskThread::new();
        PAUSE_NEXT_ALLOCATION.with(|armed| armed.set(true));
        let registration = first.admit(&owner);
        first_report.send(AdmissionReport { owner: owner.id().get(), success: registration.is_ok(),
            error: registration.as_ref().err().copied(), roots: owner.live_tasks() }).unwrap();
        if first_park.recv_timeout(Duration::from_secs(12)).is_ok() {
            let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| owner.close())).is_err();
            first_close.send((owner.id().get(), panicked)).unwrap();
            let _ = first_park.recv_timeout(Duration::from_secs(12));
        }
        // 保持登记（registration）和所属域（owner domain）存活；主观察者负责进程退出。
        std::hint::black_box(&registration);
        std::process::abort();
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ALLOCATION_ENTERED.load(Ordering::SeqCst) {
        assert!(Instant::now() < deadline, "allocation-gate-not-reached");
        std::thread::yield_now();
    }
    let second = runnable.clone();
    let _second = std::thread::spawn(move || {
        let owner = TaskThread::new();
        let registration = second.admit(&owner);
        report_tx.send(AdmissionReport { owner: owner.id().get(), success: registration.is_ok(),
            error: registration.as_ref().err().copied(), roots: owner.live_tasks() }).unwrap();
        if second_park.recv_timeout(Duration::from_secs(12)).is_ok() {
            let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| owner.close())).is_err();
            close_tx.send((owner.id().get(), panicked)).unwrap();
            let _ = second_park.recv_timeout(Duration::from_secs(12));
        }
        std::hint::black_box(&registration);
        std::process::abort();
    });
    let second = reports.recv_timeout(Duration::from_secs(3)).unwrap();
    ALLOCATION_RELEASED.store(true, Ordering::SeqCst);
    let first = reports.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(!ALLOCATION_TIMED_OUT.load(Ordering::SeqCst));
    let snapshot = runnable.snapshot();
    let winner = if second.success { second.owner } else { first.owner };
    println!("ADMISSION_REPORTS first={first:?} second={second:?} bound={:?}", snapshot.owner);
    println!("ADMISSION_FACTS successes={} roots={} owner_stable={} polls={} drops={} errors={:?}/{:?}",
        usize::from(first.success) + usize::from(second.success), first.roots + second.roots,
        usize::from(snapshot.owner.map(|id| id.get()) == Some(winner)),
        polls.load(Ordering::SeqCst), drops.load(Ordering::SeqCst), first.error, second.error);
    if close_after {
        // 两次关闭（close）严格串行；不以并发析构（concurrent drop）触发未定义行为（UB）。
        second_command.send(()).unwrap();
        let second_close = closes.recv_timeout(Duration::from_secs(3)).unwrap();
        first_command.send(()).unwrap();
        let first_close = closes.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(first_close.0, first.owner);
        assert_eq!(second_close.0, second.owner);
        println!("CLOSE_FACTS first_panicked={} second_panicked={} close_drops={}",
            usize::from(first_close.1), usize::from(second_close.1), drops.load(Ordering::SeqCst));
    }
    std::io::stdout().flush().unwrap();
    std::process::exit(0);
}

/// @test T83；@api Runnable::admit；@impl ref_admit_inner；@design-doc T83。
/// 同一共享执行持有（Arc<Runnable>）只有一个所属域（owner domain）可成功接纳（admission）。
#[test]
fn test_concurrent_admission_has_one_owner_and_one_registration() {
    if child_scenario().as_deref() == Some("admission") { admission_child(false); }
    let output = run_child("test_concurrent_admission_has_one_owner_and_one_registration", "admission");
    assert!(output.status.success());
    assert_eq!(fact(&output, "polls="), 0);
    assert_eq!(fact(&output, "drops="), 0);
    assert_eq!((fact(&output, "successes="), fact(&output, "roots="), fact(&output, "owner_stable=")),
        (1, 1, 1), "接纳（admission）须唯一，失败不得遗留清理根（cleanup root）或改写原目标");
}

/// @test T83/T75；@api Runnable::admit/TaskThread::close；@impl ref_admit_inner；@design-doc T83。
/// 双接纳（duplicate admission）后的顺序关闭（sequential close）不得访问错误所属域（wrong owner）。
#[test]
fn test_concurrent_admission_does_not_corrupt_sequential_owner_close() {
    if child_scenario().as_deref() == Some("admission-close") { admission_child(true); }
    let output = run_child("test_concurrent_admission_does_not_corrupt_sequential_owner_close", "admission-close");
    assert!(output.status.success());
    assert_eq!(fact(&output, "close_drops="), 1);
    assert_eq!((fact(&output, "first_panicked="), fact(&output, "second_panicked=")), (0, 0),
        "合法关闭（close）不得因双绑定（duplicate binding）触发错误所属域断言");
}

/// @test T83；@api Runnable::admit；@impl ref_admit_inner；@design-doc T83。
/// 关闭所属域（closed owner）的拒绝不绑定、不取消、不消耗初始持有（Runnable）。
#[test]
fn test_closed_owner_admission_preserves_unbound_task() {
    let owner = TaskThread::new();
    owner.close();
    let parts = create(std::future::pending::<()>(), NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("creation-failed"));
    let before = parts.control.snapshot();
    assert!(matches!(parts.runnable.admit(&owner), Err(AdmissionError::OwnerClosed)));
    assert_eq!(parts.control.snapshot(), before);
    assert_eq!(owner.live_tasks(), 0);
    drop(parts.runnable);
    assert_eq!(parts.control.snapshot().termination, Some(TerminationKind::Abandoned));
}

/// 仅用于真正双恐慌（double panic）对照的用户字段（field），默认析构（drop）无副作用。
struct DoublePanicField(bool);
impl Drop for DoublePanicField {
    fn drop(&mut self) {
        if self.0 && std::thread::panicking() { panic!("owner-second-drop"); }
    }
}

/// 两个非Send原值（local Futures）中仅首次清理（cleanup）独立恐慌（panic）。
struct OwnerDropProbe {
    drops: Arc<AtomicUsize>,
    fail_first: bool,
    thread: std::thread::ThreadId,
    local: std::rc::Rc<()>,
    _double_panic: DoublePanicField,
}
impl Future for OwnerDropProbe {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> { Poll::Pending }
}
impl Drop for OwnerDropProbe {
    fn drop(&mut self) {
        let count = self.drops.fetch_add(1, Ordering::SeqCst) + 1;
        eprintln!("FUTURE_DROP count={count} thread_ok={} already_panicking={} local_refs={}",
            usize::from(self.thread == std::thread::current().id()), std::thread::panicking(),
            std::rc::Rc::strong_count(&self.local));
        if self.fail_first && count == 1 { panic!("owner-first-drop"); }
    }
}

/// 对照正常退出、保留观察句柄（control handles）和仅剩登记根（registry root）三种边界。
fn owner_drop_child(retain: bool, fail_first: bool, double_panic: bool) -> ! {
    let hooks = Arc::new(AtomicUsize::new(0));
    let hook_count = hooks.clone();
    std::panic::set_hook(Box::new(move |info| {
        let count = hook_count.fetch_add(1, Ordering::SeqCst) + 1;
        eprintln!("PANIC_HOOK count={count} payload={:?}", info.payload().downcast_ref::<&str>());
    }));
    let drops = Arc::new(AtomicUsize::new(0));
    let owner = TaskThread::new();
    let mut controls = Vec::new();
    for _ in 0..2 {
        let parts = create_local(&owner, OwnerDropProbe { drops: drops.clone(), fail_first,
            thread: std::thread::current().id(), local: std::rc::Rc::new(()), _double_panic: DoublePanicField(double_panic) },
            NoSchedule, (), TaskOptions::plain()).unwrap_or_else(|_| panic!("creation-failed"));
        assert!(matches!(parts.runnable.run(&owner), Ok(RunOutcome::Pending)));
        drop(parts.join);
        if retain { controls.push(parts.control); } else { drop(parts.control); }
    }
    assert_eq!(owner.live_tasks(), 2);
    eprintln!("OWNER_DROP_BEGIN retain={retain} fail_first={fail_first} roots=2");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner)));
    if fail_first {
        assert_eq!(result.unwrap_err().downcast_ref::<&str>(), Some(&"owner-first-drop"));
    } else {
        assert!(result.is_ok());
    }
    let unconfirmed = controls.iter().filter(|control|
        control.snapshot().execution == pi_async_task::definitions::ExecutionStatus::CleanupUnconfirmed).count();
    let remaining = controls.iter().filter(|control|
        control.snapshot().execution == pi_async_task::definitions::ExecutionStatus::Idle).count();
    println!("OWNER_DROP_FACTS drops={} hooks={} unconfirmed={unconfirmed} remaining={remaining}",
        drops.load(Ordering::SeqCst), hooks.load(Ordering::SeqCst));
    // 所属域（owner domain）已消失；这里只保全剩余责任证据，不让观察句柄释放掩盖首个结果。
    std::io::stdout().flush().unwrap();
    std::process::exit(0);
}

/// @test T75/T76/T94；@api TaskThread::drop；@impl thread_drop_inner/ref_drop_inner；@design-doc T76。
/// 首次独立析构恐慌（drop panic）须可向外展开（unwind），不被未处理登记的末引用中止（abort）替代。
#[test]
fn test_owner_drop_first_cleanup_panic_propagates_without_last_ref_abort() {
    match child_scenario().as_deref() {
        Some("owner-normal") => owner_drop_child(false, false, false),
        Some("owner-retained") => owner_drop_child(true, true, false),
        Some("owner-last-root") => owner_drop_child(false, true, false),
        Some("owner-double-panic") => owner_drop_child(false, true, true),
        _ => {}
    }
    let test = "test_owner_drop_first_cleanup_panic_propagates_without_last_ref_abort";
    let normal = run_child(test, "owner-normal");
    let retained = run_child(test, "owner-retained");
    let last_root = run_child(test, "owner-last-root");
    let double = run_child(test, "owner-double-panic");
    assert!(!double.status.success());
    assert!(String::from_utf8_lossy(&double.stderr).contains("PANIC_HOOK count=2 payload=Some(\"owner-second-drop\")"));
    assert!(normal.status.success());
    assert_eq!((fact(&normal, "drops="), fact(&normal, "hooks=")), (2, 0));
    assert!(retained.status.success());
    assert_eq!((fact(&retained, "drops="), fact(&retained, "hooks="),
        fact(&retained, "unconfirmed="), fact(&retained, "remaining=")), (2, 1, 1, 0));
    let stderr = String::from_utf8_lossy(&last_root.stderr);
    assert!(stderr.contains("PANIC_HOOK count=1 payload=Some(\"owner-first-drop\")"));
    assert!(!stderr.contains("PANIC_HOOK count=2"), "不能误把真正双恐慌（double panic）当末引用问题");
    assert!(last_root.status.success(), "首次独立析构恐慌（drop panic）被末引用中止（abort）替代");
    assert_eq!((fact(&last_root, "drops="), fact(&last_root, "hooks=")), (2, 1));
    for output in [&normal, &retained, &last_root] {
        assert!(!String::from_utf8_lossy(&output.stderr).contains("thread_ok=0"));
    }
}

/// 在单一子进程（subprocess）安装宿主钩子（host hook），即使断言展开也先恢复原钩子（hook）。
fn with_capture_hook(body: impl FnOnce(&Arc<parking_lot::Mutex<Vec<bool>>>)) {
    let captures = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let recorded = captures.clone();
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |_| {
        let captured = pi_async_task::capture_current_panic_backtrace();
        recorded.lock().push(captured);
    }));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(&captures)));
    std::panic::set_hook(previous);
    if let Err(payload) = result { std::panic::resume_unwind(payload); }
}

/// 从唯一结果句柄（JoinHandle）领取本次恐慌（panic）资料并核固定身份（TaskId）。
fn captured_panic(value: u32, enabled: bool) -> (pi_async_task::definitions::TaskId, pi_async_task::join::PanicRecord) {
    let owner = TaskThread::new();
    let mut options = TaskOptions::plain();
    options.capture_panic_backtrace = enabled;
    let future = std::future::poll_fn(move |_| -> Poll<()> { std::panic::panic_any(value); });
    let mut parts = create(future, NoSchedule, (), options).unwrap_or_else(|_| panic!("creation-failed"));
    let id = parts.control.id();
    assert_eq!(parts.join.id(), id);
    assert!(matches!(parts.runnable.run(&owner), Ok(RunOutcome::Finished(TerminationKind::Panicked))));
    let mut context = Context::from_waker(std::task::Waker::noop());
    let record = match Pin::new(&mut parts.join).poll(&mut context) {
        Poll::Ready(Err(TaskError::Panicked(record))) => record,
        _ => panic!("panic-record-missing"),
    };
    assert_eq!(record.payload.downcast_ref::<u32>(), Some(&value));
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut context), Poll::Ready(Err(TaskError::AlreadyTaken))));
    (id, record)
}

/// @test T102/T110；@api capture_current_panic_backtrace/Runnable::run/JoinHandle::poll。
/// @impl capture_current_backtrace_inner/storage_run_inner；@design-doc T102。
/// 无当前任务（current task）与选项关闭均false；启用记录按真实任务身份（TaskId）隔离。
#[test]
fn test_panic_capture_enabled_tracks_task_and_does_not_leak_records() {
    if child_scenario().as_deref() == Some("capture") {
        with_capture_hook(|captures| {
            assert!(!pi_async_task::capture_current_panic_backtrace());
            let (_, disabled) = captured_panic(11, false);
            let (first_id, first) = captured_panic(12, true);
            let (second_id, second) = captured_panic(13, true);
            assert_ne!(first_id, second_id);
            assert!(disabled.backtraces.is_empty());
            assert_eq!(first.backtraces.len(), 1);
            assert_eq!(second.backtraces.len(), 1);
            for record in [&first, &second] {
                assert_eq!(record.backtraces[0].status(), std::backtrace::BacktraceStatus::Captured);
            }
            assert_eq!(*captures.lock(), vec![false, true, true]);
            assert!(!pi_async_task::capture_current_panic_backtrace());
            println!("CAPTURE_FACTS first={first_id:?} second={second_id:?} disabled=0 first_records=1 second_records=1");
        });
        return;
    }
    let output = run_child("test_panic_capture_enabled_tracks_task_and_does_not_leak_records", "capture");
    assert!(output.status.success());
}

/// 两个真实恐慌起点（panic origins）用独立符号保留调用栈（stack）辨认入口。
#[inline(never)]
fn first_panic_origin() -> ! { std::panic::panic_any(21_u32); }
#[inline(never)]
fn second_panic_origin() -> ! { std::panic::panic_any(22_u32); }

/// @test T110/T102；@api Runnable::run/JoinHandle::poll；@impl storage_run_inner；@design-doc T110。
/// 内部捕获（catch_unwind）两次后重新展开（resume_unwind），全部起点记录仍属于本次轮询（poll）。
#[test]
fn test_poll_internal_catches_and_resume_preserve_all_panic_origins() {
    if child_scenario().as_deref() == Some("multi-panic") {
        with_capture_hook(|captures| {
            let owner = TaskThread::new();
            let mut options = TaskOptions::plain();
            options.capture_panic_backtrace = true;
            let future = std::future::poll_fn(|_| -> Poll<()> {
                let first = std::panic::catch_unwind(first_panic_origin).unwrap_err();
                assert_eq!(first.downcast_ref::<u32>(), Some(&21));
                let final_payload = std::panic::catch_unwind(second_panic_origin).unwrap_err();
                std::panic::resume_unwind(final_payload);
            });
            let mut parts = create(future, NoSchedule, (), options).unwrap_or_else(|_| panic!("creation-failed"));
            let id = parts.control.id();
            assert!(matches!(parts.runnable.run(&owner), Ok(RunOutcome::Finished(TerminationKind::Panicked))));
            let mut context = Context::from_waker(std::task::Waker::noop());
            let record = match Pin::new(&mut parts.join).poll(&mut context) {
                Poll::Ready(Err(TaskError::Panicked(record))) => record,
                _ => panic!("panic-record-missing"),
            };
            assert_eq!(record.payload.downcast_ref::<u32>(), Some(&22));
            assert_eq!(record.backtraces.len(), 2);
            assert!(record.backtraces[0].to_string().contains("first_panic_origin"));
            assert!(record.backtraces[1].to_string().contains("second_panic_origin"));
            assert_eq!(*captures.lock(), vec![true, true]);
            println!("MULTI_PANIC_FACTS task={id:?} records=2 final_payload=22 hook_calls=2");
        });
        return;
    }
    let output = run_child("test_poll_internal_catches_and_resume_preserve_all_panic_origins", "multi-panic");
    assert!(output.status.success());
}

/// 非Unpin输出（output）可合法移动领取；其析构（drop）必须晚于原值清理。
#[derive(Debug)]
struct PinnedOutput {
    value: String,
    drops: Arc<AtomicUsize>,
    future_drops: Arc<AtomicUsize>,
    observed_future_drops: Arc<AtomicUsize>,
    _pin: PhantomPinned,
}
impl Drop for PinnedOutput {
    fn drop(&mut self) {
        self.observed_future_drops.store(self.future_drops.load(Ordering::SeqCst), Ordering::SeqCst);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// 非Unpin原值（Future）跨两次轮询（poll）记录固定地址，原址析构（drop）另行记录。
struct PinnedFuture {
    polls: Cell<usize>,
    first_address: Arc<AtomicUsize>,
    second_address: Arc<AtomicUsize>,
    drop_address: Arc<AtomicUsize>,
    future_drops: Arc<AtomicUsize>,
    output_drops: Arc<AtomicUsize>,
    observed_future_drops: Arc<AtomicUsize>,
    _pin: PhantomPinned,
}
impl Future for PinnedFuture {
    type Output = PinnedOutput;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_ref().get_ref();
        let address = std::ptr::from_ref(this) as usize;
        let count = this.polls.get() + 1;
        this.polls.set(count);
        if count == 1 {
            this.first_address.store(address, Ordering::SeqCst);
            Poll::Pending
        } else {
            this.second_address.store(address, Ordering::SeqCst);
            Poll::Ready(PinnedOutput { value: String::from("unique-output"), drops: this.output_drops.clone(),
                future_drops: this.future_drops.clone(), observed_future_drops: this.observed_future_drops.clone(), _pin: PhantomPinned })
        }
    }
}
impl Drop for PinnedFuture {
    fn drop(&mut self) {
        self.drop_address.store(std::ptr::from_ref(self) as usize, Ordering::SeqCst);
        self.future_drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// 同一公共执行链（public execution path）分别验证唯一领取及提前放弃领取。
fn verify_pinned_output(abandon: bool) {
    let owner = TaskThread::new();
    let (sender, receiver) = mpsc::channel();
    let first = Arc::new(AtomicUsize::new(0));
    let second = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let future_drops = Arc::new(AtomicUsize::new(0));
    let output_drops = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(AtomicUsize::new(0));
    let parts = create(PinnedFuture { polls: Cell::new(0), first_address: first.clone(), second_address: second.clone(),
        drop_address: dropped.clone(), future_drops: future_drops.clone(), output_drops: output_drops.clone(),
        observed_future_drops: observed.clone(), _pin: PhantomPinned }, Delivery(sender), (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("creation-failed"));
    let mut join = Some(parts.join);
    assert!(matches!(parts.runnable.run(&owner), Ok(RunOutcome::Pending)));
    if abandon { drop(join.take()); }
    parts.control.wake();
    let followup = receiver.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(matches!(followup.run(&owner), Ok(RunOutcome::Finished(TerminationKind::Completed))));
    assert_ne!(first.load(Ordering::SeqCst), 0);
    assert_eq!(first.load(Ordering::SeqCst), second.load(Ordering::SeqCst));
    assert_eq!(first.load(Ordering::SeqCst), dropped.load(Ordering::SeqCst));
    assert_eq!(future_drops.load(Ordering::SeqCst), 1);
    assert_eq!(parts.control.snapshot().future_drop, FutureDropStatus::Confirmed);
    assert_eq!(owner.live_tasks(), 0);
    if let Some(mut join) = join {
        assert_eq!(output_drops.load(Ordering::SeqCst), 0);
        let mut context = Context::from_waker(std::task::Waker::noop());
        let value = match Pin::new(&mut join).poll(&mut context) {
            Poll::Ready(Ok(value)) => value,
            _ => panic!("output-missing"),
        };
        assert_eq!(value.value, "unique-output");
        assert!(matches!(Pin::new(&mut join).poll(&mut context), Poll::Ready(Err(TaskError::AlreadyTaken))));
        drop(join);
        assert_eq!(output_drops.load(Ordering::SeqCst), 0);
        drop(value);
    }
    assert_eq!(output_drops.load(Ordering::SeqCst), 1);
    assert_eq!(observed.load(Ordering::SeqCst), 1);
}

/// @test T110/T111/T59/T114；@api Runnable::run/JoinHandle::poll；@design-doc T110。
#[test]
fn test_non_unpin_output_is_taken_once_after_future_drops_in_place() {
    verify_pinned_output(false);
}

/// @test T110/T111/T60；@api Runnable::run/JoinHandle::drop；@design-doc T110。
#[test]
fn test_abandoned_non_unpin_output_is_dropped_once_after_future() {
    verify_pinned_output(true);
}

/// 只记录实际事件（event）的身份和次序，不代补停止事实。
#[cfg(feature = "observe")]
#[derive(Default)]
struct Events(parking_lot::Mutex<Vec<(pi_async_task::definitions::TaskId, pi_async_task::task::TaskEvent)>>);
#[cfg(feature = "observe")]
impl pi_async_task::task::TaskObserver for Events {
    fn event(&self, id: pi_async_task::definitions::TaskId, event: pi_async_task::task::TaskEvent) {
        self.0.lock().push((id, event));
    }
}

/// 通过同一身份（TaskId）直接核三项事实及一次性次序，不要求未冻结的通知先后。
#[cfg(feature = "observe")]
fn verify_stop_events(cancel: bool) {
    use pi_async_task::task::TaskEvent;
    let owner = TaskThread::new();
    let events = Arc::new(Events::default());
    let drops = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let mut options = TaskOptions::plain();
    options.observer = Some(events.clone());
    let mut parts = create(PendingProbe { polls: polls.clone(), drops: drops.clone() }, NoSchedule, (), options)
        .unwrap_or_else(|_| panic!("creation-failed"));
    let id = parts.control.id();
    if cancel {
        parts.control.cancel();
        assert!(matches!(parts.runnable.run(&owner), Ok(RunOutcome::Finished(TerminationKind::Cancelled))));
    } else {
        drop(parts.runnable);
    }
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let mut context = Context::from_waker(std::task::Waker::noop());
    let result = Pin::new(&mut parts.join).poll(&mut context);
    if cancel { assert!(matches!(result, Poll::Ready(Err(TaskError::Cancelled)))); }
    else { assert!(matches!(result, Poll::Ready(Err(TaskError::Abandoned)))); }
    drop(parts.join);
    drop(parts.control);
    drop(owner);
    let recorded = events.0.lock().clone();
    println!("STOP_EVENTS cancel={cancel} task={id:?} events={recorded:?}");
    assert!(recorded.iter().all(|(actual, _)| *actual == id));
    let positions = |event| recorded.iter().enumerate().filter_map(|(index, (_, actual))|
        if *actual == event { Some(index) } else { None }).collect::<Vec<_>>();
    let stopped = positions(TaskEvent::PollingStopped);
    let cleaned = positions(TaskEvent::FutureDropped);
    let reclaimed = positions(TaskEvent::ReclaimStarted);
    assert_eq!(stopped.len(), 1, "永久停止轮询（PollingStopped）事实缺失或重复");
    assert_eq!(cleaned.len(), 1);
    assert_eq!(reclaimed.len(), 1);
    assert!(stopped[0] < cleaned[0] && cleaned[0] < reclaimed[0]);
}

/// @test T110/T111；@api Runnable::run/TaskObserver::event；@impl storage_run_inner；@design-doc D106。
/// 正常就绪（Ready）对照须保留三项可辨事实，而不是把全部停止观测（stop observation）误报为缺失。
#[cfg(feature = "observe")]
#[test]
fn test_ready_completion_records_stopped_cleanup_and_reclaim_in_order() {
    use pi_async_task::task::TaskEvent;
    let events = Arc::new(Events::default());
    let mut options = TaskOptions::plain();
    options.observer = Some(events.clone());
    let owner = TaskThread::new();
    let mut parts = create(std::future::ready(7), NoSchedule, (), options)
        .unwrap_or_else(|_| panic!("creation-failed"));
    let id = parts.control.id();
    assert!(matches!(parts.runnable.run(&owner), Ok(RunOutcome::Finished(TerminationKind::Completed))));
    let mut context = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut context), Poll::Ready(Ok(7))));
    drop(parts.join);
    drop(parts.control);
    drop(owner);
    let recorded = events.0.lock();
    let actual: Vec<_> = recorded.iter().filter_map(|(actual_id, event)| {
        assert_eq!(*actual_id, id);
        matches!(event, TaskEvent::PollingStopped | TaskEvent::FutureDropped | TaskEvent::ReclaimStarted).then_some(*event)
    }).collect();
    assert_eq!(actual, vec![TaskEvent::PollingStopped, TaskEvent::FutureDropped, TaskEvent::ReclaimStarted]);
}

/// @test T111/T119；@api Runnable::drop/TaskObserver::event；@impl storage_drop_future_inner。
/// @design-doc D106；初始放弃（initial abandonment）也须记录永久停止轮询（PollingStopped）。
#[cfg(feature = "observe")]
#[test]
fn test_initial_abandonment_records_polling_stopped_before_cleanup() {
    verify_stop_events(false);
}

/// @test T111/T119；@api ControlHandle::cancel/Runnable::run/TaskObserver::event。
/// @impl storage_drop_future_inner；@design-doc D106；正常取消（cancellation）须记录永久停止事实。
#[cfg(feature = "observe")]
#[test]
fn test_normal_cancellation_records_polling_stopped_before_cleanup() {
    verify_stop_events(true);
}

/// @test T75/T111/T119；@api TaskThread::close/Runnable::run/TaskObserver::event。
/// @impl storage_drop_future_inner；@design-doc D106；重复关闭（close）及迟到清理持有不重复报告事实。
#[cfg(feature = "observe")]
#[test]
fn test_owner_close_records_stopped_cleanup_and_reclaim_once() {
    use pi_async_task::task::TaskEvent;
    let owner = TaskThread::new();
    let events = Arc::new(Events::default());
    let drops = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let mut options = TaskOptions::plain();
    options.observer = Some(events.clone());
    let mut parts = create_local(&owner, PendingProbe { polls: polls.clone(), drops: drops.clone() },
        NoSchedule, (), options).unwrap_or_else(|_| panic!("creation-failed"));
    let id = parts.control.id();
    owner.close();
    owner.close();
    assert!(matches!(parts.runnable.run(&owner), Ok(RunOutcome::Finished(TerminationKind::Cancelled))));
    assert_eq!(owner.live_tasks(), 0);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let mut context = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut context), Poll::Ready(Err(TaskError::Cancelled))));
    drop(parts.join);
    drop(parts.control);
    drop(owner);
    let recorded = events.0.lock();
    let facts: Vec<_> = recorded.iter().filter_map(|(actual, event)| {
        assert_eq!(*actual, id);
        matches!(event, TaskEvent::PollingStopped | TaskEvent::FutureDropped | TaskEvent::ReclaimStarted).then_some(*event)
    }).collect();
    assert_eq!(facts, vec![TaskEvent::PollingStopped, TaskEvent::FutureDropped, TaskEvent::ReclaimStarted]);
}

/// @test T111/T123；@api ControlHandle::cancel/Runnable::run/TaskThread::close/TaskObserver::event。
/// @impl storage_drop_future_inner；@design-doc D106；原值析构（drop）失败不得虚报清理完成。
#[cfg(feature = "observe")]
#[test]
fn test_cleanup_panic_does_not_report_future_dropped() {
    use pi_async_task::task::TaskEvent;
    let owner = TaskThread::new();
    let events = Arc::new(Events::default());
    let drops = Arc::new(AtomicUsize::new(0));
    let mut options = TaskOptions::plain();
    options.observer = Some(events.clone());
    let mut parts = create_local(&owner, OwnerDropProbe { drops: drops.clone(), fail_first: true,
        thread: std::thread::current().id(), local: std::rc::Rc::new(()), _double_panic: DoublePanicField(false) },
        NoSchedule, (), options).unwrap_or_else(|_| panic!("creation-failed"));
    let id = parts.control.id();
    parts.control.cancel();
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| { let _ = parts.runnable.run(&owner); }));
    assert_eq!(failure.unwrap_err().downcast_ref::<&str>(), Some(&"owner-first-drop"));
    assert_eq!(parts.control.snapshot().future_drop, FutureDropStatus::Unconfirmed);
    assert_eq!(parts.control.snapshot().termination, Some(TerminationKind::Cancelled));
    owner.close();
    owner.close();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let mut context = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut context), Poll::Ready(Err(TaskError::CleanupUnconfirmed))));
    drop(parts.join);
    drop(parts.control);
    drop(owner);
    let recorded = events.0.lock();
    assert!(recorded.iter().all(|(actual, _)| *actual == id));
    assert!(!recorded.iter().any(|(_, event)| *event == TaskEvent::FutureDropped));
    assert_eq!(recorded.iter().filter(|(_, event)| *event == TaskEvent::ReclaimStarted).count(), 1);
}

/// 只在永久停止轮询事件（PollingStopped）展开（unwind），此前先记录真实调用。
#[cfg(feature = "observe")]
struct PanicOnStopped(Arc<Events>);
#[cfg(feature = "observe")]
impl pi_async_task::task::TaskObserver for PanicOnStopped {
    fn event(&self, id: pi_async_task::definitions::TaskId, event: pi_async_task::task::TaskEvent) {
        self.0.0.lock().push((id, event));
        if event == pi_async_task::task::TaskEvent::PollingStopped { panic!("stop-observer"); }
    }
}

/// @test T111/T119/T122；@api Runnable::run/Runnable::drop/TaskObserver::event/JoinHandle::poll。
/// @impl storage_drop_future_inner；@design-doc D106；停止事件接收者展开不丢结果或重复清理。
#[cfg(feature = "observe")]
#[test]
fn test_stopped_observer_panic_preserves_cleanup_and_result() {
    use pi_async_task::task::TaskEvent;
    for cancel in [false, true] {
        let owner = TaskThread::new();
        let events = Arc::new(Events::default());
        let drops = Arc::new(AtomicUsize::new(0));
        let polls = Arc::new(AtomicUsize::new(0));
        let mut options = TaskOptions::plain();
        options.observer = Some(Arc::new(PanicOnStopped(events.clone())));
        let mut parts = create(PendingProbe { polls: polls.clone(), drops: drops.clone() }, NoSchedule, (), options)
            .unwrap_or_else(|_| panic!("creation-failed"));
        let id = parts.control.id();
        if cancel { parts.control.cancel(); }
        let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if cancel { let _ = parts.runnable.run(&owner); } else { drop(parts.runnable); }
        }));
        assert_eq!(failure.unwrap_err().downcast_ref::<&str>(), Some(&"stop-observer"));
        assert_eq!(parts.control.snapshot().future_drop, FutureDropStatus::Confirmed);
        assert_eq!(parts.control.snapshot().termination,
            Some(if cancel { TerminationKind::Cancelled } else { TerminationKind::Abandoned }));
        owner.close();
        owner.close();
        assert_eq!(owner.live_tasks(), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        let mut context = Context::from_waker(std::task::Waker::noop());
        let result = Pin::new(&mut parts.join).poll(&mut context);
        if cancel { assert!(matches!(result, Poll::Ready(Err(TaskError::Cancelled)))); }
        else { assert!(matches!(result, Poll::Ready(Err(TaskError::Abandoned)))); }
        assert!(matches!(Pin::new(&mut parts.join).poll(&mut context), Poll::Ready(Err(TaskError::AlreadyTaken))));
        drop(parts.join);
        drop(parts.control);
        drop(owner);
        let recorded = events.0.lock();
        assert!(recorded.iter().all(|(actual, _)| *actual == id));
        assert_eq!(recorded.iter().filter(|(_, event)| *event == TaskEvent::PollingStopped).count(), 1);
        assert!(!recorded.iter().any(|(_, event)| *event == TaskEvent::FutureDropped));
        assert_eq!(recorded.iter().filter(|(_, event)| *event == TaskEvent::ReclaimStarted).count(), 1);
    }
}
