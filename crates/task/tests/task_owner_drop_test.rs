//! 所属域析构（owner-domain drop）的公共接口（public API）异常边界测试。
//! 采用2026-11-09-owner-drop.1；所有危险退出及全局恐慌钩子（panic hook）均在有界子进程隔离。
//! 等待槽（NotifySlot）的内部1→3窗口没有公共暂停点，本文件不声称直接观测该原子分支。

use pi_async_task::definitions::{CancellationError, CancellationOutcome, ExecutionStatus, FutureDropStatus, RunOutcome, TaskPhase, TerminationKind};
use pi_async_task::{create, create_local, ControlHandle, ExternalWaitSource, ExternalWaiter, Runnable, Schedule, TaskError, TaskOptions, TaskThread};
use std::future::Future;
use std::io::{Read, Write};
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::process::{Command, Output, Stdio};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker};
use std::time::{Duration, Instant};

/// 每个入口只执行自己的子场景；父进程不安装恐慌钩子（panic hook）。
fn child_case(case: &str) -> bool {
    std::env::var("PI_ASYNC_OWNER_DROP_CHILD").as_deref() == Ok(case)
}

/// 以15秒截止收集两个输出流（streams），超时不是目标失败（RED）。
fn run_case(test: &str, case: &str) -> Output {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture", "--test-threads=1"])
        .env("PI_ASYNC_OWNER_DROP_CHILD", case)
        .stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let out = std::thread::spawn(move || { let mut data = Vec::new(); stdout.read_to_end(&mut data).unwrap(); data });
    let err = std::thread::spawn(move || { let mut data = Vec::new(); stderr.read_to_end(&mut data).unwrap(); data });
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut timed_out = false;
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() { break status; }
        if Instant::now() >= deadline {
            timed_out = true;
            child.kill().unwrap();
            break child.wait().unwrap();
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    let output = Output { status, stdout: out.join().unwrap(), stderr: err.join().unwrap() };
    println!("OWNER_CASE case={case} status={status} timeout={timed_out}\nSTDOUT:\n{}\nSTDERR:\n{}",
        String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
    assert!(String::from_utf8_lossy(&output.stdout).contains("running 1 test"), "exact-test-not-executed");
    assert!(!timed_out, "owner-case-timeout");
    assert!(!String::from_utf8_lossy(&output.stderr).contains("gate-timeout"), "交错门（gate）超时不是合法反例");
    output
}

/// 只解析实际打印的数字事实（facts），缺失即失败，不能推算默认结果。
fn fact(output: &Output, key: &str) -> usize {
    String::from_utf8_lossy(&output.stdout).split_whitespace()
        .find_map(|word| word.strip_prefix(key)).unwrap_or_else(|| panic!("missing-fact:{key}")).parse().unwrap()
}

/// 中止（abort）用例必须是目标平台的真实异常退出，测试框架的101不能冒充中止。
fn assert_aborted(output: &Output) {
    assert!(!output.status.success());
    #[cfg(windows)]
    assert_eq!(output.status.code(), Some(0xc0000409_u32 as i32));
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(output.status.signal(), Some(6));
    }
}

/// 子进程（subprocess）退出前刷新证据，不让失败实现的遗留持有析构掩盖观测。
fn finish() -> ! { std::io::stdout().flush().unwrap(); std::io::stderr().flush().unwrap(); std::process::exit(0); }

/// 不允许发生安排（Schedule）的场景，错误调用继续暴露。
struct NoSchedule;
impl Schedule for NoSchedule { fn schedule(&self, _task: Runnable) { panic!("unexpected-schedule"); } }

/// 仅合法保存清理持有（Runnable）；不代执行、不清理原异步计算（Future）。
#[derive(Clone, Default)]
struct Delivery(Arc<parking_lot::Mutex<Vec<Runnable>>>);
impl Schedule for Delivery { fn schedule(&self, task: Runnable) { self.0.lock().push(task); } }

/// 宿主为本测试实际承接外部等待身份（external waiter）的存活责任。
struct Alive;
impl ExternalWaitSource for Alive { fn is_alive(&self) -> bool { true } }
/// 每次建立由宿主实际承担责任的外部等待者（ExternalWaiter）；不伪造受管任务（Task）身份。
fn external() -> ExternalWaiter { ExternalWaiter::new(Arc::new(Alive)) }

/// 原载荷（panic payload）不实现Clone；内部唯一身份保持，析构另行计数。
struct IdentityPayload { token: Arc<()>, drops: Arc<AtomicUsize> }
impl Drop for IdentityPayload { fn drop(&mut self) { self.drops.fetch_add(1, Ordering::SeqCst); } }

/// 记录真实钩子（hook）调用次数及载荷内部固定身份；不假定panic_any装箱前后地址相同。
struct Hooks { calls: Arc<AtomicUsize>, identity: Arc<AtomicUsize> }
/// 仅由隔离子进程（subprocess）调用；钩子（hook）记录异常身份，不接管生产恢复。
fn install_hook() -> Hooks {
    let calls = Arc::new(AtomicUsize::new(0));
    let identity = Arc::new(AtomicUsize::new(0));
    let hook_calls = calls.clone();
    let hook_identity = identity.clone();
    std::panic::set_hook(Box::new(move |info| {
        let n = hook_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(value) = info.payload().downcast_ref::<IdentityPayload>() {
            hook_identity.store(Arc::as_ptr(&value.token) as usize, Ordering::SeqCst);
            eprintln!("OWNER_HOOK n={n} identity-payload");
        } else { eprintln!("OWNER_HOOK n={n} payload={:?}", info.payload().downcast_ref::<&str>()); }
    }));
    Hooks { calls, identity }
}

/// 按真实析构（drop）次序而非登记表（HashMap）顺序注入首错或独立二错。
#[derive(Clone)]
enum Fault { None, First, Every, Identity(Arc<()>, Arc<AtomicUsize>) }
/// 两个原值的尝试、线程（thread）和轮询（poll）事实；地址只用作载荷身份比较，不恢复指针（pointer）。
#[derive(Default)]
struct Probes { drops: AtomicUsize, polls: AtomicUsize, each: [AtomicUsize; 2], thread_ok: AtomicUsize, payload_address: AtomicUsize }
/// Rc确保原值真实不满足Send；每次析构（drop）按实际顺序决定是否注入独立错误。
struct LocalProbe { stats: Arc<Probes>, index: usize, thread: std::thread::ThreadId, local: Rc<()>, fault: Fault }
impl Future for LocalProbe {
    type Output = usize;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<usize> {
        self.stats.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}
impl Drop for LocalProbe {
    fn drop(&mut self) {
        let n = self.stats.drops.fetch_add(1, Ordering::SeqCst) + 1;
        self.stats.each[self.index].fetch_add(1, Ordering::SeqCst);
        let thread_ok = self.thread == std::thread::current().id();
        self.stats.thread_ok.fetch_add(usize::from(thread_ok), Ordering::SeqCst);
        eprintln!("PROBE_DROP n={n} index={} thread_ok={} panicking={} local_refs={}", self.index,
            usize::from(thread_ok), usize::from(std::thread::panicking()), Rc::strong_count(&self.local));
        match &self.fault {
            Fault::First if n == 1 => panic!("owner-first-cleanup"),
            Fault::Every => { if n == 1 { panic!("owner-first-cleanup"); } else { panic!("owner-second-cleanup"); } }
            Fault::Identity(token, drops) if n == 1 => {
                // 用户原值（Future）内部先取得实际装箱载荷（boxed panic payload）的身份，再原样展开（resume_unwind）。
                // 这里只确定注入错误的身份，不捕获或修正生产清理路径；钩子（hook）仍仅执行一次。
                let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    std::panic::panic_any(IdentityPayload { token: token.clone(), drops: drops.clone() });
                })).unwrap_err();
                let identity = payload.downcast_ref::<IdentityPayload>().unwrap();
                self.stats.payload_address.store(std::ptr::from_ref(identity) as usize, Ordering::SeqCst);
                std::panic::resume_unwind(payload);
            }
            _ => {}
        }
    }
}

/// 通过公开本地创建及单次运行（run）建立两个真正挂起（Pending）的原值。
fn suspended(owner: &TaskThread, stats: &Arc<Probes>, fault: Fault) -> Vec<ControlHandle> {
    let mut controls = Vec::new();
    for index in 0..2 {
        let parts = create_local(owner, LocalProbe { stats: stats.clone(), index,
            thread: std::thread::current().id(), local: Rc::new(()), fault: fault.clone() }, NoSchedule, (), TaskOptions::plain())
            .unwrap_or_else(|_| panic!("creation-failed"));
        assert!(matches!(parts.runnable.run(owner), Ok(RunOutcome::Pending)));
        drop(parts.join);
        controls.push(parts.control);
    }
    assert_eq!(owner.live_tasks(), 2);
    controls
}

/// 快照（snapshot）只检查公开事实，不读失效原值或内部计数。
fn report_probes(stats: &Probes, controls: &[ControlHandle], hooks: &Hooks) {
    let unconfirmed = controls.iter().filter(|c| c.snapshot().execution == ExecutionStatus::CleanupUnconfirmed).count();
    let idle = controls.iter().filter(|c| c.snapshot().execution == ExecutionStatus::Idle).count();
    let confirmed = controls.iter().filter(|c| c.snapshot().future_drop == FutureDropStatus::Confirmed).count();
    let failed_phase = controls.iter().filter(|c| c.snapshot().execution == ExecutionStatus::CleanupUnconfirmed)
        .all(|c| c.snapshot().phase == TaskPhase::Exiting && c.snapshot().future_drop == FutureDropStatus::Unconfirmed);
    println!("PROBES drops={} polls={} each0={} each1={} thread_ok={} hooks={} unconfirmed={unconfirmed} idle={idle} confirmed={confirmed} failed_phase={}",
        stats.drops.load(Ordering::SeqCst), stats.polls.load(Ordering::SeqCst), stats.each[0].load(Ordering::SeqCst), stats.each[1].load(Ordering::SeqCst),
        stats.thread_ok.load(Ordering::SeqCst), hooks.calls.load(Ordering::SeqCst), usize::from(failed_phase));
}

/// @test T76/T94/T111/T123；@api TaskThread::drop/ControlHandle::snapshot；@impl thread_drop_inner。
/// @design-doc 2026-11-09-owner-drop.1；首错后原线程继续，原载荷仅转交一次且未提前析构。
#[test]
fn test_owner_drop_keeps_first_payload_identity_and_cleans_remaining() {
    if child_case("identity") {
        let hooks = install_hook();
        let stats = Arc::new(Probes::default());
        let token = Arc::new(());
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let owner = TaskThread::new();
        let controls = suspended(&owner, &stats, Fault::Identity(token.clone(), payload_drops.clone()));
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner))).unwrap_err();
        let payload = caught.downcast::<IdentityPayload>().unwrap_or_else(|_| panic!("payload-replaced"));
        let same = hooks.identity.load(Ordering::SeqCst) == Arc::as_ptr(&payload.token) as usize
            && Arc::ptr_eq(&payload.token, &token)
            && stats.payload_address.load(Ordering::SeqCst) == std::ptr::from_ref(payload.as_ref()) as usize;
        let before = payload_drops.load(Ordering::SeqCst);
        drop(payload);
        println!("PAYLOAD same={} before={before} after={}", usize::from(same), payload_drops.load(Ordering::SeqCst));
        report_probes(&stats, &controls, &hooks);
        finish();
    }
    let output = run_case("test_owner_drop_keeps_first_payload_identity_and_cleans_remaining", "identity");
    assert!(output.status.success());
    assert_eq!((fact(&output, "same="), fact(&output, "before="), fact(&output, "after=")), (1, 0, 1));
    assert_eq!((fact(&output, "drops="), fact(&output, "hooks="), fact(&output, "thread_ok=")), (2, 1, 2));
    assert_eq!((fact(&output, "each0="), fact(&output, "each1="), fact(&output, "unconfirmed="), fact(&output, "idle="), fact(&output, "failed_phase=")), (1, 1, 1, 0, 1));
    assert_eq!((fact(&output, "confirmed="), fact(&output, "polls=")), (1, 2));
}

/// @test T75/T76/T123；@api TaskThread::close/TaskThread::drop；@impl thread_close_inner/thread_drop_inner。
/// @design-doc 2026-11-09-owner-drop.1；显式关闭首错立即传播，最终析构只处理剩余原值。
#[test]
fn test_explicit_close_failure_is_not_retried_by_owner_drop() {
    if child_case("close-failed") {
        let hooks = install_hook();
        let stats = Arc::new(Probes::default());
        let owner = TaskThread::new();
        let controls = suspended(&owner, &stats, Fault::First);
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| owner.close())).unwrap_err();
        assert_eq!(caught.downcast_ref::<&str>(), Some(&"owner-first-cleanup"));
        println!("CLOSE immediate={} closed={}", stats.drops.load(Ordering::SeqCst), usize::from(owner.is_closed()));
        let dropped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner)));
        println!("CLOSE drop_ok={}", usize::from(dropped.is_ok()));
        for c in &controls { c.wake(); c.cancel(); }
        report_probes(&stats, &controls, &hooks);
        finish();
    }
    let output = run_case("test_explicit_close_failure_is_not_retried_by_owner_drop", "close-failed");
    assert!(output.status.success());
    assert_eq!((fact(&output, "immediate="), fact(&output, "closed="), fact(&output, "drop_ok=")), (1, 1, 1));
    assert_eq!((fact(&output, "drops="), fact(&output, "each0="), fact(&output, "each1="), fact(&output, "hooks="), fact(&output, "idle=")), (2, 1, 1, 1, 0));
    assert_eq!((fact(&output, "unconfirmed="), fact(&output, "confirmed="), fact(&output, "failed_phase="), fact(&output, "thread_ok=")), (1, 1, 1, 2));
    assert_eq!(fact(&output, "polls="), 2);
}

/// 两次错误发生于两个独立析构调用（drop calls），不是字段展开期间再次恐慌。
fn independent_errors() -> ! {
    let hooks = install_hook();
    let stats = Arc::new(Probes::default());
    let owner = TaskThread::new();
    let controls = suspended(&owner, &stats, Fault::Every);
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner)));
    println!("UNEXPECTED_RETURN caught={}", usize::from(caught.is_err()));
    report_probes(&stats, &controls, &hooks);
    finish();
}

/// @test T76；@api TaskThread::drop；@impl thread_drop_inner；@design-doc 2026-11-09-owner-drop.1。
/// 首错暂存后独立二错须中止（abort）；第二次清理应从正常控制流而非展开守卫发起。
#[test]
fn test_owner_drop_aborts_on_second_independent_cleanup_error() {
    if child_case("second-error") { independent_errors(); }
    let output = run_case("test_owner_drop_aborts_on_second_independent_cleanup_error", "second-error");
    assert_aborted(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("owner-first-cleanup") && stderr.contains("owner-second-cleanup"));
    assert!(stderr.contains("OWNER_HOOK n=2") && !stderr.contains("OWNER_HOOK n=3"));
    assert!(stderr.lines().any(|line| line.contains("PROBE_DROP n=2") && line.contains("panicking=0")));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("UNEXPECTED_RETURN"));
}

/// 通过编译器（compiler）正常展开让所属域进入析构，不手工伪造展开标记。
fn outer_unwind(fail: bool) -> ! {
    let hooks = install_hook();
    let stats = Arc::new(Probes::default());
    let owner = TaskThread::new();
    let controls = suspended(&owner, &stats, if fail { Fault::First } else { Fault::None });
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _owner = owner;
        panic!("outer-original");
    }));
    assert_eq!(caught.unwrap_err().downcast_ref::<&str>(), Some(&"outer-original"));
    println!("OUTER_RETURNED original=1");
    report_probes(&stats, &controls, &hooks);
    finish();
}

/// @test T76；@api TaskThread::drop；@impl thread_drop_inner；@design-doc 2026-11-09-owner-drop.1。
/// 已有外层展开（outer unwinding）且无新错时仍完成全部清理，并原样延续外层异常。
#[test]
fn test_owner_drop_during_outer_unwind_without_new_error_cleans_all() {
    if child_case("outer-normal") { outer_unwind(false); }
    let output = run_case("test_owner_drop_during_outer_unwind_without_new_error_cleans_all", "outer-normal");
    assert!(output.status.success());
    assert_eq!((fact(&output, "original="), fact(&output, "drops="), fact(&output, "hooks="), fact(&output, "confirmed="), fact(&output, "thread_ok=")), (1, 2, 1, 2, 2));
    assert_eq!((fact(&output, "each0="), fact(&output, "each1="), fact(&output, "idle="), fact(&output, "polls=")), (1, 1, 0, 2));
}

/// @test T76；@api TaskThread::drop；@impl thread_drop_inner；@design-doc 2026-11-09-owner-drop.1。
/// 已有外层展开（outer unwinding）时新错按策略中止；不将此结果解释为所有Rust捕获均不可能。
#[test]
fn test_owner_drop_during_outer_unwind_aborts_on_new_error() {
    if child_case("outer-error") { outer_unwind(true); }
    let output = run_case("test_owner_drop_during_outer_unwind_aborts_on_new_error", "outer-error");
    assert_aborted(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("outer-original") && stderr.contains("owner-first-cleanup"));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("OUTER_RETURNED"));
}

/// 外部唤醒（wake）分别记录尝试及正常返回；首错按真实回调顺序注入。
struct WakeProbe { attempts: Arc<AtomicUsize>, delivered: Arc<AtomicUsize>, panic_first: bool }
impl Wake for WakeProbe {
    fn wake(self: Arc<Self>) { self.wake_by_ref(); }
    fn wake_by_ref(self: &Arc<Self>) {
        let n = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        eprintln!("WAKE_ATTEMPT n={n} panicking={}", usize::from(std::thread::panicking()));
        if self.panic_first && n == 1 { panic!("notification-first"); }
        self.delivered.fetch_add(1, Ordering::SeqCst);
    }
}
/// 建立标准唤醒器（Waker）及彼此独立的尝试／正常送达计数。
fn wake_probe(panic_first: bool) -> (Waker, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let attempts = Arc::new(AtomicUsize::new(0));
    let delivered = Arc::new(AtomicUsize::new(0));
    (Waker::from(Arc::new(WakeProbe { attempts: attempts.clone(), delivered: delivered.clone(), panic_first })), attempts, delivered)
}

/// 清理失败前已返回挂起（Pending）的等待者，必须由生产完成方通知，而非测试主动补通知。
fn preregistered(cancel: bool, fail_wake: bool) -> ! {
    let hooks = install_hook();
    let owner = TaskThread::new();
    let stats = Arc::new(Probes::default());
    let mut parts = create_local(&owner, LocalProbe { stats: stats.clone(), index: 0,
        thread: std::thread::current().id(), local: Rc::new(()), fault: Fault::First }, NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("creation-failed"));
    let source = external();
    let mut wait = parts.control.cancel_and_wait_from(&source);
    let (waker, wakes, delivered) = wake_probe(fail_wake);
    let mut cx = Context::from_waker(&waker);
    if cancel { assert!(Pin::new(&mut wait).poll(&mut cx).is_pending()); }
    else { assert!(Pin::new(&mut parts.join).poll(&mut cx).is_pending()); }
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner))).unwrap_err();
    assert_eq!(caught.downcast_ref::<&str>(), Some(&"owner-first-cleanup"));
    if fail_wake {
        println!("OWNER_DROP_RETURNED_WITHOUT_SECOND_ERROR");
        finish();
    }
    let before = wakes.load(Ordering::SeqCst);
    let delivered_before = delivered.load(Ordering::SeqCst);
    let ready = if cancel {
        matches!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(CancellationOutcome::Failed(CancellationError::CleanupUnconfirmed)))
    } else { matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Err(TaskError::CleanupUnconfirmed))) };
    let repeated = if cancel {
        matches!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(CancellationOutcome::Failed(CancellationError::CleanupUnconfirmed)))
    } else { matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Err(TaskError::AlreadyTaken))) };
    println!("WAIT before={before} delivered={} ready={} repeated={} drops={} hooks={} failed_phase={}",
        delivered_before, usize::from(ready), usize::from(repeated), stats.drops.load(Ordering::SeqCst),
        hooks.calls.load(Ordering::SeqCst), usize::from(parts.control.snapshot().phase == TaskPhase::Exiting));
    finish();
}

/// @test T76/T86/T59；@api JoinHandle::poll/TaskThread::drop；@impl thread_drop_inner/ref_register_result_inner。
/// @design-doc 2026-11-09-owner-drop.1；稳定预登记（registration）覆盖空闲槽由完成者接管的公共效果。
#[test]
fn test_owner_drop_cleanup_failure_wakes_preregistered_result_waiter() {
    if child_case("result-pending") { preregistered(false, false); }
    let output = run_case("test_owner_drop_cleanup_failure_wakes_preregistered_result_waiter", "result-pending");
    assert!(output.status.success());
    assert_eq!((fact(&output, "before="), fact(&output, "delivered="), fact(&output, "ready="), fact(&output, "repeated=")), (1, 1, 1, 1));
    assert_eq!((fact(&output, "drops="), fact(&output, "hooks="), fact(&output, "failed_phase=")), (1, 1, 1));
}

/// @test T76/T89/T54；@api CancelWait::poll/TaskThread::drop；@impl thread_drop_inner/wait_complete_inner。
/// @design-doc 2026-11-09-owner-drop.1；取消等待（cancellation wait）先于失败挂起，正常获得失败通知。
#[test]
fn test_owner_drop_cleanup_failure_wakes_preregistered_cancel_waiter() {
    if child_case("cancel-pending") { preregistered(true, false); }
    let output = run_case("test_owner_drop_cleanup_failure_wakes_preregistered_cancel_waiter", "cancel-pending");
    assert!(output.status.success());
    assert_eq!((fact(&output, "before="), fact(&output, "delivered="), fact(&output, "ready="), fact(&output, "repeated=")), (1, 1, 1, 1));
    assert_eq!((fact(&output, "drops="), fact(&output, "hooks=")), (1, 1));
}

/// @test T76/T86/T59/T91；@api JoinHandle::poll/CancelWait::poll；@impl join_poll_inner/ref_register_result_inner。
/// @design-doc 2026-11-09-owner-drop.1；失败早于登记（registration）时直接返回，不制造普通输出。
#[test]
fn test_cleanup_failure_before_registration_returns_existing_errors() {
    if child_case("completed-first") {
        let hooks = install_hook();
        let owner = TaskThread::new();
        let stats = Arc::new(Probes::default());
        let mut parts = create_local(&owner, LocalProbe { stats: stats.clone(), index: 0,
            thread: std::thread::current().id(), local: Rc::new(()), fault: Fault::First }, NoSchedule, (), TaskOptions::plain())
            .unwrap_or_else(|_| panic!("creation-failed"));
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner))).unwrap_err();
        assert_eq!(caught.downcast_ref::<&str>(), Some(&"owner-first-cleanup"));
        let (waker, wakes, _) = wake_probe(false);
        let mut cx = Context::from_waker(&waker);
        let result = matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Err(TaskError::CleanupUnconfirmed)));
        let mut wait = parts.control.cancel_and_wait_from(&external());
        let cancel = matches!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(CancellationOutcome::Failed(CancellationError::CleanupUnconfirmed)));
        println!("AFTER_FAILURE result={} cancel={} wakes={} hooks={} drops={}", usize::from(result), usize::from(cancel), wakes.load(Ordering::SeqCst), hooks.calls.load(Ordering::SeqCst), stats.drops.load(Ordering::SeqCst));
        finish();
    }
    let output = run_case("test_cleanup_failure_before_registration_returns_existing_errors", "completed-first");
    assert!(output.status.success());
    assert_eq!((fact(&output, "result="), fact(&output, "cancel="), fact(&output, "wakes="), fact(&output, "hooks="), fact(&output, "drops=")), (1, 1, 0, 1, 1));
}

/// 同一进程内的确定性交错（interleaving）：只在公共Waker克隆或替换旧Waker时请原线程析构所属域。
struct WaitGate {
    request: mpsc::SyncSender<()>, done: parking_lot::Mutex<mpsc::Receiver<()>>,
    on_drop: bool, panic_on_clone: bool, armed: AtomicBool, fired: AtomicUsize, wakes: AtomicUsize,
}
impl WaitGate {
    /// 单次触发并等待原线程（owner thread）处理完成；截止失败只表示夹具（fixture）未就绪。
    fn fire(&self) {
        if !self.armed.swap(false, Ordering::SeqCst) { return; }
        self.fired.fetch_add(1, Ordering::SeqCst);
        self.request.send(()).unwrap();
        self.done.lock().recv_timeout(Duration::from_secs(4)).expect("gate-timeout");
    }
}

/// SAFETY: data始终是Arc<WaitGate>的拥有引用；clone增加一次，wake/drop各归还一次，借用入口不减引用。
unsafe fn gate_clone(data: *const ()) -> RawWaker {
    let state = ManuallyDrop::new(unsafe { Arc::<WaitGate>::from_raw(data.cast()) });
    if !state.on_drop { state.fire(); }
    if state.panic_on_clone { panic!("registration-clone"); }
    RawWaker::new(Arc::into_raw(Arc::clone(&state)).cast(), &GATE_VTABLE)
}
/// 消耗唤醒（wake）拥有的Arc只归还一次；回调可展开，不在此处恢复生产事实。
unsafe fn gate_wake(data: *const ()) {
    // SAFETY: vtable调用保证本次拥有一个仍存活的Arc引用。
    let state = unsafe { Arc::<WaitGate>::from_raw(data.cast()) };
    state.wakes.fetch_add(1, Ordering::SeqCst);
}
/// 借用唤醒（wake_by_ref）仅记录调用；不消费原Arc持有。
unsafe fn gate_wake_by_ref(data: *const ()) {
    // SAFETY: 借用唤醒（wake_by_ref）不消费vtable保证的原引用。
    let state = ManuallyDrop::new(unsafe { Arc::<WaitGate>::from_raw(data.cast()) });
    state.wakes.fetch_add(1, Ordering::SeqCst);
}
/// SAFETY: 释放（drop）只消费本次原始Arc持有，暂停期间不持生产权限。
unsafe fn gate_drop(data: *const ()) {
    let state = unsafe { Arc::<WaitGate>::from_raw(data.cast()) };
    if state.on_drop { state.fire(); }
}
static GATE_VTABLE: RawWakerVTable = RawWakerVTable::new(gate_clone, gate_wake, gate_wake_by_ref, gate_drop);
/// 原始唤醒器（RawWaker）仅引用测试数据，创建时不触发登记暂停。
fn gate_waker(state: &Arc<WaitGate>) -> Waker {
    // SAFETY: 公开Waker持有一份Arc；所有数据满足Send+Sync且不引用所属域本地值。
    unsafe { Waker::from_raw(RawWaker::new(Arc::into_raw(state.clone()).cast(), &GATE_VTABLE)) }
}

/// 可跨线程创建（Send creation）的原值仍由绑定后的原线程清理。
struct SendFailure { drops: Arc<AtomicUsize>, thread: Arc<parking_lot::Mutex<Option<std::thread::ThreadId>>>, thread_ok: Arc<AtomicBool> }
impl Future for SendFailure { type Output = usize; fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<usize> { Poll::Pending } }
impl Drop for SendFailure {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        self.thread_ok.store(*self.thread.lock() == Some(std::thread::current().id()), Ordering::SeqCst);
        panic!("wait-future-drop");
    }
}

/// 只控制合法公共调用边界，不读取或改写内部槽；clone窗口早于槽写权限，drop窗口在权限已退出后。
fn registration_race(cancel: bool, on_drop: bool, callback_panic: bool) -> ! {
    let hooks = install_hook();
    let drops = Arc::new(AtomicUsize::new(0));
    let thread = Arc::new(parking_lot::Mutex::new(None));
    let thread_ok = Arc::new(AtomicBool::new(false));
    let delivery = Delivery::default();
    let mut parts = create(SendFailure { drops: drops.clone(), thread: thread.clone(), thread_ok: thread_ok.clone() },
        delivery.clone(), (), TaskOptions::plain()).unwrap_or_else(|_| panic!("creation-failed"));
    let (request, incoming) = mpsc::sync_channel(1);
    let (done_tx, done) = mpsc::channel();
    let (ready_tx, ready) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let owner = TaskThread::new();
        *thread.lock() = Some(std::thread::current().id());
        assert!(matches!(parts.runnable.run(&owner), Ok(RunOutcome::Pending)));
        ready_tx.send(()).unwrap();
        incoming.recv_timeout(Duration::from_secs(4)).expect("gate-timeout");
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner))).unwrap_err();
        assert_eq!(caught.downcast_ref::<&str>(), Some(&"wait-future-drop"));
        done_tx.send(()).unwrap();
    });
    ready.recv_timeout(Duration::from_secs(4)).expect("gate-timeout");
    let gate = Arc::new(WaitGate { request, done: parking_lot::Mutex::new(done), on_drop, panic_on_clone: callback_panic,
        armed: AtomicBool::new(!on_drop), fired: AtomicUsize::new(0), wakes: AtomicUsize::new(0) });
    let waker = gate_waker(&gate);
    let mut cx = Context::from_waker(&waker);
    let source = external();
    let mut wait = parts.control.cancel_and_wait_from(&source);
    if on_drop {
        assert!(Pin::new(&mut parts.join).poll(&mut cx).is_pending());
        gate.armed.store(true, Ordering::SeqCst);
        cx = Context::from_waker(Waker::noop());
    }
    let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if cancel {
            matches!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(CancellationOutcome::Failed(CancellationError::CleanupUnconfirmed)))
        } else { matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Err(TaskError::CleanupUnconfirmed))) }
    }));
    let (first_ready, callback_caught) = match polled {
        Ok(ready) => (ready, false),
        Err(payload) => {
            assert!(callback_panic, "unexpected-callback-panic");
            assert_eq!(payload.downcast_ref::<&str>(), Some(&"registration-clone"));
            (false, true)
        }
    };
    worker.join().unwrap();
    println!("RACE ready={} callback_caught={} fired={} wakes={} hooks={} drops={} thread_ok={}", usize::from(first_ready), usize::from(callback_caught), gate.fired.load(Ordering::SeqCst), gate.wakes.load(Ordering::SeqCst), hooks.calls.load(Ordering::SeqCst), drops.load(Ordering::SeqCst), usize::from(thread_ok.load(Ordering::SeqCst)));
    finish();
}

/// @test T76/T86/T59；@api JoinHandle::poll/Waker::clone；@impl ref_register_result_inner/join_poll_inner。
/// @design-doc 2026-11-09-owner-drop.1；登记中的克隆回调（clone callback）返回前失败，当前poll必须复核。
#[test]
fn test_result_registration_rechecks_failure_during_waker_clone() {
    if child_case("result-clone") { registration_race(false, false, false); }
    let output = run_case("test_result_registration_rechecks_failure_during_waker_clone", "result-clone");
    assert!(output.status.success());
    assert_eq!((fact(&output, "ready="), fact(&output, "fired="), fact(&output, "drops="), fact(&output, "thread_ok=")), (1, 1, 1, 1));
    assert_eq!(fact(&output, "hooks="), 1);
}

/// @test T86/T59；@api JoinHandle::poll/Waker::drop；@impl ref_register_result_inner/join_poll_inner。
/// @design-doc 2026-11-09-owner-drop.1；替换旧Waker后的失败必须在最终挂起（Pending）返回前重查。
#[test]
fn test_result_poll_rechecks_failure_after_old_waker_drop() {
    if child_case("result-final") { registration_race(false, true, false); }
    let output = run_case("test_result_poll_rechecks_failure_after_old_waker_drop", "result-final");
    assert!(output.status.success());
    assert_eq!((fact(&output, "ready="), fact(&output, "fired="), fact(&output, "drops="), fact(&output, "thread_ok=")), (1, 1, 1, 1));
}

/// @test T76/T89/T54；@api CancelWait::poll/Waker::clone；@impl thread_drop_inner/wait_complete_inner。
/// @design-doc 2026-11-09-owner-drop.1；取消等待的结果复核与Waker登记交错不丢失已发布失败。
#[test]
fn test_cancel_registration_rechecks_failure_during_waker_clone() {
    if child_case("cancel-clone") { registration_race(true, false, false); }
    let output = run_case("test_cancel_registration_rechecks_failure_during_waker_clone", "cancel-clone");
    assert!(output.status.success());
    assert_eq!((fact(&output, "ready="), fact(&output, "fired="), fact(&output, "drops="), fact(&output, "thread_ok=")), (1, 1, 1, 1));
}

/// 首次通知错误发生于先完成正常清理的那个原值；验证余下原值继续，保留独立就绪（Ready）业务结果。
fn notification_first(cancel: bool) -> ! {
    let hooks = install_hook();
    let owner = TaskThread::new();
    let stats = Arc::new(Probes::default());
    let (waker, attempts, delivered) = wake_probe(true);
    let mut cx = Context::from_waker(&waker);
    let mut ready = create_local(&owner, std::future::ready(7_usize), NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("creation-failed"));
    assert!(matches!(ready.runnable.run(&owner), Ok(RunOutcome::Finished(TerminationKind::Completed))));
    let mut joins = Vec::new();
    let mut controls = Vec::new();
    let mut waits = Vec::new();
    let mut runnables = Vec::new();
    let source = external();
    for index in 0..2 {
        let mut parts = create_local(&owner, LocalProbe { stats: stats.clone(), index,
            thread: std::thread::current().id(), local: Rc::new(()), fault: Fault::None }, NoSchedule, (), TaskOptions::plain())
            .unwrap_or_else(|_| panic!("creation-failed"));
        if cancel {
            let mut wait = parts.control.cancel_and_wait_from(&source);
            assert!(Pin::new(&mut wait).poll(&mut cx).is_pending());
            waits.push(wait);
        } else { assert!(Pin::new(&mut parts.join).poll(&mut cx).is_pending()); }
        runnables.push(parts.runnable);
        controls.push(parts.control);
        joins.push(parts.join);
    }
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner))).unwrap_err();
    assert_eq!(caught.downcast_ref::<&str>(), Some(&"notification-first"));
    let attempts_before = attempts.load(Ordering::SeqCst);
    let delivered_before = delivered.load(Ordering::SeqCst);
    let confirmed = controls.iter().filter(|c| c.snapshot().future_drop == FutureDropStatus::Confirmed).count();
    let results = joins.iter_mut().map(|join| usize::from(matches!(Pin::new(join).poll(&mut cx), Poll::Ready(Err(TaskError::Cancelled))))).sum::<usize>();
    let cancellations = waits.iter_mut().map(|wait| usize::from(matches!(Pin::new(wait).poll(&mut cx), Poll::Ready(CancellationOutcome::Cancelled)))).sum::<usize>();
    let business = matches!(Pin::new(&mut ready.join).poll(&mut cx), Poll::Ready(Ok(7)));
    println!("NOTIFY attempts={} delivered={} confirmed={confirmed} results={results} cancellations={cancellations} business={} drops={} hooks={}", attempts_before, delivered_before, usize::from(business), stats.drops.load(Ordering::SeqCst), hooks.calls.load(Ordering::SeqCst));
    finish();
}

/// @test T76/T86；@api TaskThread::drop/JoinHandle::poll/Waker::wake；@impl thread_drop_inner。
/// @design-doc 2026-11-09-owner-drop.1；通知首错后确认事实不回退，后继原值仍清理，不虚报首回调送达。
#[test]
fn test_result_notification_first_panic_preserves_facts_and_remaining_cleanup() {
    if child_case("result-notify") { notification_first(false); }
    let output = run_case("test_result_notification_first_panic_preserves_facts_and_remaining_cleanup", "result-notify");
    assert!(output.status.success());
    assert_eq!((fact(&output, "attempts="), fact(&output, "delivered="), fact(&output, "confirmed="), fact(&output, "results="), fact(&output, "business="), fact(&output, "drops="), fact(&output, "hooks=")), (2, 1, 2, 2, 1, 2, 1));
}

/// @test T76/T54；@api CancelWait::poll/TaskThread::drop/Waker::wake；@impl wait_complete_inner/thread_drop_inner。
/// @design-doc 2026-11-09-owner-drop.1；取消通知首错不回退原值确认或妨碍其它原值。
#[test]
fn test_cancel_notification_first_panic_preserves_remaining_cleanup() {
    if child_case("cancel-notify") { notification_first(true); }
    let output = run_case("test_cancel_notification_first_panic_preserves_remaining_cleanup", "cancel-notify");
    assert!(output.status.success());
    assert_eq!((fact(&output, "attempts="), fact(&output, "delivered="), fact(&output, "confirmed="), fact(&output, "drops="), fact(&output, "hooks="), fact(&output, "cancellations=")), (2, 1, 2, 2, 1, 2));
    assert_eq!((fact(&output, "results="), fact(&output, "business=")), (2, 1));
}

/// @test T76/T86/T111；@api TaskThread::drop/JoinHandle::poll/Waker::wake；@impl thread_drop_inner。
/// @design-doc 2026-11-09-owner-drop.1；原值首错后结果通知再错属于独立二错，不能恢复返回。
#[test]
fn test_cleanup_panic_then_result_notification_panic_aborts() {
    if child_case("result-second") { preregistered(false, true); }
    let output = run_case("test_cleanup_panic_then_result_notification_panic_aborts", "result-second");
    assert_aborted(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("owner-first-cleanup") && stderr.contains("notification-first"));
    assert!(stderr.contains("OWNER_HOOK n=2"));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("OWNER_DROP_RETURNED_WITHOUT_SECOND_ERROR"));
}

/// @test T76/T54/T111；@api TaskThread::drop/CancelWait::poll/Waker::wake；@impl thread_drop_inner。
/// @design-doc 2026-11-09-owner-drop.1；原值首错后的取消通知再错同样中止（abort）。
#[test]
fn test_cleanup_panic_then_cancel_notification_panic_aborts() {
    if child_case("cancel-second") { preregistered(true, true); }
    let output = run_case("test_cleanup_panic_then_cancel_notification_panic_aborts", "cancel-second");
    assert_aborted(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("owner-first-cleanup") && stderr.contains("notification-first"));
    assert!(stderr.contains("OWNER_HOOK n=2"));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("OWNER_DROP_RETURNED_WITHOUT_SECOND_ERROR"));
}

/// @test T76/T86；@api JoinHandle::poll/Waker::clone；@impl ref_register_result_inner。
/// @design-doc 2026-11-09-owner-drop.1；原线程已捕获清理首错后，另一线程的公开克隆回调（clone callback）异常独立传播。
/// 本用例不要求就绪时额外唤醒（wake），也不能证明内部1→3交接；该窗口没有公共暂停点。
#[test]
fn test_result_registration_callback_panic_stays_on_registering_thread() {
    if child_case("registration-callback") { registration_race(false, false, true); }
    let output = run_case("test_result_registration_callback_panic_stays_on_registering_thread", "registration-callback");
    assert!(output.status.success());
    assert_eq!((fact(&output, "callback_caught="), fact(&output, "fired="), fact(&output, "hooks="), fact(&output, "drops=")), (1, 1, 2, 1));
}

/// 完成回调中仅通过公开入口建立另一份等待；不读取内部登记表（registry）。
struct ReentrantWake { other: ControlHandle, source: ExternalWaiter, calls: Arc<AtomicUsize>, registered: Arc<AtomicUsize> }
impl Wake for ReentrantWake {
    fn wake(self: Arc<Self>) { self.wake_by_ref(); }
    fn wake_by_ref(self: &Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut next = self.other.cancel_and_wait_from(&self.source);
        let mut cx = Context::from_waker(Waker::noop());
        if Pin::new(&mut next).poll(&mut cx).is_pending() { self.registered.fetch_add(1, Ordering::SeqCst); }
    }
}

/// @test T76/T54/T86；@api CancelWait::poll/JoinHandle::poll/Waker::wake；@impl thread_drop_inner/wait_complete_inner。
/// @design-doc 2026-11-09-owner-drop.1；两个预登记等待均在再次轮询（poll）之前获通知，通知中可合法建立其它等待。
/// 重入（reentrancy）验证通知可再次正常登记；旧键（key）和图边（edge）的删除时点由各自交接判据核对。
#[test]
fn test_failure_notifications_release_permissions_before_reentrant_waits() {
    if child_case("reentrant-waits") {
        let hooks = install_hook();
        let stats = Arc::new(Probes::default());
        let other = create(std::future::pending::<usize>(), NoSchedule, (), TaskOptions::plain())
            .unwrap_or_else(|_| panic!("creation-failed"));
        let owner = TaskThread::new();
        let mut parts = create_local(&owner, LocalProbe { stats: stats.clone(), index: 0,
            thread: std::thread::current().id(), local: Rc::new(()), fault: Fault::First }, NoSchedule, (), TaskOptions::plain())
            .unwrap_or_else(|_| panic!("creation-failed"));
        let calls = Arc::new(AtomicUsize::new(0));
        let registered = Arc::new(AtomicUsize::new(0));
        let source = external();
        let waker = Waker::from(Arc::new(ReentrantWake { other: other.control.clone(), source: source.clone(),
            calls: calls.clone(), registered: registered.clone() }));
        let mut cx = Context::from_waker(&waker);
        let mut wait = parts.control.cancel_and_wait_from(&source);
        assert!(Pin::new(&mut parts.join).poll(&mut cx).is_pending());
        assert!(Pin::new(&mut wait).poll(&mut cx).is_pending());
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner))).unwrap_err();
        assert_eq!(caught.downcast_ref::<&str>(), Some(&"owner-first-cleanup"));
        let calls_before = calls.load(Ordering::SeqCst);
        let registrations_before = registered.load(Ordering::SeqCst);
        let result = matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Err(TaskError::CleanupUnconfirmed)));
        let cancel = matches!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(CancellationOutcome::Failed(CancellationError::CleanupUnconfirmed)));
        println!("REENTRANT calls={calls_before} registered={registrations_before} result={} cancel={} hooks={}", usize::from(result), usize::from(cancel), hooks.calls.load(Ordering::SeqCst));
        finish();
    }
    let output = run_case("test_failure_notifications_release_permissions_before_reentrant_waits", "reentrant-waits");
    assert!(output.status.success());
    assert_eq!((fact(&output, "calls="), fact(&output, "registered="), fact(&output, "result="), fact(&output, "cancel="), fact(&output, "hooks=")), (2, 2, 1, 1, 1));
}

/// 元数据（metadata）仅在真实末次存储持有释放时执行；首错按实际释放次序确定。
struct DropMetadata { calls: Arc<AtomicUsize>, every: bool }
impl Drop for DropMetadata {
    fn drop(&mut self) {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        eprintln!("METADATA_DROP n={n} panicking={}", usize::from(std::thread::panicking()));
        if n == 1 { panic!("metadata-first"); }
        if self.every { panic!("metadata-second"); }
    }
}

/// @test T76/T94/T112；@api TaskThread::drop；@impl thread_drop_inner/storage_destroy_inner。
/// @design-doc 2026-11-09-owner-drop.1；本域实际归还末持有时的首错不得跳过其它原值。
#[test]
fn test_metadata_release_first_panic_does_not_skip_remaining_futures() {
    if child_case("metadata") {
        let hooks = install_hook();
        let stats = Arc::new(Probes::default());
        let metadata = Arc::new(AtomicUsize::new(0));
        let owner = TaskThread::new();
        for index in 0..2 {
            let parts = create_local(&owner, LocalProbe { stats: stats.clone(), index,
                thread: std::thread::current().id(), local: Rc::new(()), fault: Fault::None },
                NoSchedule, DropMetadata { calls: metadata.clone(), every: false }, TaskOptions::plain()).unwrap_or_else(|_| panic!("creation-failed"));
            assert!(matches!(parts.runnable.run(&owner), Ok(RunOutcome::Pending)));
            drop(parts.join);
            drop(parts.control);
        }
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner))).unwrap_err();
        assert_eq!(caught.downcast_ref::<&str>(), Some(&"metadata-first"));
        println!("METADATA futures={} metadata={} thread_ok={} hooks={}", stats.drops.load(Ordering::SeqCst), metadata.load(Ordering::SeqCst), stats.thread_ok.load(Ordering::SeqCst), hooks.calls.load(Ordering::SeqCst));
        finish();
    }
    let output = run_case("test_metadata_release_first_panic_does_not_skip_remaining_futures", "metadata");
    assert!(output.status.success());
    assert_eq!((fact(&output, "futures="), fact(&output, "metadata="), fact(&output, "thread_ok="), fact(&output, "hooks=")), (2, 2, 2, 1));
}

/// @test T76/T94/T112；@api TaskThread::drop；@impl thread_drop_inner/storage_destroy_inner。
/// @design-doc 2026-11-09-owner-drop.1；两次独立末持有释放错误按同一二错规则中止（abort）。
#[test]
fn test_owner_drop_aborts_on_second_metadata_release_error() {
    if child_case("metadata-second") {
        let _hooks = install_hook();
        let stats = Arc::new(Probes::default());
        let metadata = Arc::new(AtomicUsize::new(0));
        let owner = TaskThread::new();
        for index in 0..2 {
            let parts = create_local(&owner, LocalProbe { stats: stats.clone(), index,
                thread: std::thread::current().id(), local: Rc::new(()), fault: Fault::None },
                NoSchedule, DropMetadata { calls: metadata.clone(), every: true }, TaskOptions::plain())
                .unwrap_or_else(|_| panic!("creation-failed"));
            assert!(matches!(parts.runnable.run(&owner), Ok(RunOutcome::Pending)));
            drop(parts.join);
            drop(parts.control);
        }
        let _caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner)));
        println!("METADATA_UNEXPECTED_RETURN");
        finish();
    }
    let output = run_case("test_owner_drop_aborts_on_second_metadata_release_error", "metadata-second");
    assert_aborted(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("metadata-first") && stderr.contains("metadata-second"));
    assert!(stderr.contains("METADATA_DROP n=2 panicking=0"));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("METADATA_UNEXPECTED_RETURN"));
}
