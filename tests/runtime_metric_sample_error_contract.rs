//! MET-T-SAMPLE-ERROR-05～07：真实错误纯值的权限、未提供能力及验证设施专项。
//!
//! 保持私有metrics::sample_error／counts层级，只包含真实源码并加入合法消费探针。
//! 不复制生产类型、不扩大权限、不补采样、截止、占位或回复功能；探针只编译元数据。
//! 测试顺序运行，不创建runtime或初始化全局时钟。截止使用独立Instant，不依赖定时轮。
//! 完整合同：docs/RUNTIME_METRICS_SAMPLE_ERROR_TEST_PLAN.md；本目标不证明上层协议。

use std::{
    fs::{self, File},
    io,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::OnceLock,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const COMPILE_LIMIT: Duration = Duration::from_secs(30);
const TARGET_LIMIT: Duration = Duration::from_secs(180);
const REAP_LIMIT: Duration = Duration::from_secs(5);

// 首个测试入口初始化一次绝对截止，后续用例不能重置整个目标的预算。
static TARGET_DEADLINE: OnceLock<Instant> = OnceLock::new();

fn target_deadline() -> io::Result<Instant> {
    let deadline = *TARGET_DEADLINE.get_or_init(|| Instant::now() + TARGET_LIMIT);
    if Instant::now() >= deadline {
        Err(io::Error::new(io::ErrorKind::TimedOut, "编译边界目标超过总截止"))
    } else {
        Ok(deadline)
    }
}

fn finish(dir: &mut Scratch) -> io::Result<()> {
    dir.cleanup()?;
    target_deadline().map(|_| ())
}

/// 排他创建的目录只保存探针、诊断和元数据，不写生产源、registry或依赖锁。
struct Scratch(PathBuf, bool);

impl Scratch {
    fn new() -> io::Result<Self> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?.as_nanos();
        for attempt in 0..16 {
            let path = std::env::temp_dir().join(format!(
                "pi-rt-sample-error-{}-{}-{}", std::process::id(), nonce, attempt,
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self(path, false)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (),
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(io::ErrorKind::AlreadyExists, "无法建立独占测试目录"))
    }

    /// 正常出口检查清理结果；展开时由Drop兜底，不能把清理失败写成成功。
    fn cleanup(&mut self) -> io::Result<()> {
        if !self.1 {
            fs::remove_dir_all(&self.0)?;
            self.1 = true;
        }
        Ok(())
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            eprintln!("清理测试目录 {} 失败：{}", self.0.display(), error);
        }
    }
}

/// 仅借用本测试创建的真实Child，不凭裸PID杀进程；正常、超时和展开均回收。
/// 同步等待只属验证设施；单次stop尝试5秒，Drop重试不保证总清理始终5秒。
struct ChildGuard<'a>(&'a mut Child);

impl ChildGuard<'_> {
    fn stop(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.0.try_wait()? {
            return Ok(status);
        }
        if let Err(error) = self.0.kill() {
            if let Some(status) = self.0.try_wait()? {
                return Ok(status);
            }
            return Err(error);
        }
        let deadline = Instant::now() + REAP_LIMIT;
        loop {
            if Instant::now() >= deadline {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "编译子进程回收超时"));
            }
            if let Some(status) = self.0.try_wait()? {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "观察回收完成时已过截止"));
                }
                return Ok(status);
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for ChildGuard<'_> {
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            eprintln!("回收测试子进程 {} 失败：{}", self.0.id(), error);
        }
    }
}

/// 截止前观察到完成才有效；到期终止、回收后返回设施错误，而非目标编译拒绝。
/// 文件I/O和OS不可回收状态不被时钟实时抢占，不能宣称绝对清理保证。
fn run(command: &mut Command, deadline: Instant) -> io::Result<ExitStatus> {
    if Instant::now() >= deadline {
        return Err(io::Error::new(io::ErrorKind::TimedOut, "编译目标截止已到"));
    }
    let mut owned = command.spawn()?;
    let mut child = ChildGuard(&mut owned);
    loop {
        if Instant::now() >= deadline {
            child.stop().map_err(|error| io::Error::new(io::ErrorKind::Other,
                format!("超时后的清理未完成：{}", error)))?;
            return Err(io::Error::new(io::ErrorKind::TimedOut, "编译超过截止"));
        }
        if let Some(status) = child.0.try_wait()? {
            if Instant::now() >= deadline {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "观察编译完成时已过截止"));
            }
            return Ok(status);
        }
        thread::sleep(Duration::from_millis(5));
    }
}

fn compiler() -> std::ffi::OsString {
    std::env::var_os("RUSTC").or_else(|| option_env!("RUSTC").map(Into::into))
        .unwrap_or_else(|| "rustc".into())
}

struct Compiled {
    status: ExitStatus,
    stderr: String,
    source: PathBuf,
}

/// 输出到独占文件而非未消费pipe，避免输出堵塞；只生成元数据，不执行探针。
fn compile(dir: &Path, name: &str, code: Option<&str>, target_deadline: Instant) -> io::Result<Compiled> {
    let source = dir.join(format!("{}.rs", name));
    if let Some(code) = code {
        fs::write(&source, code)?;
    }
    let stderr = dir.join(format!("{}.stderr", name));
    let mut command = Command::new(compiler());
    command.args(["--crate-name", "metric_sample_error_probe", "--edition=2018", "--emit=metadata",
        "--error-format=short", "--color=never"])
        .arg(&source).arg("-o").arg(dir.join(format!("{}.rmeta", name)))
        .stdin(Stdio::null()).stdout(Stdio::from(File::create(dir.join(format!("{}.stdout", name)))?))
        .stderr(Stdio::from(File::create(&stderr)?));
    let status = run(&mut command, target_deadline.min(Instant::now() + COMPILE_LIMIT))?;
    Ok(Compiled { status, stderr: fs::read_to_string(stderr)?, source })
}

/// 单故障负例必须exit1且只有目标源上的准确错误码；环境、额外错误或异常退出不通过。
/// 短格式以诊断标记筛选，不能用路径中的error字样把普通warning误判为编译错误。
fn assess(result: &Compiled, expected: Option<&str>) -> Result<(), String> {
    match expected {
        None if result.status.success() => Ok(()),
        Some(code) if result.status.code() == Some(1) => {
            let errors: Vec<_> = result.stderr.lines()
                .filter(|line| line.contains(": error") || line.starts_with("error:")).collect();
            let prefix = format!("{}:", result.source.display());
            let marker = format!(": error[{}]:", code);
            let targeted = errors.iter().filter(|line| line.starts_with(&prefix) && line.contains(&marker)).count();
            let unexpected = errors.iter().any(|line| {
                !(line.starts_with(&prefix) && line.contains(&marker))
                    && !line.starts_with("error: aborting due to ")
            });
            if targeted == 1 && !unexpected {
                Ok(())
            } else {
                Err(format!("预期单一 {} 诊断，实际：\n{}", code, result.stderr))
            }
        }
        _ => Err(format!("退出 {:?} 与预期 {:?} 不符：\n{}", result.status, expected, result.stderr)),
    }
}

/// 保持真实两个叶模块的相对层级；包装只提供消费位置，不重定义类型或方法。
fn fixture(inside: &str, outside: &str) -> io::Result<String> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let counts = fs::canonicalize(root.join("src/metrics/counts.rs"))?;
    let error = fs::canonicalize(root.join("src/metrics/sample_error.rs"))?;
    let counts = counts.to_str().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "读数源码路径非UTF-8"))?;
    let error = error.to_str().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "错误源码路径非UTF-8"))?;
    Ok(format!("mod metrics {{\n#[path = {:?}] mod counts;\n#[path = {:?}] mod sample_error;\n{}\n}}\n{}\nfn main() {{}}\n",
        counts, error, inside, outside))
}

/// MET-T-SAMPLE-ERROR-05／06／07：先合法对照，再两权限和八trait单故障拒绝。
/// trait用例都在合法命名域；附加错误、错误码或源定位不符不能被判作目标拒绝。
#[test]
fn test_real_source_visibility_and_absent_traits() -> io::Result<()> {
    let deadline = target_deadline()?;
    let mut dir = Scratch::new()?;
    println!("编译器：{:?}；真实源：{}/src/metrics/sample_error.rs", compiler(), env!("CARGO_MANIFEST_DIR"));
    let allowed = fixture("mod consumer { use super::counts::RuntimeMetricCounts;\n\
        use super::sample_error::RuntimeMetricSampleError;\n\
        const ERRORS: [RuntimeMetricSampleError; 3] = [RuntimeMetricSampleError::RequestInFlight,\n\
        RuntimeMetricSampleError::DeadlineExceeded, RuntimeMetricSampleError::ReplyClosed];\n\
        fn traits<T: Copy + Clone + PartialEq + Eq + Send + Sync + 'static>() {}\n\
        fn use_value() { traits::<RuntimeMetricSampleError>();\n\
        traits::<Result<RuntimeMetricCounts, RuntimeMetricSampleError>>();\n\
        for first in ERRORS { let copy = first; let cloned = Clone::clone(&first);\n\
        let _ = (copy == cloned, first); match first { RuntimeMetricSampleError::RequestInFlight => (),\n\
        RuntimeMetricSampleError::DeadlineExceeded => (), RuntimeMetricSampleError::ReplyClosed => () } }\n\
        let result: Result<RuntimeMetricCounts, RuntimeMetricSampleError> =\n\
        Ok(RuntimeMetricCounts::new(usize::MAX, Some(0)));\n\
        match result { Ok(c) => { let _ = (c.len(), c.wait_len()); }, Err(_) => () } } }", "")?;
    let positive = compile(&dir.0, "allowed", Some(&allowed), deadline)?;
    assess(&positive, None).unwrap();
    assert!(assess(&positive, Some("E0277")).is_err(), "成功编译不可算作拒绝");

    for (name, inside, outside, code) in [
        ("outside_error", "", "use crate::metrics::sample_error;", "E0603"),
        ("export_error", "pub(crate) use self::sample_error::RuntimeMetricSampleError;", "", "E0365"),
    ] {
        let source = fixture(inside, outside)?;
        let result = compile(&dir.0, name, Some(&source), deadline)?;
        assess(&result, Some(code)).unwrap();
        assert!(assess(&result, None).is_err());
        assert!(assess(&result, Some("E0308")).is_err(), "其它错误码不能代替目标拒绝");
        let mislocated = Compiled { status: result.status, stderr: result.stderr.clone(),
            source: dir.0.join("different_source.rs") };
        assert!(assess(&mislocated, Some(code)).is_err(), "其它源定位不能代替目标拒绝");
        let extra_error = Compiled { status: result.status,
            stderr: format!("{}\n{}:1:1: error[E0308]: 仅注入判定器的附加错误\n", result.stderr, result.source.display()),
            source: result.source.clone() };
        assert!(assess(&extra_error, Some(code)).is_err(), "附加错误不可忽略");
        println!("{}：准确拒绝 {}\n{}", name, code, result.stderr);
    }
    for (name, bound) in [
        ("no_default", "Default"), ("no_debug", "std::fmt::Debug"),
        ("no_hash", "std::hash::Hash"), ("no_ord", "Ord"),
        ("no_partial_ord", "PartialOrd"), ("no_deref", "std::ops::Deref"),
        ("no_display", "std::fmt::Display"), ("no_error", "std::error::Error"),
    ] {
        let source = fixture(&format!("mod consumer {{ use super::sample_error::RuntimeMetricSampleError;\n\
            fn require<T: {}>() {{}} fn bad() {{ require::<RuntimeMetricSampleError>(); }} }}", bound), "")?;
        let result = compile(&dir.0, name, Some(&source), deadline)?;
        assess(&result, Some("E0277")).unwrap();
        assert!(assess(&result, None).is_err());
        assert!(assess(&result, Some("E0603")).is_err(), "不可见性不能冒充trait拒绝");
        println!("{}：准确拒绝 E0277\n{}", name, result.stderr);
    }
    finish(&mut dir)
}

/// MET-T-SAMPLE-ERROR-07：真实缺输入和不存在编译器属于设施故障，不是权限或trait拒绝。
#[test]
fn test_driver_rejects_environment_failures() -> io::Result<()> {
    let deadline = target_deadline()?;
    let mut dir = Scratch::new()?;
    let missing = compile(&dir.0, "missing_input", None, deadline)?;
    assert!(assess(&missing, None).is_err());
    assert!(assess(&missing, Some("E0277")).is_err());
    let error = run(&mut Command::new(dir.0.join("missing_compiler")), deadline.min(Instant::now() + COMPILE_LIMIT))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    finish(&mut dir)
}

/// MET-T-SAMPLE-ERROR-07：已过截止不启动进程；真实rustc等stdin时超时并有界回收。
#[test]
fn test_driver_deadlines_and_cleanup() -> io::Result<()> {
    let deadline = target_deadline()?;
    let mut dir = Scratch::new()?;
    let mut expired = Command::new(dir.0.join("must_not_spawn"));
    assert_eq!(run(&mut expired, Instant::now()).unwrap_err().kind(), io::ErrorKind::TimedOut);
    let mut command = waiting_compiler(&dir.0);
    assert_eq!(run(&mut command, deadline.min(Instant::now() + Duration::from_millis(100)))
        .unwrap_err().kind(), io::ErrorKind::TimedOut);
    assert!(!dir.0.join("waiting.rmeta").exists());
    finish(&mut dir)
}

/// 仅让真实编译器等未关闭的stdin，驱动器故障注入不补任何生产能力。
fn waiting_compiler(dir: &Path) -> Command {
    let mut command = Command::new(compiler());
    command.args(["--crate-name", "waiting_error_probe", "--emit=metadata", "-"])
        .arg("-o").arg(dir.join("waiting.rmeta"))
        .stdin(Stdio::piped()).stdout(Stdio::null()).stderr(Stdio::null());
    command
}

/// MET-T-SAMPLE-ERROR-07：设施panic展开后回收真实Child，异常终止不可算作正确拒绝。
#[test]
fn test_child_cleanup_on_unwind() -> io::Result<()> {
    target_deadline()?;
    let mut dir = Scratch::new()?;
    let mut child = waiting_compiler(&dir.0).spawn()?;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = ChildGuard(&mut child);
        panic!("主动验证编译子进程展开清理");
    }));
    assert!(result.is_err());
    let status = child.try_wait()?.expect("展开后必须已回收真实子进程");
    assert!(!status.success());
    let terminated = Compiled { status, stderr: String::new(), source: dir.0.join("waiting.rs") };
    assert!(assess(&terminated, None).is_err());
    assert!(assess(&terminated, Some("E0277")).is_err());
    finish(&mut dir)
}

/// MET-T-SAMPLE-ERROR-07：仅验证设施展开清理目录，不捕获或改变生产poll／wake panic。
#[test]
fn test_scratch_cleanup_on_unwind() {
    target_deadline().unwrap();
    let mut owned_path = None;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let dir = Scratch::new().unwrap();
        owned_path = Some(dir.0.clone());
        fs::write(dir.0.join("owned"), b"fixture").unwrap();
        panic!("主动验证测试目录展开清理");
    }));
    assert!(result.is_err());
    assert!(!owned_path.unwrap().exists());
    target_deadline().unwrap();
}
