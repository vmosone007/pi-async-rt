//! MET-T-COUNT-05／08：真实指标纯值源码的权限与未提供能力专项。
//!
//! 临时模块只提供消费探针，保持 metrics::counts 层级；不复制类型／方法，
//! 不为生产侧开放接口或补注册、采样、任务、唤醒能力。判定器及子进程清理测试
//! 只验证测试设施，不作为运行时功能证据。所有编译串行，无运行时工作线程。

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

// 整个测试进程共用一次绝对截止，不能把每个用例重新计时当成全目标门禁。
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

/// 独占目录只保存测试输入／诊断／元数据；不写被测源或 Cargo registry。
struct Scratch(PathBuf, bool);

impl Scratch {
    fn new() -> io::Result<Self> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?.as_nanos();
        for attempt in 0..16 {
            let path = std::env::temp_dir().join(format!(
                "pi-rt-count-{}-{}-{}", std::process::id(), nonce, attempt,
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self(path, false)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (),
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(io::ErrorKind::AlreadyExists, "无法建立独占测试目录"))
    }

    /// 正常出口必须检查清理结果；Drop 只负责异常展开时的兜底，不吞正常错误。
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

/// 独占真实 Child 的可变借用，不凭已回收的裸 PID 发信号；正常／超时／展开均回收。
/// 临界等待属于同步测试设施，不参与生产 poll。回收也有界，不调用无界 wait。
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

/// 在绝对截止前观察完成才算成功；超时先终止／回收，不能误归为预期编译拒绝。
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

/// 诊断写文件而不是未消费 pipe；只生成元数据，不执行探针或其它库功能。
fn compile(dir: &Path, name: &str, code: Option<&str>, target_deadline: Instant) -> io::Result<Compiled> {
    let source = dir.join(format!("{}.rs", name));
    if let Some(code) = code {
        fs::write(&source, code)?;
    }
    let stderr = dir.join(format!("{}.stderr", name));
    let mut command = Command::new(compiler());
    command.args(["--crate-name", "metric_counts_probe", "--edition=2018", "--emit=metadata",
        "--error-format=short", "--color=never"])
        .arg(&source).arg("-o").arg(dir.join(format!("{}.rmeta", name)))
        .stdin(Stdio::null()).stdout(Stdio::from(File::create(dir.join(format!("{}.stdout", name)))?))
        .stderr(Stdio::from(File::create(&stderr)?));
    let status = run(&mut command, target_deadline.min(Instant::now() + COMPILE_LIMIT))?;
    Ok(Compiled { status, stderr: fs::read_to_string(stderr)?, source })
}

/// 每例至多一个故意错误；必须是目标源定位上的准确错误码，且无其它错误。
/// 环境失败／ICE／信号退出／成功编译不可能冒充目标 trait 或权限拒绝。
fn assess(result: &Compiled, expected: Option<&str>) -> Result<(), String> {
    match expected {
        None if result.status.success() => Ok(()),
        Some(code) if result.status.code() == Some(1) => {
            let errors: Vec<_> = result.stderr.lines().filter(|line| line.contains("error")).collect();
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

/// 直接包含真实源码，不移动层级或改变其可见性；包装只添加消费位置。
fn fixture(inside: &str, outside: &str) -> io::Result<String> {
    let path = fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/metrics/counts.rs"))?;
    let path = path.to_str().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "源码路径非 UTF-8"))?;
    Ok(format!("mod metrics {{\n#[path = {:?}] mod counts;\n{}\n}}\n{}\nfn main() {{}}\n", path, inside, outside))
}

/// MET-T-COUNT-05／08：合法同层消费及十二个单故障拒绝用例，全部引用真实类型。
/// 先验证正面控制；字段读写分别覆盖，未提供 trait 不以不可见类型代替。
#[test]
fn test_real_source_visibility_and_absent_traits() -> io::Result<()> {
    let deadline = target_deadline()?;
    let mut dir = Scratch::new()?;
    println!("编译器：{:?}；真实源：{}/src/metrics/counts.rs", compiler(), env!("CARGO_MANIFEST_DIR"));
    let allowed = fixture("mod consumer { use super::counts::RuntimeMetricCounts;\n\
        const C: RuntimeMetricCounts = RuntimeMetricCounts::new(usize::MAX, Some(0));\n\
        const L: usize = C.len(); const W: Option<usize> = C.wait_len();\n\
        fn traits<T: Copy + Clone + PartialEq + Eq + Send + Sync + 'static>() {}\n\
        fn use_value() { traits::<RuntimeMetricCounts>(); let first = C; let copy = first;\n\
        let cloned = first.clone(); let _ = (L, W, copy == cloned, first.len()); } }", "")?;
    let positive = compile(&dir.0, "allowed", Some(&allowed), deadline)?;
    assess(&positive, None).unwrap();
    assert!(assess(&positive, Some("E0616")).is_err(), "成功编译不可算作拒绝");

    for (name, inside, outside, code) in [
        ("outside_counts", "", "use crate::metrics::counts;", "E0603"),
        ("export_type", "pub(crate) use self::counts::RuntimeMetricCounts;", "", "E0365"),
        ("read_len", "mod consumer { use super::counts::RuntimeMetricCounts; fn bad() { let c = RuntimeMetricCounts::new(0, None); let _ = c.len; } }", "", "E0616"),
        ("write_len", "mod consumer { use super::counts::RuntimeMetricCounts; fn bad() { let mut c = RuntimeMetricCounts::new(0, None); c.len = 1; } }", "", "E0616"),
        ("read_wait", "mod consumer { use super::counts::RuntimeMetricCounts; fn bad() { let c = RuntimeMetricCounts::new(0, None); let _ = c.wait_len; } }", "", "E0616"),
        ("write_wait", "mod consumer { use super::counts::RuntimeMetricCounts; fn bad() { let mut c = RuntimeMetricCounts::new(0, None); c.wait_len = Some(0); } }", "", "E0616"),
    ] {
        let source = fixture(inside, outside)?;
        let result = compile(&dir.0, name, Some(&source), deadline)?;
        assess(&result, Some(code)).unwrap();
        assert!(assess(&result, None).is_err());
        assert!(assess(&result, Some("E0308")).is_err(), "其它错误码不能代替目标拒绝");
        println!("{}：准确拒绝 {}\n{}", name, code, result.stderr);
    }
    for (name, bound) in [
        ("no_default", "Default"), ("no_debug", "std::fmt::Debug"),
        ("no_hash", "std::hash::Hash"), ("no_ord", "Ord"),
        ("no_partial_ord", "PartialOrd"), ("no_deref", "std::ops::Deref"),
    ] {
        let source = fixture(&format!("mod consumer {{ use super::counts::RuntimeMetricCounts;\n\
            fn require<T: {}>() {{}} fn bad() {{ require::<RuntimeMetricCounts>(); }} }}", bound), "")?;
        let result = compile(&dir.0, name, Some(&source), deadline)?;
        assess(&result, Some("E0277")).unwrap();
        println!("{}：准确拒绝 E0277\n{}", name, result.stderr);
    }
    finish(&mut dir)
}

/// 仅验证判定器：真实 rustc 的缺失输入及不存在编译器是环境错误，不能当权限拒绝。
#[test]
fn test_driver_rejects_environment_failures() -> io::Result<()> {
    let deadline = target_deadline()?;
    let mut dir = Scratch::new()?;
    let missing = compile(&dir.0, "missing_input", None, deadline)?;
    assert!(assess(&missing, Some("E0616")).is_err());
    let error = run(&mut Command::new(dir.0.join("missing_compiler")), deadline.min(Instant::now() + COMPILE_LIMIT))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    finish(&mut dir)
}

/// 仅验证测试子进程的有界终止／回收：真实 rustc 等待尚未关闭的 stdin，不能误报编译拒绝。
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

/// 让真实编译器等待尚未关闭的标准输入，仅用于测试设施的故障注入。
fn waiting_compiler(dir: &Path) -> Command {
    let mut command = Command::new(compiler());
    command.args(["--crate-name", "waiting_probe", "--emit=metadata", "-"])
        .arg("-o").arg(dir.join("waiting.rmeta"))
        .stdin(Stdio::piped()).stdout(Stdio::null()).stderr(Stdio::null());
    command
}

/// 独立验证展开路径真的终止并回收子进程；检查原 Child 缓存状态，不查询裸 PID。
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
    assert!(child.try_wait()?.is_some(), "展开后必须已回收真实子进程");
    finish(&mut dir)
}

/// 仅验证测试设施在 panic 展开时清理独占目录，不把展开恢复扩张为运行时合同。
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
