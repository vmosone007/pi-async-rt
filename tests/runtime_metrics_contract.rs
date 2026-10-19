#![cfg(not(target_arch = "wasm32"))]

//! MET-F-B03：实际本库产物的外部权限／封闭边界标准专项，不构建运行时或初始化时钟。
//!
//! 正面控制先通过，再逐项核验唯一目标诊断；环境、依赖或编译器失败不算拒绝。
//! 只生成独占临时消费源码及元数据，不复制生产实现，不改可见性或Cargo依赖。
//! 逐出口／范围／截止：docs/RUNTIME_METRICS_FINAL_VALIDATION_PLAN.md。

use std::{fs::{self, File}, io, path::{Path, PathBuf}, process::{Child, Command, ExitStatus, Stdio},
    thread, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> io::Result<Self> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        for index in 0..16 {
            let path = std::env::temp_dir().join(format!("pi-rt-metrics-contract-{}-{}-{}", std::process::id(), nonce, index));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (),
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(io::ErrorKind::AlreadyExists, "无法创建独占编译目录"))
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        if self.0.exists() {
            if let Err(error) = fs::remove_dir_all(&self.0) { eprintln!("清理编译目录失败：{}", error); }
        }
    }
}

/// 只管理自己创建的Child，成功截止前后复核；异常终止／回收最多尝试5秒。
struct OwnedChild(Child);
impl OwnedChild {
    fn wait(&mut self, deadline: Instant) -> io::Result<ExitStatus> {
        loop {
            if Instant::now() >= deadline { return Err(io::Error::new(io::ErrorKind::TimedOut, "编译探针超过截止")); }
            if let Some(status) = self.0.try_wait()? {
                if Instant::now() >= deadline { return Err(io::Error::new(io::ErrorKind::TimedOut, "过时编译退出不能通过")); }
                return Ok(status);
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(Some(_))) { return; }
        if let Err(error) = self.0.kill() { eprintln!("终止编译探针失败：{}", error); }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match self.0.try_wait() {
                Ok(Some(_)) => return,
                Err(error) => { eprintln!("回收编译探针失败：{}", error); return; }
                Ok(None) => (),
            }
            if Instant::now() >= deadline { eprintln!("编译探针回收超过5秒"); return; }
            thread::sleep(Duration::from_millis(5));
        }
    }
}

/// 消费当前profile中Cargo实际生成的本地rlib；依赖文件须指向本仓库api.rs。
/// 最新候选只选择一次，不因正面控制失败退回旧产物；公开面在四原生feature相同。
/// Cargo本包可能记录相对路径，依赖包通常记录绝对路径；相对行还须关联本目录同hash目标。
/// 这只区分可信Cargo产物，不鉴证恶意伪造文件；最终仍须真实正面编译控制。
fn library(deps: &Path) -> io::Result<PathBuf> {
    let root = fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/metrics/api.rs"))?;
    let path = root.to_str().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "编译探针要求UTF-8源码路径"))?;
    let escaped = path.replace('\\', "\\\\").replace(' ', "\\ ").replace('#', "\\#");
    let mut candidates = Vec::new();
    for entry in fs::read_dir(deps)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(hash) = name.strip_prefix("libpi_async_rt-").and_then(|name| name.strip_suffix(".rlib")) {
            let dependency = deps.join(format!("pi_async_rt-{}.d", hash));
            let header = format!("{}:", dependency.display());
            if fs::read_to_string(&dependency).map_or(false, |text| text.contains(&escaped)
                || (text.starts_with(&header) && text.lines().any(|line| line == "src/metrics/api.rs:"))) {
                candidates.push((entry.metadata()?.modified()?, entry.path()));
            }
        }
    }
    candidates.sort();
    candidates.pop().map(|(_, path)| path)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "缺少关联真实本库源码的rlib"))
}

/// 每探针只有一个故意错误；按源位置／唯一错误码判定，不能用编译失败泛指封装成立。
fn probe(dir: &Path, deps: &Path, library: &Path, name: &str, body: &str,
    expected: Option<&str>, target_deadline: Instant) -> io::Result<()>
{
    let source = dir.join(format!("{}.rs", name));
    fs::write(&source, format!("#![allow(dead_code, unused_imports)]\nuse pi_async_rt::runtime_metrics::*;\n{}\nfn main() {{}}\n", body))?;
    let diagnostics = dir.join(format!("{}.stderr", name));
    let mut command = Command::new(std::env::var_os("RUSTC")
        .or_else(|| option_env!("RUSTC").map(Into::into)).unwrap_or_else(|| "rustc".into()));
    command.args(["--crate-name", "runtime_metrics_external_probe", "--edition=2018", "--emit=metadata", "--error-format=short", "--color=never"])
        .arg("--extern").arg(format!("pi_async_rt={}", library.display()))
        .arg("-L").arg(format!("dependency={}", deps.display()))
        .arg(&source).arg("-o").arg(dir.join(format!("{}.rmeta", name)))
        .stdin(Stdio::null()).stdout(Stdio::from(File::create(dir.join(format!("{}.stdout", name)))?))
        .stderr(Stdio::from(File::create(&diagnostics)?));
    let mut child = OwnedChild(command.spawn()?);
    let status = child.wait(target_deadline.min(Instant::now() + Duration::from_secs(30)))?;
    let stderr = fs::read_to_string(diagnostics)?;
    match expected {
        None => assert!(status.success(), "合法控制必须真实编译成功：{}", stderr),
        Some(code) => {
            assert_eq!(status.code(), Some(1), "只有编译器正常拒绝才有效：{}", stderr);
            let prefix = format!("{}:", source.display());
            let marker = format!(": error[{}]:", code);
            let errors: Vec<_> = stderr.lines().filter(|line| line.contains("error")).collect();
            assert_eq!(errors.iter().filter(|line| line.starts_with(&prefix) && line.contains(&marker)).count(), 1,
                "必须是目标位置的唯一指定错误：{}", stderr);
            assert!(errors.iter().all(|line| (line.starts_with(&prefix) && line.contains(&marker))
                || line.starts_with("error: aborting due to ")), "其它错误不能代替目标拒绝：{}", stderr);
            println!("{}：准确拒绝 {}\n{}", name, code, stderr);
        }
    }
    Ok(())
}

/// 正面控制覆盖只读报告／错误／注册trait对象／!Send本地输出；12个单故障拒绝。
/// 每rustc30秒／整目标180秒，四feature公开面相同；不执行探针或初始化全局时钟。
#[test]
fn test_public_metrics_visibility_and_sealed_contract() -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(180);
    let deps = std::env::current_exe()?.parent().unwrap().to_path_buf();
    let library = library(&deps)?;
    let dir = Scratch::new()?;
    println!("实际本库产物：{}", library.display());
    probe(&dir.0, &deps, &library, "allowed", r#"
fn traits<T: Send + Sync + std::fmt::Debug>() {}
fn error<T: std::error::Error + Send + Sync>() {}
fn consume(rt: &pi_async_rt::rt::single_thread::SingleTaskRuntime<usize>) {
    let object: &dyn RuntimeMetricRegistrable = rt;
    let id = register_runtime_metrics(object, "真实实例").unwrap();
    let _ = (id.as_u64(), unregister_runtime_metrics(id), init_runtime_metrics_reporting("进程实例"));
    let _ = collect_and_report_runtime_metrics(RuntimeMetricCollectionOptions::default());
}
fn local(rt: &pi_async_rt::rt::serial_local_thread::LocalTaskRuntime<std::rc::Rc<()>>) {
    let _ = register_runtime_metrics(rt, "本地实例");
}
fn report(r: &RuntimeMetricCollectionReport) {
    traits::<RuntimeMetricId>(); traits::<RuntimeMetricCollectionReport>();
    error::<RuntimeMetricRegistrationError>(); error::<RuntimeMetricCollectionError>(); error::<RuntimeMetricReportingInitError>();
    for item in r.instances() {
        let _ = (item.id(), item.name(), item.kind(), item.status().code(), item.counts());
        let mut names = item.names(); let _ = (names.len(), names.next(), names.next_back());
    }
    let _ = (r.active_count(), r.retired_count(), r.sampled_count(), r.count(RuntimeMetricInstanceStatus::RecordInvoked));
}
"#, None, deadline)?;
    for (name, body, error) in [
        ("private_namespace", "use pi_async_rt::metrics;", "E0603"),
        ("private_source", "use pi_async_rt::runtime_metrics::RuntimeMetricsSource;", "E0432"),
        ("private_id", "fn bad(id: RuntimeMetricId) { let _ = id.0; }", "E0616"),
        ("forge_id", "fn bad() { let _ = RuntimeMetricId(1); }", "E0423"),
        ("id_default", "fn need<T: Default>() {} fn bad() { need::<RuntimeMetricId>(); }", "E0277"),
        ("private_slot", "fn bad(rt: &pi_async_rt::rt::single_thread::SingleTaskRuntime<()>) { let _ = rt.runtime_metric_slot(); }", "E0624"),
        ("private_report", "fn bad(r: &RuntimeMetricCollectionReport) { let _ = &r.instances; }", "E0616"),
        ("immutable_report", "fn bad(r: &mut RuntimeMetricCollectionReport) { r.instances().clear(); }", "E0599"),
        ("sealed_trait", "struct Outside; impl RuntimeMetricRegistrable for Outside { fn register_metrics(&self, _: &str) -> Result<RuntimeMetricId, RuntimeMetricRegistrationError> { Err(RuntimeMetricRegistrationError::InvalidName) } }", "E0277"),
        ("no_raw_facade", "fn bad(rt: &pi_async_rt::rt::LocalAsyncRuntime<()>) { let _ = register_runtime_metrics(rt, \"原始门面\"); }", "E0277"),
        ("no_compatible_wasm", "fn bad(rt: &pi_async_rt::rt::serial_local_compatible_wasm_runtime::LocalTaskRuntime<()>) { let _ = register_runtime_metrics(rt, \"兼容类型\"); }", "E0277"),
        ("local_not_sync", "fn need<T: Sync>() {} fn bad() { need::<pi_async_rt::rt::serial_local_thread::LocalTaskRuntime<()>>(); }", "E0277"),
    ] { probe(&dir.0, &deps, &library, name, body, Some(error), deadline)?; }
    fs::remove_dir_all(&dir.0)?;
    assert!(Instant::now() < deadline);
    Ok(())
}
