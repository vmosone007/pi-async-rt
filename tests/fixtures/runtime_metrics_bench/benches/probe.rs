//! MET-PERF-PROBE：仅观测真实System分配、Linux进程CPU和父子资源，不代替产品能力。
//!
//! 所有分配原样交给System；原子统计本身会干扰时间，报告不得外推无仪器极限。
//! /proc为采样而非硬资源隔离，父只拥有自己创建的Child，异常清理不延长成功截止。

use std::{alloc::{GlobalAlloc, Layout, System}, fs, process::{Child, Command},
    sync::atomic::{AtomicU64, Ordering}, thread, time::{Duration, Instant}};

pub(super) struct Allocator;
static CALLS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static FREED: AtomicU64 = AtomicU64::new(0);

// 安全边界：完整转交原指针／Layout给System，只在成功分配后做无分配原子观测。
// realloc失败不释放旧布局；成功时按新分配／旧释放计账，不改变内存所有权或对齐。
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = System.alloc(layout);
        if !pointer.is_null() { CALLS.fetch_add(1, Ordering::Relaxed); BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed); }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = System.alloc_zeroed(layout);
        if !pointer.is_null() { CALLS.fetch_add(1, Ordering::Relaxed); BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed); }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        System.dealloc(pointer, layout);
        FREED.fetch_add(layout.size() as u64, Ordering::Relaxed);
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = System.realloc(pointer, layout, size);
        if !next.is_null() {
            CALLS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(size as u64, Ordering::Relaxed);
            FREED.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        next
    }
}

/// 只读观测快照，不清零全局计数；跨并发线程的三个数字不是联合原子快照。
pub(super) fn allocations() -> [u64; 3] {
    [CALLS.load(Ordering::Relaxed), BYTES.load(Ordering::Relaxed), FREED.load(Ordering::Relaxed)]
}

/// Linux stat的comm允许空格／右括号，先取最后右括号再按官方字段位置解析。
pub(super) fn cpu_ticks() -> u64 {
    let stat = fs::read_to_string("/proc/self/stat").unwrap();
    let fields: Vec<_> = stat.rsplit_once(')').unwrap().1.split_whitespace().collect();
    fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap()
}

pub(super) fn clock_ticks() -> u64 {
    let value = Command::new("getconf").arg("CLK_TCK").output().unwrap();
    assert!(value.status.success());
    let ticks = std::str::from_utf8(&value.stdout).unwrap().trim().parse().unwrap();
    assert!(ticks > 0);
    ticks
}

/// 使用向上秩分位，打印可机器解析的完整原始汇总；排序／输出在计时及分配窗外。
pub(super) fn row(case: &str, phase: &str, mut nanos: Vec<u128>, before: [u64; 3], after: [u64; 3], cpu: u64, hz: u64) {
    assert!(!nanos.is_empty());
    let sum: u128 = nanos.iter().sum();
    nanos.sort_unstable();
    let rank = |percent: usize| nanos[(nanos.len() * percent + 99) / 100 - 1];
    let live = (after[1] as i128 - after[2] as i128) - (before[1] as i128 - before[2] as i128);
    // 并发任务延迟之和不是墙钟吞吐；该倒数另名，真实吞吐只在tasks行报告。
    let rate = if phase == "submit_to_first_poll" { "inverse_mean_latency_per_second" } else { "ops_per_second" };
    println!("{{\"type\":\"phase\",\"case\":\"{}\",\"phase\":\"{}\",\"ops\":{},\"sum_ns\":{},\"{}\":{},\"p50_ns\":{},\"p90_ns\":{},\"p99_ns\":{},\"max_ns\":{},\"allocation_calls\":{},\"allocated_bytes\":{},\"net_live_bytes\":{},\"cpu_ticks\":{},\"clock_ticks_per_second\":{}}}",
        case, phase, nanos.len(), sum, rate, nanos.len() as f64 * 1e9 / sum.max(1) as f64,
        rank(50), rank(90), rank(99), nanos[nanos.len() - 1], after[0] - before[0], after[1] - before[1], live, cpu, hz);
}

/// 缺少RSS只返回未观测，不猜测退出状态或填零；父负责以所拥有的Child确认退出。
fn rss(pid: u32) -> Option<u64> {
    let status = match fs::read_to_string(format!("/proc/{}/status", pid)) {
        Ok(status) => status,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => panic!("无法观测进程RSS：{}", error),
    };
    if let Some(value) = status.lines().find_map(|line| line.strip_prefix("VmRSS:")) {
        return Some(value.split_whitespace().next().expect("RSS字段缺值").parse().expect("RSS字段非法"));
    }
    None
}

fn available() -> u64 {
    fs::read_to_string("/proc/meminfo").unwrap().lines()
        .find_map(|line| line.strip_prefix("MemAvailable:").and_then(|line| line.split_whitespace().next()?.parse().ok())).unwrap()
}

struct OwnedChild(Child);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(Some(_))) { return; }
        if let Err(error) = self.0.kill() { eprintln!("终止基准子进程失败：{}", error); }
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            match self.0.try_wait() {
                Ok(Some(_)) => return,
                Err(error) => { eprintln!("回收基准子进程失败：{}", error); return; }
                Ok(None) => thread::sleep(Duration::from_millis(5)),
            }
        }
        eprintln!("基准子进程回收超过5秒");
    }
}

/// 严格串行父入口，场景30秒／整运行180秒；每10ms采样资源，超门即失败而非重试。
pub(super) fn parent(cases: &[String]) {
    let total = Instant::now() + Duration::from_secs(180);
    for case in cases {
        assert!(Instant::now() < total);
        assert!(available() >= 4 * 1024 * 1024, "可用内存不足4GiB，不开始基准");
        let deadline = total.min(Instant::now() + Duration::from_secs(30));
        let mut child = OwnedChild(Command::new(std::env::current_exe().unwrap()).env("PI_RT_METRICS_BENCH_CASE", case).spawn().unwrap());
        let mut peak = 0;
        let mut minimum = u64::MAX;
        let mut exit_without_rss = false;
        'observe: loop {
            assert!(Instant::now() < deadline, "基准场景超过截止：{}", case);
            let parent_memory = rss(std::process::id()).expect("父入口RSS必须可观测");
            minimum = minimum.min(available());
            assert!(parent_memory <= 1024 * 1024 && minimum >= 4 * 1024 * 1024, "基准资源门禁失败：{}", case);
            let child_memory = match rss(child.0.id()) {
                Some(memory) => memory,
                None => {
                    // 退出过渡中的status不必为Z；只凭Child确认，不用缺字段假装RSS为0。
                    // 观察预算不超过原10ms采样间隔，且绝不延长场景／整批成功截止。
                    let exit_deadline = deadline.min(Instant::now() + Duration::from_millis(10));
                    loop {
                        assert!(Instant::now() < exit_deadline, "无法确认缺RSS子进程已退出：{}", case);
                        if let Some(status) = child.0.try_wait().unwrap() {
                            assert!(Instant::now() < exit_deadline && status.success(), "基准场景失败：{}／{}", case, status);
                            exit_without_rss = true;
                            break 'observe;
                        }
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            };
            let memory = child_memory + parent_memory;
            peak = peak.max(memory);
            assert!(memory <= 1024 * 1024 && minimum >= 4 * 1024 * 1024, "基准资源门禁失败：{}", case);
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(Instant::now() < deadline && status.success(), "基准场景失败：{}／{}", case, status);
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        println!("{{\"type\":\"resource\",\"case\":\"{}\",\"sampled_parent_child_peak_rss_kb\":{},\"minimum_available_kb\":{},\"exit_rss_unavailable_confirmed\":{}}}", case, peak, minimum, exit_without_rss);
    }
}
