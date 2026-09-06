//! owner 热路径的独立分配门禁，测量真实执行器/任务池，不改写生产分流。
//! 冻结矩阵 OWN-F01/F03；本测试不创建后台时钟或 worker，按单测试线程运行。
//! 只统计测试线程指定窗口的分配请求，避免进程内其它线程的测试设施分配污染证据。

use std::{alloc::{GlobalAlloc, Layout, System}, cell::Cell};
use pi_async_rt::prelude::{AsyncRuntime, AsyncTaskPool, SingleTaskRunner};

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
}

struct ObservedAllocator;

fn observe() {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        let _ = CALLS.try_with(|calls| calls.set(calls.get() + 1));
    }
}

// 安全：所有请求按原 layout/pointer 原样委托 System；TLS为无析构Cell，不分配或重入。
// 计数仅观察，绝不改变指针、容量、分配成功条件或生产行为。
unsafe impl GlobalAlloc for ObservedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        observe();
        System.alloc(layout)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        observe();
        System.alloc_zeroed(layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        observe();
        System.realloc(ptr, layout, size)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) { System.dealloc(ptr, layout); }
}

#[global_allocator]
static ALLOCATOR: ObservedAllocator = ObservedAllocator;

/// 清晰区分首次绑定/队列扩容与稳定热路径，不把整个任务创建宣称为零分配。
#[test]
fn test_warm_owner_checks_and_local_queue_operations_allocate_nothing() {
    let runner = SingleTaskRunner::<()>::default();
    let rt = runner.startup().unwrap();
    runner.run_once().unwrap();
    let pool = rt.shared_pool();
    rt.spawn(async {}).unwrap();
    let mut task = pool.try_pop().unwrap();
    for _ in 0..2 {
        pool.push_local(task).unwrap();
        task = pool.try_pop().unwrap();
        pool.push_priority(10, task).unwrap();
        task = pool.try_pop().unwrap();
    }
    CALLS.with(|calls| calls.set(0));
    TRACK.with(|track| track.set(true));
    for _ in 0..1024 {
        assert_eq!(runner.run_once().unwrap(), 0);
        pool.push_local(task).unwrap();
        task = pool.try_pop().unwrap();
        pool.push_priority(10, task).unwrap();
        task = pool.try_pop().unwrap();
    }
    TRACK.with(|track| track.set(false));
    assert_eq!(CALLS.with(Cell::get), 0, "稳定owner授权/本地入出队不应请求分配");
    assert_eq!(rt.len(), 0);
    drop(task);
}
