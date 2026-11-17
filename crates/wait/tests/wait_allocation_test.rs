//! 等待库（wait crate）的公开分配与释放计数（allocation/deallocation counts）。
//! 独立测试目标（test target）仅一个用例（test case），只计当前测试线程（test thread）的短观测窗口（observation window）。
//! 使用真实系统分配器（System allocator）、无分配原值（value）和空唤醒器（noop Waker），不替代生产分配（allocation）或清理（cleanup）。

use pi_async_wait::{wait_all, wait_any, wait_one, SendErrorKind, WaitError};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

/// 观测（observation）只过滤当前线程（thread）的计数，不添加锁（lock）或分配（allocation）。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Counts {
    allocations: usize,
    deallocations: usize,
    reallocations: usize,
}

thread_local! {
    static OBSERVING: Cell<bool> = const { Cell::new(false) };
    static COUNTS: Cell<Counts> = const { Cell::new(Counts { allocations: 0, deallocations: 0, reallocations: 0 }) };
}

/// 计数分配器（counting allocator）只转发合法系统分配操作（system allocation operations）。
struct CountingAllocator;
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// 常量初始化的线程局部计数（thread-local counts）不调用分配器（allocator）；线程退出（thread exit）后的不可用状态不在观测窗口（observation window）。
fn record(operation: usize) {
    let _ = OBSERVING.try_with(|observing| {
        if observing.get() {
            let _ = COUNTS.try_with(|counts| {
                let mut value = counts.get();
                match operation {
                    0 => value.allocations = value.allocations.saturating_add(1),
                    1 => value.deallocations = value.deallocations.saturating_add(1),
                    _ => value.reallocations = value.reallocations.saturating_add(1),
                }
                counts.set(value);
            });
        }
    });
}

// SAFETY: 原样转交系统分配器（System allocator）的布局（Layout）和指针（pointer），计数（counting）不读取分配内容（allocation contents）。
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: 调用者（caller）提供GlobalAlloc规定的有效布局（valid layout）。
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record(0);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: 同一有效布局（valid layout）直接交系统零初始化分配（zeroed allocation）。
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record(0);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        record(1);
        // SAFETY: 原指针（pointer）及匹配布局（matching layout）不变，恰转交一次系统释放（deallocation）。
        unsafe { System.dealloc(pointer, layout); }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: 原指针（pointer）、布局（layout）和新大小（new size）沿GlobalAlloc前提原样传递。
        let pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !pointer.is_null() {
            record(2);
        }
        pointer
    }
}

/// 异常展开（unwinding）也结束观测窗口（observation window），不把测试报告（test reporting）误计为生产成本（production cost）。
struct ObservationWindow;
impl Drop for ObservationWindow {
    fn drop(&mut self) {
        OBSERVING.with(|observing| observing.set(false));
    }
}

/// 调用前预初始化线程局部存储（TLS），窗口内只运行原公开操作（public operation）。
fn measure<R>(action: impl FnOnce() -> R) -> (R, Counts) {
    COUNTS.with(|counts| counts.set(Counts::default()));
    OBSERVING.with(|observing| assert!(!observing.replace(true)));
    let window = ObservationWindow;
    let result = action();
    drop(window);
    (result, COUNTS.with(Cell::get))
}

/// 期望值（expected counts）来自公开成本约定（public cost contract），报告（reporting）始终在窗口外。
fn expect_counts(label: &str, actual: Counts, allocations: usize, deallocations: usize) {
    eprintln!("{label}: {actual:?}");
    assert_eq!(actual, Counts { allocations, deallocations, reallocations: 0 });
}

/// T380／T383／T398／T406：构造（construction）分别两次／一次共享分配（Arc allocation），有效all克隆（Clone）一次，any克隆（Clone）零次。
/// 配对释放（paired deallocation）覆盖正常领取（normal collection）、已接受后放弃（abandonment）及关闭后拒绝（rejection），不把T::drop当共享分配释放证明。
#[test]
fn test_public_wait_allocations_and_paired_releases() {
    let (_, counts) = measure(|| {
        let value = std::hint::black_box(Box::new([0u8; 8]));
        drop(std::hint::black_box(value));
    });
    expect_counts("observer control", counts, 1, 1);
    let mut context = Context::from_waker(Waker::noop());

    let ((mut wait, sender), counts) = measure(wait_one::<()>);
    expect_counts("one create", counts, 1, 0);
    let (result, counts) = measure(|| sender.send(()));
    result.unwrap();
    expect_counts("one send", counts, 0, 0);
    let (result, counts) = measure(|| Pin::new(&mut wait).poll(&mut context));
    assert_eq!(result, Poll::Ready(Ok(())));
    expect_counts("one collect", counts, 0, 1);
    let (_, counts) = measure(|| drop(wait));
    expect_counts("one completed drop", counts, 0, 0);

    let ((wait, sender), counts) = measure(wait_one::<()>);
    expect_counts("one unused create", counts, 1, 0);
    let (_, counts) = measure(|| drop(wait));
    expect_counts("one close with active sender", counts, 0, 0);
    let (_, counts) = measure(|| drop(sender));
    expect_counts("one final sender drop", counts, 0, 1);

    let ((wait, sender), counts) = measure(wait_any::<()>);
    expect_counts("any create", counts, 1, 0);
    let (other, counts) = measure(|| sender.clone());
    expect_counts("any active clone", counts, 0, 0);
    let (result, counts) = measure(|| sender.send(()));
    result.unwrap();
    expect_counts("any send", counts, 0, 0);
    let (rejected, counts) = measure(|| other.clone());
    expect_counts("any rejected clone", counts, 0, 0);
    let (_, counts) = measure(|| drop(wait));
    expect_counts("any close with remaining sender", counts, 0, 0);
    let (result, counts) = measure(|| other.send(()));
    assert_eq!(result.unwrap_err().into_parts(), (SendErrorKind::ValueAlreadyAccepted, ()));
    expect_counts("any last active rejection", counts, 0, 1);
    let (_, counts) = measure(|| drop(rejected));
    expect_counts("any rejected drop", counts, 0, 0);

    let ((mut wait, first), counts) = measure(wait_all::<()>);
    expect_counts("all create", counts, 2, 0);
    let (second, counts) = measure(|| first.clone());
    expect_counts("all active clone", counts, 1, 0);
    let (result, counts) = measure(|| first.send(()));
    result.unwrap();
    expect_counts("all first send", counts, 0, 0);
    let (_, counts) = measure(|| drop(second));
    expect_counts("all final sender drop", counts, 0, 0);
    let (result, counts) = measure(|| Pin::new(&mut wait).poll(&mut context));
    let Poll::Ready(values) = result else { panic!("全部位置（slots）已结束"); };
    expect_counts("all collect result Vec and release group/slots", counts, 1, 3);
    assert_eq!(values, vec![Ok(()), Err(WaitError::SenderDropped)]);
    let (_, counts) = measure(|| drop(values));
    expect_counts("all result Vec drop", counts, 0, 1);
    let (_, counts) = measure(|| drop(wait));
    expect_counts("all completed drop", counts, 0, 0);

    let ((wait, first), counts) = measure(wait_all::<()>);
    expect_counts("all abandoned create", counts, 2, 0);
    let (second, counts) = measure(|| first.clone());
    expect_counts("all abandoned active clone", counts, 1, 0);
    let (_, counts) = measure(|| { first.send(()).unwrap(); second.send(()).unwrap(); });
    expect_counts("all accepted sends", counts, 0, 0);
    let (_, counts) = measure(|| drop(wait));
    expect_counts("all abandoned group and slots drop", counts, 0, 3);

    let ((wait, sender), counts) = measure(wait_all::<()>);
    expect_counts("all closed create", counts, 2, 0);
    let (_, counts) = measure(|| drop(wait));
    expect_counts("all close with active sender", counts, 0, 0);
    let (rejected, counts) = measure(|| sender.clone());
    expect_counts("all closed rejected clone", counts, 0, 0);
    let (_, counts) = measure(|| drop(sender));
    expect_counts("all closed final group and slot drop", counts, 0, 2);
    let (_, counts) = measure(|| drop(rejected));
    expect_counts("all rejected drop", counts, 0, 0);
}
