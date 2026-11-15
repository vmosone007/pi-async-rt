//! 仅编译公开签名及静态组合，不执行构造或任何占位。
use pi_async_runtime::prelude::*;
use pi_async_runtime::{ExecutorHooks, IdlePolicy, ManualClock};
use std::{
    cell::Cell,
    future::Future,
    marker::PhantomPinned,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

struct Borrowed<'a> {
    value: &'a Rc<usize>,
    fixed: PhantomPinned,
}
impl Future for Borrowed<'_> {
    type Output = Rc<usize>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Rc::clone(self.as_ref().get_ref().value))
    }
}
struct SendNotSync {
    value: Cell<usize>,
    fixed: PhantomPinned,
}
impl Future for SendNotSync {
    type Output = usize;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(self.as_ref().get_ref().value.get())
    }
}
struct LocalPolicy(Rc<()>);
impl IdlePolicy for LocalPolicy {}
struct LocalHooks(Rc<()>);
impl ExecutorHooks for LocalHooks {}
fn send_sync<T: Send + Sync>() {}
fn capabilities<T: pi_async_runtime::RuntimeCapabilities>() {}
fn roots(runtime: &ConcurrentRuntime, handle: &Handle) {
    let value = Rc::new(7);
    let _: Rc<usize> = runtime.block_on(Borrowed {
        value: &value,
        fixed: PhantomPinned,
    });
    let _: Rc<usize> = handle.block_on(Borrowed {
        value: &value,
        fixed: PhantomPinned,
    });
    let _ = runtime.timeout_on(
        Borrowed {
            value: &value,
            fixed: PhantomPinned,
        },
        std::time::Duration::from_millis(1),
    );
    let _ = handle.spawn(SendNotSync {
        value: Cell::new(3),
        fixed: PhantomPinned,
    });
    let _ = ConcurrentRuntime::spawn_local(async { Rc::new(1usize) });
    let _ = ConcurrentRuntime::spawn_local_front(async { Rc::new(2usize) });
    let _ = SerialRuntime::idle_local(async { Cell::new(4usize) });
    let _ = handle.wait_one_timeout::<Rc<usize>>(std::time::Duration::from_secs(2));
}
fn declarations(clock: ManualClock) {
    send_sync::<Handle>();
    capabilities::<ConcurrentRuntime>();
    capabilities::<SerialRuntime>();
    capabilities::<Handle>();
    let _ = ConcurrentRuntime::with_components(
        clock,
        LocalPolicy(Rc::new(())),
        LocalHooks(Rc::new(())),
        RuntimeOptions::default(),
    );
}
fn main() {}
