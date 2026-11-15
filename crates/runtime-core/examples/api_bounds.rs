//! 仅编译的公开约束示例；不执行尚未实现的运行时（runtime）能力。
use pi_async_runtime_core::{Runtime, UnsubmittedTask};
use std::{cell::Cell, rc::Rc, time::Duration};

#[allow(dead_code)]
fn borrowed_root<R: Runtime>(runtime: &R) {
    let text = String::from("borrowed");
    let _value = runtime.block_on(async { text.as_str() });
    let _wait = runtime.timeout_on(async { text.as_str() }, Duration::from_millis(2));
}
#[allow(dead_code)]
fn local_non_send<R: Runtime>() {
    let value = Rc::new(Cell::new(1));
    let _result = R::spawn_local(async move {
        value.set(2);
        value
    });
    let value = Rc::new(Cell::new(3));
    let _timed = R::spawn_timing_local(async move { value }, Duration::from_millis(2));
}
#[allow(dead_code)]
fn borrowed_wait<R: Runtime>(runtime: &R, value: &str) {
    let (_wait, sender) = runtime.wait_one_timeout::<&str>(Duration::from_millis(2));
    let _sent = sender.send(value);
    let (_wait, sender) = runtime.wait_any_timeout::<Cell<u32>>(Duration::from_millis(3));
    let _sent = sender.send(Cell::new(1));
}
#[allow(dead_code)]
fn submission_is_transparent() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<UnsubmittedTask>();
}
fn main() {}
