//! 通过所属域（TaskThread）公开入口核对生命周期（lifecycle）基础。

use pi_async_task::TaskThread;

/// @test T71；@api TaskThread::new；@impl thread_new_inner。
/// 空所属域（TaskThread）创建（creation）及正常析构（drop）必须完成，不启动额外线程（thread）。
#[test]
fn test_thread_creation_establishes_an_empty_owner() {
    let created = std::panic::catch_unwind(TaskThread::new);
    assert!(created.is_ok(), "公开创建（creation）必须能建立所属域（TaskThread）");
}

/// @test T72；@api TaskThread::id；@impl thread_id_inner。
/// 同线程（thread）的不同所属域（TaskThread）也必须具有稳定且不同的非零身份（ID）。
#[test]
fn test_thread_identity_is_stable_and_never_reused() {
    let first = TaskThread::new();
    let second = TaskThread::new();
    let id = first.id();
    assert_ne!(id.get(), 0);
    assert_eq!(first.id(), id);
    assert_ne!(second.id(), id);
}

/// @test T73；@api TaskThread::is_closed；@impl thread_closed_inner。
/// 新所属域（TaskThread）必须允许接纳（admission），而不是默认关闭（closed）。
#[test]
fn test_new_thread_is_open() {
    let owner = TaskThread::new();
    assert!(!owner.is_closed());
}

/// @test T74；@api TaskThread::live_tasks；@impl thread_live_inner。
/// 空所属域（TaskThread）没有原值清理责任；查询（query）不产生登记（registration）。
#[test]
fn test_empty_owner_has_no_live_tasks() {
    let owner = TaskThread::new();
    assert_eq!(owner.live_tasks(), 0);
    assert_eq!(owner.live_tasks(), 0);
}
