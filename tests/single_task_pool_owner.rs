//! 单线程默认池所有者误判的安全红线及对照。
//!
//! 对应问题：`PI-ASYNC-RT-OWNER-BUG-001`，本地分析入口：
//! `docs/SINGLE_TASK_POOL_OWNER_ANALYSIS.md#owner-tests`。
//! 使用真实 `WorkerTaskRunner -> SingleTaskRunner -> SingleTaskPool`，由 `prelude`
//! 分别选择默认实现或 `serial` 实现。这里不使用替代队列、手工任务驱动或 `block_on`。
//!
//! 全部队列访问通过线程结束与 `join` 建立先后关系。测试故意不让旧实现的本地
//! 容器被并发访问；失败仅证明身份副作用或错误分流，不声称复现内存破坏。
//! 每例至多两个任务，按 `--test-threads=1` 执行，无大规模负载或人工观测要求。

use std::sync::{Arc, Mutex};
use std::thread;

use futures::channel::oneshot;
use pi_async_rt::prelude::{AsyncRuntime, AsyncTaskPool, WorkerTaskRunner};
use pi_async_rt::rt::{multi_thread::ComputationalTaskPool, AsyncTaskPool as DefaultTaskPool};

/// 为每例提供干净的线程局部状态，并在返回前等待该线程完全退出。
///
/// 不推进运行时、不注册身份、不替代任何生产功能；线程创建可能失败并使测试失败。
/// 时间为被测闭包耗时加线程生命周期成本，额外空间为一个线程栈；仅测试控制线程阻塞。
fn fresh_thread<R: Send + 'static>(run: impl FnOnce() -> R + Send + 'static) -> R {
    thread::spawn(run).join().expect("测试线程异常退出")
}

/// 借用生产侧已有的只读查询观测共享 TLS，不构建多线程运行时也不启动 worker。
///
/// `ComputationalTaskPool::get_thread_id` 本身无绑定副作用；此处不访问它的本地队列。
/// 单 worker 配置只限制夹具空间，不能为被测单线程池提供所有者授权。
fn observe_thread_id(probe: &ComputationalTaskPool<()>) -> usize {
    DefaultTaskPool::get_thread_id(probe)
}

/// 只读查询不能使没有运行时身份的普通线程获得目标池的所有者身份。
///
/// 先让创建线程退出，再查询保留的空池；不依赖具体数字 ID，也不访问本地队列。
#[test]
fn test_unbound_query_does_not_claim_owner() {
    let (pool, owner_id, creator) = fresh_thread(|| {
        let runner = WorkerTaskRunner::<()>::default();
        let runtime = runner.get_runtime();
        (runtime.shared_pool(), runtime.get_id(), thread::current().id())
    });
    let (before, after, observed, observer) = fresh_thread(move || {
        let probe = ComputationalTaskPool::<()>::new(1);
        let before = observe_thread_id(&probe);
        let first = pool.get_thread_id();
        let second = pool.get_thread_id();
        assert_eq!(first, second, "没有状态推进时重复查询应一致");
        (before, observe_thread_id(&probe), first, thread::current().id())
    });
    assert_ne!(creator, observer);
    assert_eq!(before, usize::MAX, "外部线程必须从未绑定状态开始");
    assert_eq!(before, after,
        "查询不得修改当前线程身份：目标运行时={}，查询结果={}", owner_id, observed);
}

/// 外部普通提交是正对照：两个任务按公共队列先后执行，且提交者 TLS 不改变。
#[test]
fn test_external_plain_spawn_uses_public_without_claiming_owner() {
    fresh_thread(|| {
        let runner = WorkerTaskRunner::<()>::default();
        let runtime = runner.get_runtime();
        let values = Arc::new(Mutex::new(Vec::new()));
        let first = values.clone();
        runtime.spawn(async move { first.lock().unwrap().push(1); }).unwrap();

        let second = values.clone();
        let (before, after) = fresh_thread(move || {
            let probe = ComputationalTaskPool::<()>::new(1);
            let before = observe_thread_id(&probe);
            runtime.spawn(async move { second.lock().unwrap().push(2); }).unwrap();
            (before, observe_thread_id(&probe))
        });
        runner.run_once().unwrap();
        runner.run_once().unwrap();
        assert_eq!(&*values.lock().unwrap(), &[1, 2]);
        assert_eq!(before, usize::MAX);
        assert_eq!(before, after);
        assert_eq!(runner.get_runtime().len(), 0);
    });
}

/// 外部最高优先级提交必须走已有公共队列回退，而不是抢占所有者的本地栈。
///
/// 公共队列先放标记 1，再由新线程提交最高优先级标记 2；外部线程退出后才消费。
/// 正确回退应得到 `[1, 2]`，错误本地栈分流会得到 `[2, 1]`；不依赖计时或随机权重。
#[test]
fn test_unbound_external_priority_uses_public_fallback() {
    fresh_thread(|| {
        let runner = WorkerTaskRunner::<()>::default();
        let runtime = runner.get_runtime();
        let values = Arc::new(Mutex::new(Vec::new()));
        let first = values.clone();
        runtime.spawn(async move { first.lock().unwrap().push(1); }).unwrap();

        let second = values.clone();
        fresh_thread(move || {
            runtime.spawn_priority(10, async move { second.lock().unwrap().push(2); }).unwrap();
        });
        runner.run_once().unwrap();
        runner.run_once().unwrap();
        assert_eq!(runner.get_runtime().len(), 0);
        assert_eq!(&*values.lock().unwrap(), &[1, 2], "外部提交不得进入所有者本地栈");
    });
}

/// 已停稳的真实 oneshot 接收任务由外部线程唤醒时，不得授予该线程本地队列权限。
///
/// 首次 `run_once` 已返回，才能发送值，排除托管任务仍在运行而合并唤醒的假阴性。
/// 发送者结束后再驱动第二次，确保旧实现也没有并发容器访问；同时校验结果和执行线程。
#[test]
fn test_external_oneshot_wake_does_not_claim_owner() {
    fresh_thread(|| {
        let runner = WorkerTaskRunner::<()>::default();
        let runtime = runner.get_runtime();
        let owner = thread::current().id();
        let (sender, receiver) = oneshot::channel::<usize>();
        let value = Arc::new(Mutex::new(None));
        let output = value.clone();
        runtime.spawn(async move {
            let received = receiver.await.unwrap();
            *output.lock().unwrap() = Some((received, thread::current().id()));
        }).unwrap();
        runner.run_once().unwrap();
        assert!(value.lock().unwrap().is_none());
        assert_eq!(runtime.len(), 0);

        let (before, after) = fresh_thread(move || {
            let probe = ComputationalTaskPool::<()>::new(1);
            let before = observe_thread_id(&probe);
            sender.send(42).unwrap();
            (before, observe_thread_id(&probe))
        });
        assert_eq!(runtime.len(), 1, "一次实际唤醒必须留下一个可运行任务");
        runner.run_once().unwrap();
        assert_eq!(*value.lock().unwrap(), Some((42, owner)));
        assert_eq!(runtime.len(), 0);
        assert_eq!(before, usize::MAX);
        assert_eq!(before, after, "真实通道唤醒不得隐式认领目标池");
    });
}

/// 已绑定其它运行时的生产者是正对照，不得改变自身身份或进入目标池本地栈。
///
/// 同一线程仍只有一个实际驱动者；其它运行时仅用于建立已存在的身份边界。
#[test]
fn test_bound_foreign_producer_preserves_identity_and_public_fallback() {
    fresh_thread(|| {
        let runner = WorkerTaskRunner::<()>::default();
        let runtime = runner.get_runtime();
        let target_id = runtime.get_id();
        let values = Arc::new(Mutex::new(Vec::new()));
        let first = values.clone();
        runtime.spawn(async move { first.lock().unwrap().push(1); }).unwrap();

        let second = values.clone();
        let (foreign_id, before, after) = fresh_thread(move || {
            let foreign = WorkerTaskRunner::<()>::default();
            foreign.run_once().unwrap();
            let foreign_id = foreign.get_runtime().get_id();
            let probe = ComputationalTaskPool::<()>::new(1);
            let before = observe_thread_id(&probe);
            let _ = runtime.shared_pool().get_thread_id();
            runtime.spawn_priority(10, async move { second.lock().unwrap().push(2); }).unwrap();
            (foreign_id, before, observe_thread_id(&probe))
        });
        runner.run_once().unwrap();
        runner.run_once().unwrap();
        assert_ne!(target_id, foreign_id);
        assert_eq!(before, after);
        assert_eq!(&*values.lock().unwrap(), &[1, 2]);
        assert_eq!(runner.get_runtime().len(), 0);
    });
}
