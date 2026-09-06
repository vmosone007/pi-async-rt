//! 所有者修复的公开合同专项，默认/serial 使用同一组真实生产装配。
//! 矩阵：`docs/SINGLE_TASK_POOL_OWNER_DESIGN.md#owner-design` 的 OWN-F02~F12。
//! 不创建多线程运行时；每例有界，串行执行。block_on 只作为拒绝边界的被测 API，
//! 不用于驱动夹具。断言遵循既有合同，不把历史批量计数/优先级语义替换为测试实现。

use std::{io::{Error, ErrorKind}, panic::{catch_unwind, AssertUnwindSafe}, sync::{Arc, Mutex},
    sync::atomic::{AtomicUsize, Ordering}, thread, time::{Duration, Instant}};
use futures::{channel::oneshot, future::{join_all, poll_fn, select, Either}, stream, StreamExt};
use pi_async_rt::prelude::{AsyncRuntime, AsyncRuntimeExt, AsyncTaskPool, AsyncValue,
    SingleTaskPool, SingleTaskRunner, SingleTaskRuntime, WorkerTaskRunner};
use pi_async_rt::rt::{startup_global_time_loop, AsyncPipelineResult};

const LIMIT: Duration = Duration::from_secs(5);

/// 只重复调用公开驱动入口，不替代轮询/唤醒。截止独立于 runtime 的 timer。
fn drive(runner: &SingleTaskRunner<()>, done: impl Fn() -> bool) {
    let start = Instant::now();
    while !done() {
        assert!(start.elapsed() < LIMIT, "真实执行器未在截止内完成");
        runner.run_once().unwrap();
        thread::yield_now();
    }
}

struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) { self.0.fetch_add(1, Ordering::SeqCst); }
}

async fn record(values: Arc<Mutex<Vec<usize>>>, value: usize) {
    values.lock().unwrap().push(value);
}

/// OWN-F02：启动不绑定；真实消费移交成功后，即使消费者退出也不能迁移回来。
#[test]
fn test_startup_move_first_consumer_and_distinct_runtime_ids() {
    let runner = SingleTaskRunner::<()>::default();
    assert_eq!(runner.run_once().unwrap_err().kind(), ErrorKind::Other);
    assert_eq!(runner.run().unwrap_err().kind(), ErrorKind::Other);
    let rt = runner.startup().unwrap();
    assert!(runner.startup().is_none());
    let second = SingleTaskRunner::<()>::default();
    let second_rt = second.startup().unwrap();
    assert_ne!(rt.get_id(), second_rt.get_id());
    let values = Arc::new(Mutex::new(Vec::new()));
    rt.spawn_local(record(values.clone(), 1)).unwrap();
    let consumer = thread::spawn(move || {
        runner.run_once().unwrap();
        (runner, thread::current().id())
    }).join().unwrap();
    assert_ne!(consumer.1, thread::current().id());
    assert_eq!(&*values.lock().unwrap(), &[1]);
    assert_eq!(consumer.0.run_once().unwrap_err().kind(), ErrorKind::PermissionDenied);
    assert_eq!(rt.len(), 0);
    second.run_once().unwrap();
    assert_eq!(second_rt.len(), 0);
}

/// OWN-F09：错误线程的全部消费入口必须前置拒绝；原owner、timer和任务结果保持可推进。
#[test]
fn test_wrong_consumer_rejected_before_timer_pop_and_block_on_enqueue() {
    let _clock = startup_global_time_loop(1);
    let runner = Arc::new(SingleTaskRunner::<()>::default());
    let rt = runner.startup().unwrap();
    runner.run_once().unwrap();
    let done = Arc::new(AtomicUsize::new(0));
    let output = done.clone();
    let timer_rt = rt.clone();
    rt.spawn(async move { timer_rt.timeout(0).await; output.fetch_add(1, Ordering::SeqCst); }).unwrap();
    runner.run_once().unwrap();
    assert_eq!(done.load(Ordering::SeqCst), 0);
    let before_len = rt.len();
    let before_wait = rt.wait_len();
    let rejected_drops = Arc::new(AtomicUsize::new(0));
    let witness = DropCount(rejected_drops.clone());
    let other = runner.clone();
    let remote = rt.clone();
    thread::spawn(move || {
        assert_eq!(other.run_once().unwrap_err().kind(), ErrorKind::PermissionDenied);
        assert_eq!(other.run().unwrap_err().kind(), ErrorKind::PermissionDenied);
        assert!(catch_unwind(AssertUnwindSafe(|| remote.shared_pool().try_pop())).is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| remote.shared_pool().try_pop_all())).is_err());
        // 只测试前置拒绝；此 future 不得被 poll，不能遗留捕获 block_on 栈的队列项。
        let result: std::io::Result<()> = remote.block_on(async move {
            drop(witness);
            panic!("被拒绝的 future 不应执行");
        });
        assert_eq!(result.unwrap_err().kind(), ErrorKind::PermissionDenied);
    }).join().unwrap();
    assert_eq!(rejected_drops.load(Ordering::SeqCst), 1);
    assert_eq!(rt.len(), before_len);
    assert_eq!(rt.wait_len(), before_wait);
    assert_eq!(done.load(Ordering::SeqCst), 0);
    drive(&runner, || done.load(Ordering::SeqCst) == 1);
    drive(&runner, || rt.len() == 0 && rt.wait_len() == 0);
}

/// OWN-F09-P：直接调用空池出队也是合法首次消费，后续Runner必须尊重这一绑定。
#[test]
fn test_direct_empty_pop_and_batch_bind_before_runner() {
    for batch in [false, true] {
        let pool = SingleTaskPool::<()>::default();
        if batch { assert_eq!(pool.try_pop_all().count(), 0); }
        else { assert!(pool.try_pop().is_none()); }
        let runner = SingleTaskRunner::new(pool);
        let rt = runner.startup().unwrap();
        let runner = thread::spawn(move || {
            assert_eq!(runner.run_once().unwrap_err().kind(), ErrorKind::PermissionDenied);
            runner
        }).join().unwrap();
        rt.spawn(async {}).unwrap();
        assert_eq!(runner.run_once().unwrap(), 0);
        assert_eq!(rt.len(), 0);
    }
}

/// OWN-F03：用独立FIFO/LIFO顺序判据证明分流正确，不用生产选择器计算期望值。
#[test]
fn test_prebind_priority_fallback_and_owner_fifo_lifo() {
    let priorities = [0, 4, 5, 9, 10, usize::MAX];
    let runner = SingleTaskRunner::<()>::default();
    let rt = runner.startup().unwrap();
    let values = Arc::new(Mutex::new(Vec::new()));
    for (i, priority) in priorities.iter().enumerate() {
        rt.spawn_priority(*priority, record(values.clone(), i)).unwrap();
    }
    drive(&runner, || values.lock().unwrap().len() == 6);
    assert_eq!(&*values.lock().unwrap(), &[0, 1, 2, 3, 4, 5]);
    values.lock().unwrap().clear();
    for (i, priority) in priorities.iter().enumerate() {
        rt.spawn_priority(*priority, record(values.clone(), i)).unwrap();
    }
    drive(&runner, || values.lock().unwrap().len() == 6);
    let result = values.lock().unwrap().clone();
    assert_eq!(&result[..2], &[5, 4], "owner 最高优先级是栈");
    assert_eq!(result.iter().copied().filter(|n| *n < 2).collect::<Vec<_>>(), vec![0, 1]);
    assert_eq!(result.iter().copied().filter(|n| *n == 2 || *n == 3).collect::<Vec<_>>(), vec![2, 3]);
    assert_eq!(rt.len(), 0);
}

/// OWN-F03-W/F12：合法权重等价类及容量增长必须保留任务全集；非法无界驱动不执行。
#[test]
fn test_weight_and_capacity_boundaries_preserve_progress() {
    for weights in [[0, 1], [1, 0], [1, 1], [1, 254], [254, 1], [254, 254]] {
        let runner = SingleTaskRunner::new(SingleTaskPool::<()>::new(weights));
        let rt = runner.startup().unwrap();
        runner.run_once().unwrap();
        let values = Arc::new(Mutex::new(Vec::new()));
        for i in 0usize..17 {
            if i.is_multiple_of(2) { rt.spawn(record(values.clone(), i)).unwrap(); }
            else { rt.spawn_local(record(values.clone(), i)).unwrap(); }
        }
        drive(&runner, || values.lock().unwrap().len() == 17);
        let mut result = values.lock().unwrap().clone();
        result.sort_unstable();
        assert_eq!(result, (0..17).collect::<Vec<_>>());
        assert_eq!(rt.len(), 0);
    }
    for weights in [[255, 0], [0, 255], [255, 255]] {
        assert!(catch_unwind(|| SingleTaskPool::<()>::new(weights)).is_err());
    }
    // [0,0] 构造属于可表达输入；驱动不在合法域，不让旧选择器的无界循环污染测试。
    assert_eq!(SingleTaskPool::<()>::new([0, 0]).len(), 0);
}

/// OWN-F12：明确保护历史批量范围和计数，而不是在测试中顺便修正其它API语义。
#[test]
fn test_batch_scope_counts_and_foreign_task_id_fallback_are_unchanged() {
    let runner = SingleTaskRunner::<()>::default();
    let rt = runner.startup().unwrap();
    runner.run_once().unwrap();
    let foreign = SingleTaskRunner::<()>::default();
    let foreign_rt = foreign.startup().unwrap();
    let pool = rt.shared_pool();
    rt.spawn(async {}).unwrap();
    rt.spawn_local(async {}).unwrap();
    rt.spawn_priority(10, async {}).unwrap();
    // 类型允许交叉 TaskId；只证明本地路由的防御边界，不宣称这是推荐生产协议。
    rt.spawn_priority_by_id(foreign_rt.alloc::<()>(), 10, async {}).unwrap();
    assert_eq!(rt.len(), 4);
    let all: Vec<_> = pool.try_pop_all().collect();
    assert_eq!(all.len(), 3);
    assert_eq!(all[0].priority(), 5);
    assert_eq!(all[2].owner(), foreign_rt.get_id());
    assert_eq!(rt.len(), 4, "历史 batch 不扣减消费计数");
    assert!(pool.try_pop().is_some(), "本地栈不属于历史 batch 范围");
    assert!(pool.try_pop().is_none());
    assert_eq!(pool.try_pop_all().count(), 0);
    assert_eq!(rt.len(), 3, "保留既有计数，不把本轮扩大为计数重构");
}

/// 穷举公开提交入口，真实任务结果及 context 释放是独立观测量。
fn enqueue_matrix<RT: AsyncRuntime<()> + AsyncRuntimeExt<()>>(
    rt: RT, values: Arc<Mutex<Vec<usize>>>, drops: Arc<AtomicUsize>) {
    rt.spawn(record(values.clone(), 0)).unwrap();
    rt.spawn_local(record(values.clone(), 1)).unwrap();
    rt.spawn_priority(10, record(values.clone(), 2)).unwrap();
    rt.spawn_yield(record(values.clone(), 3)).unwrap();
    rt.spawn_timing(record(values.clone(), 4), 0).unwrap();
    rt.spawn_by_id(rt.alloc::<()>(), record(values.clone(), 5)).unwrap();
    rt.spawn_local_by_id(rt.alloc::<()>(), record(values.clone(), 6)).unwrap();
    rt.spawn_priority_by_id(rt.alloc::<()>(), 9, record(values.clone(), 7)).unwrap();
    rt.spawn_yield_by_id(rt.alloc::<()>(), record(values.clone(), 8)).unwrap();
    rt.spawn_timing_by_id(rt.alloc::<()>(), record(values.clone(), 9), 1).unwrap();
    rt.spawn_with_context(rt.alloc::<()>(), record(values.clone(), 10), DropCount(drops.clone())).unwrap();
    rt.spawn_timing_with_context(rt.alloc::<()>(), record(values, 11), DropCount(drops), 1).unwrap();
}

/// OWN-F04/F12-C：公开提交入口、context释放、关闭拒绝同时覆盖Single及Worker委托。
#[test]
fn test_all_spawn_variants_context_and_worker_wrappers() {
    let _clock = startup_global_time_loop(1);
    let values = Arc::new(Mutex::new(Vec::new()));
    let drops = Arc::new(AtomicUsize::new(0));
    let runner = SingleTaskRunner::<()>::default();
    let rt = runner.startup().unwrap();
    enqueue_matrix(rt.clone(), values.clone(), drops.clone());
    drive(&runner, || values.lock().unwrap().len() == 12 && rt.len() == 0 && rt.wait_len() == 0);
    values.lock().unwrap().sort_unstable();
    assert_eq!(&*values.lock().unwrap(), &(0..12).collect::<Vec<_>>());
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert!(!rt.close(), "单线程运行时的 close 原合同是不支持");
    values.lock().unwrap().clear();
    let worker = WorkerTaskRunner::<()>::default();
    let rt = worker.get_runtime();
    let submitted = rt.clone();
    let observed = values.clone();
    let released = drops.clone();
    thread::spawn(move || enqueue_matrix(submitted, observed, released)).join().unwrap();
    let start = Instant::now();
    while values.lock().unwrap().len() != 12 || rt.len() != 0 || rt.wait_len() != 0 {
        assert!(start.elapsed() < LIMIT);
        worker.run_once().unwrap();
        thread::yield_now();
    }
    values.lock().unwrap().sort_unstable();
    assert_eq!(&*values.lock().unwrap(), &(0..12).collect::<Vec<_>>());
    assert_eq!(drops.load(Ordering::SeqCst), 4);
    assert!(rt.close());
    assert!(!rt.close());
    assert!(rt.spawn(async {}).is_err());
    assert!(rt.spawn_local(async {}).is_err());
    assert!(rt.spawn_priority(usize::MAX, async {}).is_err());
    assert!(rt.spawn_yield(async {}).is_err());
    assert!(rt.spawn_timing(async {}, 0).is_err());
    assert!(rt.spawn_by_id(rt.alloc::<()>(), async {}).is_err());
    assert!(rt.spawn_local_by_id(rt.alloc::<()>(), async {}).is_err());
    assert!(rt.spawn_priority_by_id(rt.alloc::<()>(), 5, async {}).is_err());
    assert!(rt.spawn_yield_by_id(rt.alloc::<()>(), async {}).is_err());
    assert!(rt.spawn_timing_by_id(rt.alloc::<()>(), async {}, 0).is_err());
    assert_eq!(rt.len(), 0);
}

/// OWN-F07：合法组合子取消子timer不应污染后续等待，真实执行器负责全部唤醒。
#[test]
fn test_local_adapter_yield_timeout_join_select_and_cancellation() {
    let _clock = startup_global_time_loop(1);
    let runner = SingleTaskRunner::<()>::default();
    let rt = runner.startup().unwrap();
    let local = rt.to_local_runtime();
    assert_eq!(local.get_id(), rt.get_id());
    let done = Arc::new(AtomicUsize::new(0));
    let output = done.clone();
    let inner = rt.clone();
    local.spawn(async move {
        inner.yield_now().await;
        join_all(vec![inner.timeout(0), inner.timeout(1), inner.timeout(2)]).await;
        let value = AsyncValue::new();
        let set = value.clone();
        set.set(19usize);
        match select(Box::pin(value), inner.timeout(1)).await {
            Either::Left((19, timer)) => drop(timer),
            _ => panic!("已就绪值应先返回"),
        }
        // 真实 poll_fn 组合子只推进其子 future，执行器/Waker 仍由生产代码提供。
        poll_fn(|cx| {
            let mut timer = inner.timeout(1);
            assert!(timer.as_mut().poll(cx).is_pending());
            drop(timer);
            std::task::Poll::Ready(())
        }).await;
        inner.timeout(3).await;
        output.fetch_add(1, Ordering::SeqCst);
    }).unwrap();
    drive(&runner, || done.load(Ordering::SeqCst) == 1 && rt.wait_len() == 0 && rt.len() == 0);
    assert_eq!(done.load(Ordering::SeqCst), 1);
}

/// OWN-F05-V：同一任务组合多个真实回填来源，检查外部wake与关闭错误能共同完成。
#[test]
fn test_pending_channel_async_value_and_sender_receiver_drop() {
    let runner = SingleTaskRunner::<()>::default();
    let rt = runner.startup().unwrap();
    let (send, recv) = oneshot::channel::<usize>();
    let (closed_send, closed_recv) = oneshot::channel::<usize>();
    let value = AsyncValue::<usize>::new();
    let setter = value.clone();
    let (channel_send, channel_recv) = flume::bounded(1);
    let result = Arc::new(Mutex::new(None));
    let output = result.clone();
    rt.spawn(async move {
        let (a, b, c, d) = futures::join!(recv, value, channel_recv.recv_async(), closed_recv);
        assert!(d.is_err());
        *output.lock().unwrap() = Some((a.unwrap(), b, c.unwrap()));
    }).unwrap();
    runner.run_once().unwrap();
    assert!(result.lock().unwrap().is_none());
    thread::spawn(move || {
        send.send(1).unwrap();
        setter.set(2);
        channel_send.send(3).unwrap();
        drop(closed_send);
    }).join().unwrap();
    drive(&runner, || result.lock().unwrap().is_some());
    assert_eq!(*result.lock().unwrap(), Some((1, 2, 3)));
    drive(&runner, || rt.len() == 0);
    let (send, recv) = oneshot::channel::<DropCount>();
    let drops = Arc::new(AtomicUsize::new(0));
    drop(recv);
    assert!(send.send(DropCount(drops.clone())).is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// OWN-F08：两个顺序驱动的真实单线程运行时，逐项断言成功、业务错误、超时及容量边界。
#[test]
fn test_wait_any_callback_map_reduce_and_pipeline_across_single_runners() {
    let _clock = startup_global_time_loop(1);
    let first = SingleTaskRunner::<()>::default();
    let a = first.startup().unwrap();
    let second = SingleTaskRunner::<()>::default();
    let b = second.startup().unwrap();
    let result = Arc::new(AtomicUsize::new(0));
    let output = result.clone();
    let caller = a.clone();
    let worker = b.clone();
    a.spawn(async move {
        let wait = caller.wait();
        wait.spawn(worker.clone(), None, async { Ok(17usize) }).unwrap();
        assert_eq!(wait.wait_result().await.unwrap(), 17);
        let wait = caller.wait::<usize>();
        wait.spawn(worker.clone(), None, async {
            Err(Error::new(ErrorKind::InvalidData, "业务失败"))
        }).unwrap();
        let error = wait.wait_result().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        let (cancel, canceled) = oneshot::channel::<()>();
        let wait = caller.wait::<usize>();
        wait.spawn(worker.clone(), Some(0), async move {
            assert!(canceled.await.is_err());
            Ok(0)
        }).unwrap();
        assert_eq!(wait.wait_result().await.unwrap_err().kind(), ErrorKind::TimedOut);
        drop(cancel);
        // 0 是 rendezvous 通道，不是无容量错误；接收者存在时应完成。
        let any = caller.wait_any(0);
        any.spawn(worker.clone(), async { Ok(23usize) }).unwrap();
        assert_eq!(any.wait_result().await.unwrap(), 23);
        let callback = caller.wait_any_callback(2);
        callback.spawn(worker.clone(), async { Ok(1usize) }).unwrap();
        callback.spawn(worker.clone(), async { Ok(2usize) }).unwrap();
        assert_eq!(callback.wait_result(|_| false).await.unwrap(), 2, "最后一项忽略 callback");
        let callback = caller.wait_any_callback(2);
        callback.spawn(worker.clone(), async { Ok(3usize) }).unwrap();
        callback.spawn(worker.clone(), async { Ok(4usize) }).unwrap();
        assert_eq!(callback.wait_result(|value| value.as_ref().map(|n| *n == 3).unwrap_or(false)).await.unwrap(), 3);
        let mut empty = caller.map_reduce::<usize>(0);
        assert!(empty.map(worker.clone(), async { Ok(0) }).is_err());
        assert!(empty.reduce(false).await.unwrap().is_empty());
        for order in [false, true] {
            let mut map = caller.map_reduce::<usize>(2);
            assert_eq!(map.map(worker.clone(), async { Ok(31) }).unwrap(), 0);
            assert_eq!(map.map(worker.clone(), async { Err(Error::new(ErrorKind::InvalidData, "映射失败")) }).unwrap(), 1);
            assert!(map.map(worker.clone(), async { Ok(99) }).is_err());
            let items = map.reduce(order).await.unwrap();
            assert_eq!(items.len(), 2);
            assert_eq!(*items[0].as_ref().unwrap(), 31);
            assert_eq!(items[1].as_ref().unwrap_err().kind(), ErrorKind::InvalidData);
        }
        let pipeline = caller.pipeline(stream::iter(0..5), |item| {
            if item == 3 { AsyncPipelineResult::Disconnect } else { AsyncPipelineResult::Filtered(item * 2) }
        });
        assert_eq!(pipeline.collect::<Vec<_>>().await, vec![0, 2, 4]);
        let empty = caller.pipeline(stream::empty::<usize>(), AsyncPipelineResult::Filtered);
        assert!(empty.collect::<Vec<_>>().await.is_empty());
        output.store(1, Ordering::SeqCst);
    }).unwrap();
    let start = Instant::now();
    while result.load(Ordering::SeqCst) != 1 || a.len() > 0 || b.len() > 0 || b.wait_len() > 0 {
        assert!(start.elapsed() < LIMIT);
        first.run_once().unwrap();
        second.run_once().unwrap();
        thread::yield_now();
    }
}

/// OWN-F06：用户poll的单一panic按原协议传播，不能使owner检查或本地后续提交失效。
#[test]
fn test_poll_panic_unwinds_without_changing_owner_or_losing_followup_task() {
    let runner = SingleTaskRunner::<()>::default();
    let rt = runner.startup().unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let witness = DropCount(drops.clone());
    rt.spawn(async move {
        let _witness = witness;
        panic!("专项预期的用户 poll panic");
    }).unwrap();
    assert!(catch_unwind(AssertUnwindSafe(|| runner.run_once())).is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let done = Arc::new(AtomicUsize::new(0));
    let output = done.clone();
    let local_rt = rt.clone();
    rt.spawn(async move {
        local_rt.spawn_local(async move { output.fetch_add(1, Ordering::SeqCst); }).unwrap();
    }).unwrap();
    drive(&runner, || done.load(Ordering::SeqCst) == 1);
    assert_eq!(rt.len(), 0);
}

/// context 析构能回调提交，不能与 owner 校验/队列借用重叠；单独的析构panic也须释放授权路径。
#[test]
fn test_context_drop_reentrant_submission_and_panic_keep_owner_usable() {
    struct ContextDrop {
        runtime: SingleTaskRuntime<()>,
        owner: thread::ThreadId,
        done: Arc<AtomicUsize>,
        panic: bool,
    }
    impl Drop for ContextDrop {
        fn drop(&mut self) {
            assert_eq!(thread::current().id(), self.owner);
            let done = self.done.clone();
            self.runtime.spawn_local(async move { done.fetch_add(1, Ordering::SeqCst); }).unwrap();
            if self.panic { panic!("专项预期的 context 析构panic"); }
        }
    }
    for panic in [false, true] {
        let runner = SingleTaskRunner::<()>::default();
        let rt = runner.startup().unwrap();
        let done = Arc::new(AtomicUsize::new(0));
        rt.spawn_with_context(rt.alloc::<()>(), async {}, ContextDrop {
            runtime: rt.clone(), owner: thread::current().id(), done: done.clone(), panic,
        }).unwrap();
        let result = catch_unwind(AssertUnwindSafe(|| runner.run_once()));
        assert_eq!(result.is_err(), panic);
        drive(&runner, || done.load(Ordering::SeqCst) == 1);
        assert_eq!(rt.len(), 0);
    }
}

/// OWN-F04-L/F05-I：保留所有适配函数真实签名，TaskId先挂起后由另一线程重复唤醒。
#[test]
fn test_local_adapter_all_routes_and_task_id_pending_external_wakeup() {
    let _clock = startup_global_time_loop(1);
    let runner = SingleTaskRunner::<()>::default();
    let rt = runner.startup().unwrap();
    let local = rt.to_local_runtime();
    let results = Arc::new(Mutex::new(Vec::new()));
    local.spawn_local(record(results.clone(), 1)).unwrap();
    local.sapwn_timing_func(record(results.clone(), 2), 1).unwrap();
    let output = results.clone();
    let timer = local.timeout(0);
    local.spawn(async move { timer.await; output.lock().unwrap().push(3); }).unwrap();
    let id = rt.alloc::<()>();
    let remote_id = id.clone();
    let pending = rt.clone();
    let ready = Arc::new(AtomicUsize::new(0));
    let ready_read = ready.clone();
    let complete = Arc::new(AtomicUsize::new(0));
    let observed = complete.clone();
    rt.spawn(async move {
        poll_fn(move |cx| {
            if ready_read.load(Ordering::Acquire) == 1 { return std::task::Poll::Ready(()); }
            pending.pending::<()>(&id, cx.waker().clone())
        }).await;
        observed.store(1, Ordering::SeqCst);
    }).unwrap();
    // 三个本地适配任务完成后，再以一次 run_once 完全退出保证 pending waker 已存入。
    drive(&runner, || results.lock().unwrap().len() == 3);
    runner.run_once().unwrap();
    assert_eq!(complete.load(Ordering::SeqCst), 0);
    let remote = rt.clone();
    thread::spawn(move || {
        ready.store(1, Ordering::Release);
        remote.wakeup::<()>(&remote_id);
        remote.wakeup::<()>(&remote_id);
    }).join().unwrap();
    drive(&runner, || complete.load(Ordering::SeqCst) == 1 && rt.len() == 0 && rt.wait_len() == 0);
    results.lock().unwrap().sort_unstable();
    assert_eq!(&*results.lock().unwrap(), &[1, 2, 3]);
}

#[cfg(feature = "serial")]
/// OWN-F11-N：Rc仅在owner创建/访问/释放，不利用serial历史unsafe边界跨线程搬运它。
#[test]
fn test_serial_non_send_value_created_polled_and_dropped_only_on_owner() {
    use std::{cell::Cell, rc::Rc};
    let runner = SingleTaskRunner::<()>::default();
    let rt = runner.startup().unwrap();
    let calls = Rc::new(Cell::new(0));
    let local = calls.clone();
    let inner = rt.clone();
    let owner = thread::current().id();
    rt.spawn_local(async move {
        let value = AsyncValue::new();
        value.clone().set(Rc::new(7));
        inner.yield_now().await;
        let result = value.await;
        assert_eq!(thread::current().id(), owner);
        local.set(*result);
        drop(result);
    }).unwrap();
    drive(&runner, || calls.get() == 7);
    assert_eq!(Rc::strong_count(&calls), 1);
}

#[cfg(feature = "serial")]
/// OWN-F11：选用真实的另一生产池，证明泛型P不会被内置池的Any分支接管。
#[test]
fn test_serial_real_custom_local_pool_keeps_original_contract() {
    use pi_async_rt::rt::serial_local_compatible_wasm_runtime::LocalTaskPool;
    let pool = LocalTaskPool::<()>::default();
    let old_id = pool.get_thread_id() >> 32;
    let runner = SingleTaskRunner::new(pool);
    let rt = runner.startup().unwrap();
    assert_eq!(rt.get_id(), old_id);
    let done = Arc::new(AtomicUsize::new(0));
    let output = done.clone();
    rt.spawn(async move { output.store(1, Ordering::SeqCst); }).unwrap();
    runner.run_once().unwrap();
    assert_eq!(done.load(Ordering::SeqCst), 1);
    assert_eq!(rt.len(), 0);
}
