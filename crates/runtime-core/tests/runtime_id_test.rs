//! 运行时身份（runtime identity）的进程内唯一性（process-local uniqueness）。
//! 仅经 RuntimeId::try_new 取得身份（identity），不创建运行时（runtime）或修改编号计数器（identifier counter）。
use pi_async_runtime_core::RuntimeId;
use std::collections::BTreeSet;

/// 验证顺序及两名并发分配者（concurrent allocators）的非零且不复用身份（nonzero non-reused identities）。
/// 每名分配者（allocator）仅取八项，属于有界功能场景（bounded functional scenario），不作为压力（stress）或性能（performance）证明。
/// u64耗尽（exhaustion）无法从公开入口（public API）有限执行，保留受检加法（checked addition）的静态核验（static verification）。
#[test]
fn test_runtime_id_is_nonzero_unique_and_stable_across_threads() {
    let first = RuntimeId::try_new().unwrap();
    let copied = first;
    assert_ne!(first.get(), 0);
    assert_eq!(first, copied);
    assert_eq!(first.get(), copied.get());
    let second = RuntimeId::try_new().unwrap();
    assert!(second.get() > first.get());
    let allocated = std::thread::scope(|scope| {
        let one = scope.spawn(|| (0..8).map(|_| RuntimeId::try_new().unwrap().get()).collect::<Vec<_>>());
        let two = scope.spawn(|| (0..8).map(|_| RuntimeId::try_new().unwrap().get()).collect::<Vec<_>>());
        let mut values = one.join().unwrap();
        values.extend(two.join().unwrap());
        values
    });
    let mut seen = BTreeSet::from([first.get(), second.get()]);
    for value in allocated {
        assert!(value > second.get());
        assert!(seen.insert(value));
    }
    assert_eq!(seen.len(), 18);
    let after = RuntimeId::try_new().unwrap();
    assert!(after.get() > *seen.last().unwrap());
}
