//! 运行时默认选项（runtime default options）只组织配置（configuration），不安装时钟（clock）。
use pi_async_runtime::{RuntimeOptions, TimerDelivery};
use std::{num::NonZeroUsize, time::Duration};

/// 验证 RuntimeOptions::default 组合四份原默认配置（default configurations），普通与空闲来源（sources）可独立修改。
/// 全局时钟（global clock）在本测试进程（test process）前后均未安装；不构造运行时（runtime）、队列（queue）或线程（thread）。
#[test]
fn test_runtime_defaults_keep_independent_sources_and_no_clock_installation() {
    assert!(pi_clock_manual::global::try_now().is_err());
    let mut options = RuntimeOptions::default();
    assert_eq!(options.external_queue.cleanup_batch_size.get(), 64);
    assert_eq!(options.external_queue.push_attempts.get(), 3);
    assert_eq!(options.idle_queue.cleanup_batch_size.get(), 64);
    assert_eq!(options.idle_queue.push_attempts.get(), 3);
    assert_eq!(options.timer.work_batch_size.get(), 64);
    assert_eq!(options.timer.advance_ticks.get(), 64);
    assert_eq!(options.timer.atomic_attempts.get(), 3);
    assert_eq!(options.executor.maintenance_batch.get(), 64);
    assert_eq!(options.executor.would_block_retries, 2);
    assert_eq!(options.executor.timer_delivery, TimerDelivery::All);
    assert_eq!(options.executor.max_sleep, Duration::from_millis(10));
    options.executor.validate().unwrap();
    options.external_queue.cleanup_batch_size = NonZeroUsize::new(1).unwrap();
    assert_eq!(options.idle_queue.cleanup_batch_size.get(), 64);
    let other = RuntimeOptions::default();
    assert_eq!(other.external_queue.cleanup_batch_size.get(), 64);
    assert!(pi_clock_manual::global::try_now().is_err());
}
