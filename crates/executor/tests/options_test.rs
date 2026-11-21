//! 执行器选项（executor options）的默认值（defaults）与合法范围（valid range）。
//! 用例（test case）只经公开接口（public API）构造值，不安装时钟（clock）或创建线程（thread）。
use pi_async_executor::{ExecutorOptions, TimerDelivery};
use pi_clock_manual::global;
use pi_result::ErrorKind;
use std::time::Duration;

/// 验证 ExecutorOptions::default 固定为64份维护额度（maintenance budget）、两次额外重试（retries）、
/// 全部到期交付（all timer delivery）及十毫秒重查上限（recheck limit）；默认构造（default construction）不安装时钟（clock）。
#[test]
fn test_executor_options_defaults_match_public_contract() {
    assert_eq!(global::try_now().unwrap_err().current_context(), &ErrorKind::InvalidState);
    let options = ExecutorOptions::default();
    assert_eq!(options.maintenance_batch.get(), 64);
    assert_eq!(options.would_block_retries, 2);
    assert_eq!(options.timer_delivery, TimerDelivery::All);
    assert_eq!(options.max_sleep, Duration::from_millis(10));
    assert_eq!(global::try_now().unwrap_err().current_context(), &ErrorKind::InvalidState);
}

/// 验证 ExecutorOptions::validate 的闭区间（closed interval）与字段级错误（field-specific errors）。
/// 合法组合（valid combinations）包括最小／最大维护额度（maintenance budget）及两种交付方式（delivery modes）；
/// 非法输入（invalid input）保持原值，错误报告（error report）含字段（field）和实际值（actual value）。
#[test]
fn test_executor_options_validation_preserves_values_and_exact_bounds() {
    for retries in 0..=2 {
        for millis in [1, 10] {
            for batch in [1, usize::MAX] {
                for delivery in [TimerDelivery::One, TimerDelivery::All] {
                    let options = ExecutorOptions {
                        maintenance_batch: std::num::NonZeroUsize::new(batch).unwrap(),
                        would_block_retries: retries,
                        timer_delivery: delivery,
                        max_sleep: Duration::from_millis(millis),
                    };
                    options.validate().unwrap();
                    assert_eq!(options.maintenance_batch.get(), batch);
                    assert_eq!(options.would_block_retries, retries);
                    assert_eq!(options.timer_delivery, delivery);
                    assert_eq!(options.max_sleep, Duration::from_millis(millis));
                }
            }
        }
    }
    for retries in [3, u8::MAX] {
        let options = ExecutorOptions { would_block_retries: retries, ..ExecutorOptions::default() };
        let error = options.validate().unwrap_err();
        assert_eq!(error.current_context(), &ErrorKind::InvalidInput);
        let diagnostic = format!("{error:?}");
        assert!(diagnostic.contains("would_block_retries"));
        assert!(diagnostic.contains(&retries.to_string()));
        assert_eq!(options.would_block_retries, retries);
    }
    for max_sleep in [Duration::ZERO, Duration::from_nanos(999_999), Duration::from_nanos(10_000_001), Duration::MAX] {
        let options = ExecutorOptions { max_sleep, ..ExecutorOptions::default() };
        let error = options.validate().unwrap_err();
        assert_eq!(error.current_context(), &ErrorKind::InvalidInput);
        let diagnostic = format!("{error:?}");
        assert!(diagnostic.contains("max_sleep"));
        assert!(diagnostic.contains(&format!("{max_sleep:?}")));
        assert_eq!(options.max_sleep, max_sleep);
    }
    assert_eq!(global::try_now().unwrap_err().current_context(), &ErrorKind::InvalidState);
}
