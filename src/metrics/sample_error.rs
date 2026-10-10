//! 采样错误纯值层：仅描述一次操作未得到成功结果的类别。
//!
//! 边界 ID：MET-DATA-SAMPLE-001；完整合同及三图见
//! docs/RUNTIME_METRICS_SAMPLE_RESULT_DESIGN.md，验证见 MET-TEST-SAMPLE-ERROR-001。
//! 本层不检测错误、不实现采样、截止、请求占位、回复或取消协议。

/// 一次指标采样未得到成功结果的三个无字段类别，仅供私有metrics子树消费。
///
/// # 接口与使用边界
///
/// 枚举项无入参或载荷，构造后按值保存类别；用完整match判别，与
/// `Result<RuntimeMetricCounts, RuntimeMetricSampleError>` 组合时，Err不含成功读数。
/// 零读数、缺少wait_len接口的None及尚未结束的等待都不是这些错误。
/// 相等只表示同一类别，不证明同一核心、请求、采样周期或重试优先级。
///
/// 以下只是已有操作事实的纯值表示，不是错误检测或回复协议：
/// ```text
/// let result: Result<RuntimeMetricCounts, RuntimeMetricSampleError> =
///     Err(RuntimeMetricSampleError::ReplyClosed);
/// match result {
///     Ok(counts) => { let _ = (counts.len(), counts.wait_len()); }
///     Err(RuntimeMetricSampleError::RequestInFlight) => {}
///     Err(RuntimeMetricSampleError::DeadlineExceeded) => {}
///     Err(RuntimeMetricSampleError::ReplyClosed) => {}
/// }
/// ```
///
/// # 生命周期、安全与成本
///
/// 仅拥有类别，无借用、runtime、请求、通道、Waker、名称、锁或原子；没有自定义
/// Drop、回调、I/O、等待、await、取消、清占位或关闭副作用。构造、match、复制、
/// Clone及比较均为O(1)时间／空间的纯值操作，无内在堆分配或引用计数。
/// 相同值重复复制或比较保持类别不变，不合并操作，也不保证实际采样有进度。
/// 自动满足Send／Sync／'static，不依赖runtime、全局时钟、V8或FFI环境；不新增unsafe，
/// 不允许据此跨线程查询或析构具体来源。纯值可重入消费，无阻塞或死锁机制。
/// 不提供默认错误、格式化、自定义转换、固有方法或固定布局／ABI；没有非法输入
/// 或panic分支。安全与复杂度结论仅限本值，不是未来失败检测及传输成本的保证。
///
/// 生产边界、图节点和完整合同：MET-DATA-SAMPLE-001及MET-GRAPH-SAMPLE-01～03。
/// 验证入口：本文件四项tests保护MET-T-SAMPLE-ERROR-01～04；
/// tests/runtime_metric_sample_error_contract.rs保护05～07；实际库装配及范围审查保护08～09。
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RuntimeMetricSampleError {
    /// 同一真实核心仍有尚未真正退出的采样请求，本次未取得新请求许可。
    /// 协议消费者应直接结束本次尝试，不等待占位或再投递；本值不负责落实同步，
    /// 也不能因超时、注销或重注册而清除旧请求的占位。旧任务仍可能完成。
    RequestInFlight,
    /// 已达到对应操作约定的截止失败条件，仍无可返回的成功结果。
    /// 不推断运行时关闭、死锁或线程退出，不取消排队任务或重置尚存请求占位；
    /// 时钟、截止参数及回复竞态优先序由后续协议定义，不由这个类别决定。
    DeadlineExceeded,
    /// 仍在等待的接收侧实际观察到回复通道失败，没有取得本次回复值。
    /// 不推断运行时close或用户取消；成功发送后Sender的正常Drop不是此错误。
    /// 等待方自身Drop后没有可接收此返回值的调用方，取消清理由后续协议负责。
    ReplyClosed,
}

#[cfg(test)]
mod tests {
    use super::RuntimeMetricSampleError;
    use super::super::counts::RuntimeMetricCounts;

    const VALUES: [usize; 5] = [0, 1, 17, usize::MAX - 1, usize::MAX];
    const WAIT_VALUES: [Option<usize>; 6] = [
        None, Some(0), Some(1), Some(17), Some(usize::MAX - 1), Some(usize::MAX),
    ];
    const ERRORS: [RuntimeMetricSampleError; 3] = [
        RuntimeMetricSampleError::RequestInFlight,
        RuntimeMetricSampleError::DeadlineExceeded,
        RuntimeMetricSampleError::ReplyClosed,
    ];

    /// MET-T-SAMPLE-ERROR-01：穷尽真实三项及九组有序比较，索引只是独立测试依据。
    /// 不使用通配分支，新增类别必须重新对齐；相等不表示同一请求或严重程度。
    #[test]
    fn test_error_variants_and_exact_equality() {
        for (index, error) in ERRORS.iter().enumerate() {
            let observed = match error {
                RuntimeMetricSampleError::RequestInFlight => 0,
                RuntimeMetricSampleError::DeadlineExceeded => 1,
                RuntimeMetricSampleError::ReplyClosed => 2,
            };
            assert_eq!(observed, index);
            for (other_index, other) in ERRORS.iter().enumerate() {
                assert_eq!(error == other, index == other_index);
                assert_eq!(error != other, index != other_index);
            }
        }
    }

    /// MET-T-SAMPLE-ERROR-02：真实Counts和标准Result的30种成功输入及全部错误类别。
    /// 零、None、Some(0)、MAX-1和MAX必须原样保存；不是错误检测或模拟采样测试。
    #[test]
    fn test_result_branches_preserve_counts_and_errors() {
        for len in VALUES {
            for wait_len in WAIT_VALUES {
                let result: Result<RuntimeMetricCounts, RuntimeMetricSampleError> =
                    Ok(RuntimeMetricCounts::new(len, wait_len));
                match result {
                    Ok(counts) => assert_eq!((counts.len(), counts.wait_len()), (len, wait_len)),
                    Err(_) => panic!("成功数字不得变成错误"),
                }
            }
        }
        for error in ERRORS {
            let result: Result<RuntimeMetricCounts, RuntimeMetricSampleError> = Err(error);
            match result {
                Err(returned) => assert!(returned == error, "错误类别不得被替换"),
                Ok(_) => panic!("错误不得提供伪造的成功读数"),
            }
        }
    }

    /// MET-T-SAMPLE-ERROR-03：三个错误及33种结果的真实Copy、Clone与独立拥有。
    /// 替换局部副本不修改原值，原局部退出后返回值可读取；不补setter或请求资源。
    #[test]
    fn test_copy_clone_preserve_original_results() {
        for error in ERRORS {
            let original = error;
            let copied = original;
            let mut cloned = Clone::clone(&original);
            assert!(copied == original && cloned == original);
            cloned = match error {
                RuntimeMetricSampleError::RequestInFlight => RuntimeMetricSampleError::ReplyClosed,
                RuntimeMetricSampleError::DeadlineExceeded => RuntimeMetricSampleError::RequestInFlight,
                RuntimeMetricSampleError::ReplyClosed => RuntimeMetricSampleError::DeadlineExceeded,
            };
            assert!(cloned != original && copied == error && original == error);
            let escaped = { let local = original; local };
            assert!(escaped == error);

            let original: Result<RuntimeMetricCounts, RuntimeMetricSampleError> = Err(error);
            let copied = original;
            let mut cloned = Clone::clone(&original);
            assert!(copied == original && cloned == original);
            cloned = Ok(RuntimeMetricCounts::new(0, None));
            assert!(cloned != original && copied == original);
            let escaped = { let local = original; local };
            for result in [original, copied, escaped] {
                match result {
                    Err(returned) => assert!(returned == error),
                    Ok(_) => panic!("复制不得改变失败分支"),
                }
            }
        }
        for len in VALUES {
            for wait_len in WAIT_VALUES {
                let original: Result<RuntimeMetricCounts, RuntimeMetricSampleError> =
                    Ok(RuntimeMetricCounts::new(len, wait_len));
                let copied = original;
                let cloned = Clone::clone(&original);
                assert!(copied == original && cloned == original);
                let mut replaced = copied;
                assert!(replaced == original);
                replaced = Err(RuntimeMetricSampleError::RequestInFlight);
                assert!(replaced != original && copied == original);
                let escaped = { let local = original; local };
                for result in [original, copied, cloned, escaped] {
                    match result {
                        Ok(counts) => assert_eq!((counts.len(), counts.wait_len()), (len, wait_len)),
                        Err(_) => panic!("复制不得改变成功分支"),
                    }
                }
            }
        }
    }

    /// MET-T-SAMPLE-ERROR-04：真实Error和Result的编译属性、三个常量项及无析构资源。
    /// 不固定布局或ABI，不要求派生方法const，也不认证来源、运行时或V8析构安全。
    #[test]
    fn test_pure_error_and_result_traits_and_no_drop() {
        fn require_traits<T: Copy + Clone + PartialEq + Eq + Send + Sync + 'static>() {}
        require_traits::<RuntimeMetricSampleError>();
        require_traits::<Result<RuntimeMetricCounts, RuntimeMetricSampleError>>();
        assert!(!std::mem::needs_drop::<RuntimeMetricSampleError>());
        assert!(!std::mem::needs_drop::<Result<RuntimeMetricCounts, RuntimeMetricSampleError>>());
        assert!(matches!(ERRORS[0], RuntimeMetricSampleError::RequestInFlight));
        assert!(matches!(ERRORS[1], RuntimeMetricSampleError::DeadlineExceeded));
        assert!(matches!(ERRORS[2], RuntimeMetricSampleError::ReplyClosed));
    }
}
