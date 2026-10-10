//! 成功读数纯值层：仅表示原 len 和可提供的原 wait_len 数字。
//!
//! 边界 ID：MET-DATA-COUNT-001；正式合同及图谱见
//! docs/RUNTIME_METRICS_COUNT_VALUE_DESIGN.md，验证见 MET-TEST-COUNT-001。
//! 本层不获取运行时状态、不保存运行时句柄，也不定义采样失败或生命周期协议。

/// 一次成功观测取得的原任务池数字和可提供的原等待读数。
///
/// # 职责与权限
///
/// 仅供私有 `metrics` 子树使用，字段不可直接向消费者开放。本类型不读取
/// `AsyncRuntime`，不刷新或修正数字；两个数字也不是全局同一瞬间的原子快照。
/// `wait_len=None` 仅表示源确实没有该接口，不能表示未采样、超时、取消或错误。
/// 即使定时器为空、配置关闭或运行时停止，只要原接口存在并合法返回数字，
/// 仍应保留 `Some(value)`，其中 `Some(0)` 与 `None` 严格不同。
///
/// # 生命周期、安全与成本
///
/// 只拥有 `usize` 和 `Option<usize>`，自动满足 `Send`／`Sync`，无借用、资源、
/// 自定义析构、锁、原子、等待或用户回调。复制、Clone、比较及固有接口均为
/// O(1) 时间／空间，无内在堆分配和调度副作用，不依赖运行时、线程或 V8 环境。
/// 持有副本不保活源，数值相等不表示同一实例或新鲜读取；来源合法性由调用方保证。
/// 本类型没有默认值、更新接口、关闭或取消状态，也不保证固定布局或 ABI。
///
/// 示例是在合法来源已取得数字后的纯值操作，不创建或伪造运行时：
/// ```text
/// let counts = RuntimeMetricCounts::new(3, Some(0));
/// assert_eq!(counts.len(), 3);
/// assert_eq!(counts.wait_len(), Some(0));
/// ```
///
/// 验证入口：本文件 `tests` 覆盖 MET-T-COUNT-01～04／07；
/// `tests/runtime_metric_counts_contract.rs` 保护 MET-T-COUNT-05／08 的真实编译边界。
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct RuntimeMetricCounts {
    len: usize,
    wait_len: Option<usize>,
}

impl RuntimeMetricCounts {
    /// API-MET-COUNT-NEW：原样保存两项已取得的成功数字，返回独立拥有的纯值。
    ///
    /// `len` 及 `Some` 内的值允许完整 `usize` 范围；`None` 的语义继承类型合同。
    /// 不做算术、归一化、来源验证或 getter 调用；相同输入重复构造得到相同字段，
    /// 但不合并对象。纯函数、无错误／panic 分支、无副作用或阻塞，O(1) 时间／空间。
    /// `const` 仅允许常量表达式，不承诺实际调用必被内联或消除。
    /// 验证：MET-T-COUNT-01／02／07。
    pub(super) const fn new(len: usize, wait_len: Option<usize>) -> Self {
        Self { len, wait_len }
    }

    /// API-MET-COUNT-LEN：借用本值，按值返回已保存的任务池数字，不重新采样。
    ///
    /// 返回值不借用 `self`；零不表示所有 Future 已完成。与另一访问器任意次序
    /// 重复调用都不改变状态。继承类型的纯值安全／成本合同，无副作用、错误或等待。
    /// 验证：MET-T-COUNT-01／03／07。
    pub(super) const fn len(&self) -> usize {
        self.len
    }

    /// API-MET-COUNT-WAIT-LEN：借用本值，原样返回可选数字，不重新调用 wait_len。
    ///
    /// 返回值不借用 `self`；保留 None、Some(0) 和 Some(usize::MAX)，不替换默认值。
    /// 重复读取幂等，继承类型的纯值安全／成本合同，无副作用、错误、阻塞或 await。
    /// 验证：MET-T-COUNT-01／02／03／07。
    pub(super) const fn wait_len(&self) -> Option<usize> {
        self.wait_len
    }
}

#[cfg(test)]
mod tests {
    use super::RuntimeMetricCounts;

    const VALUES: [usize; 5] = [0, 1, 17, usize::MAX - 1, usize::MAX];
    const WAIT_VALUES: [Option<usize>; 6] = [
        None, Some(0), Some(1), Some(17), Some(usize::MAX - 1), Some(usize::MAX),
    ];

    /// MET-T-COUNT-01：真实构造器与两个访问器的 30 组独立输入对照。
    /// 同时核验保存值，防止字段交换在构造与读取中相互补偿；不测试真实采样。
    #[test]
    fn test_input_matrix_preserves_each_metric() {
        for len in VALUES {
            for wait_len in WAIT_VALUES {
                let counts = RuntimeMetricCounts::new(len, wait_len);
                assert_eq!((counts.len, counts.wait_len), (len, wait_len));
                assert_eq!((counts.len(), counts.wait_len()), (len, wait_len));
            }
        }
    }

    /// MET-T-COUNT-02：900 组比较以独立输入元组为依据，保护两个字段的完整相等。
    /// 不用读数相等推导来源身份；None 和 Some(0) 必须不同，不依赖 Debug。
    #[test]
    fn test_equality_matches_independent_input_pairs() {
        for left_len in VALUES {
            for left_wait in WAIT_VALUES {
                for right_len in VALUES {
                    for right_wait in WAIT_VALUES {
                        let left = RuntimeMetricCounts::new(left_len, left_wait);
                        let right = RuntimeMetricCounts::new(right_len, right_wait);
                        let expected = (left_len, left_wait) == (right_len, right_wait);
                        assert_eq!(left == right, expected);
                        assert_eq!(left != right, !expected);
                    }
                }
            }
        }
    }

    /// MET-T-COUNT-03：复制与 Clone 都操作真实纯值，替换局部副本不改变原值。
    /// 只重新赋值测试变量，不添加生产 setter 或任何运行时引用。
    #[test]
    fn test_copy_and_clone_keep_independent_values() {
        for len in VALUES {
            for wait_len in WAIT_VALUES {
                let original = RuntimeMetricCounts::new(len, wait_len);
                let copied = original;
                let mut cloned = Clone::clone(&original);
                assert!(copied == original && cloned == original);
                cloned = RuntimeMetricCounts::new(len, if wait_len.is_none() { Some(0) } else { None });
                assert!(cloned != original);
                assert_eq!((original.len(), original.wait_len()), (len, wait_len));
                assert_eq!((copied.len(), copied.wait_len()), (len, wait_len));
            }
        }
    }

    /// MET-T-COUNT-03／07：两种调用顺序与重复调用都返回同一保存数字，不推进状态。
    #[test]
    fn test_accessors_repeat_in_either_order() {
        for len in VALUES {
            for wait_len in WAIT_VALUES {
                let counts = RuntimeMetricCounts::new(len, wait_len);
                for _ in 0..4 {
                    assert_eq!((counts.len(), counts.wait_len()), (len, wait_len));
                    assert_eq!((counts.wait_len(), counts.len()), (wait_len, len));
                }
            }
        }
    }

    /// MET-T-COUNT-07：真实常量表达式覆盖两个标量极值及 Option 的各类边界。
    /// 仅构造和访问是 const，不要求标准派生 trait 的调用也是 const。
    #[test]
    fn test_const_construction_and_accessors() {
        const COUNTS: [RuntimeMetricCounts; 6] = [
            RuntimeMetricCounts::new(0, None),
            RuntimeMetricCounts::new(usize::MAX, None),
            RuntimeMetricCounts::new(0, Some(0)),
            RuntimeMetricCounts::new(usize::MAX, Some(0)),
            RuntimeMetricCounts::new(0, Some(usize::MAX)),
            RuntimeMetricCounts::new(usize::MAX, Some(usize::MAX)),
        ];
        const READ: [(usize, Option<usize>); 6] = [
            (COUNTS[0].len(), COUNTS[0].wait_len()),
            (COUNTS[1].len(), COUNTS[1].wait_len()),
            (COUNTS[2].len(), COUNTS[2].wait_len()),
            (COUNTS[3].len(), COUNTS[3].wait_len()),
            (COUNTS[4].len(), COUNTS[4].wait_len()),
            (COUNTS[5].len(), COUNTS[5].wait_len()),
        ];
        assert_eq!(READ, [
            (0, None), (usize::MAX, None), (0, Some(0)), (usize::MAX, Some(0)),
            (0, Some(usize::MAX)), (usize::MAX, Some(usize::MAX)),
        ]);
    }

    /// MET-T-COUNT-04：编译核验真实类型的自动线程／所有权属性及纯值派生。
    /// needs_drop 不取代源码审查，也不认证指标源、运行时或 V8 的析构域。
    #[test]
    fn test_pure_value_traits_and_no_drop() {
        fn require_traits<T: Copy + Clone + PartialEq + Eq + Send + Sync + 'static>() {}
        require_traits::<RuntimeMetricCounts>();
        assert!(!std::mem::needs_drop::<RuntimeMetricCounts>());
    }

    /// MET-T-COUNT-07：源值离开作用域后返回标量仍独立可用，不保留内部借用。
    #[test]
    fn test_returned_numbers_outlive_value_scope() {
        for len in VALUES {
            for wait_len in WAIT_VALUES {
                let returned = {
                    let counts = RuntimeMetricCounts::new(len, wait_len);
                    (counts.len(), counts.wait_len())
                };
                assert_eq!(returned, (len, wait_len));
            }
        }
    }
}
