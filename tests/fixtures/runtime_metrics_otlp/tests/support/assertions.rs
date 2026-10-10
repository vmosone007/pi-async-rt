//! OTLP-ASSERT：只读核验官方已解码报文，不调用SDK控制接口、runtime或生产采集。
//!
//! 期望来自场景独立建立的任务数和冻结协议，而不是实际Report或实际报文的复制。
//! 不依赖属性／指标／数据点顺序；必须核验全集及唯一性，避免只查到一个正确点。

use opentelemetry_proto::tonic::{collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::any_value, metrics::v1::{metric, number_data_point, NumberDataPoint}};
use std::collections::{BTreeMap, BTreeSet};

const LEN: &str = "pi_async_rt.runtime.len";
const WAIT: &str = "pi_async_rt.runtime.wait_len";
const STATE: &str = "pi_async_rt.runtime.sample_state";

/// 独立期望的只读输入，不含生产控制权限；退休实例的len可以是SDK累计保存的旧值。
pub(super) struct ExpectedInstance {
    pub id: u64,
    pub name: &'static str,
    pub len: i64,
    pub wait_len: Option<i64>,
    pub state: i64,
}

/// 验证一份真实导出中的完整指标、属性、类型、值、身份和数量，错误直接使测试失败。
///
/// request只借用且不修改；process是本例已初始化的完整进程标识，instances是独立
/// 期望全集，必须ID唯一且位于正i64范围。O(P log P)时间／O(P)辅助空间，P为点数。
/// 纯读取、无阻塞／锁／线程／unsafe，不验证生产远端保留配置或SDK性能。
pub(super) fn assert_export(request: &ExportMetricsServiceRequest, process: &str,
    instances: &[ExpectedInstance])
{
    assert_eq!(request.resource_metrics.len(), 1, "本例只有一个SDK资源");
    let resource = &request.resource_metrics[0];
    assert_eq!(resource.scope_metrics.len(), 1, "不得混入其它仪器作用域");
    let scope_metrics = &resource.scope_metrics[0];
    let scope = scope_metrics.scope.as_ref().expect("必须保留仪器作用域");
    assert_eq!(scope.name, "pi_async_rt");
    assert_eq!(scope.version, "0.5.13");
    assert!(scope.attributes.is_empty(), "进程身份必须逐点携带，不转移到作用域属性");
    assert_eq!(scope_metrics.metrics.len(), 3, "恰好三个共享Gauge");
    let mut expected = BTreeMap::new();
    let mut ids = BTreeSet::new();
    for instance in instances {
        assert!(instance.id > 0 && instance.id <= i64::MAX as u64);
        assert!(ids.insert(instance.id), "独立期望也不得含重复ID");
        expected.insert((LEN, instance.id), (instance.name, instance.len));
        if let Some(wait_len) = instance.wait_len {
            expected.insert((WAIT, instance.id), (instance.name, wait_len));
        }
        expected.insert((STATE, instance.id), (instance.name, instance.state));
    }
    let mut metric_names = BTreeSet::new();
    let mut actual = BTreeMap::new();
    for metric in &scope_metrics.metrics {
        assert!(metric_names.insert(metric.name.as_str()), "仪器名称不得重复");
        let (unit, description) = match metric.name.as_str() {
            LEN => ("{task}", "已登记运行时原任务池待轮询数量"),
            WAIT => ("{task}", "已登记运行时原等待读数"),
            STATE => ("1", "运行时指标采样及记录状态码"),
            _ => panic!("出现未冻结的指标名：{}", metric.name),
        };
        assert_eq!(metric.unit, unit);
        assert_eq!(metric.description, description);
        assert!(metric.metadata.is_empty(), "不得额外增加指标元数据");
        let gauge = match metric.data.as_ref() {
            Some(metric::Data::Gauge(gauge)) => gauge,
            _ => panic!("数量和状态必须使用Gauge，不能求和或改为其它聚合"),
        };
        for point in &gauge.data_points {
            let (id, name, actual_process) = identity(point);
            assert_eq!(actual_process, process, "每点必须有完整进程标识");
            assert!(point.time_unix_nano > 0);
            assert_eq!(point.flags, 0);
            assert!(point.exemplars.is_empty());
            let value = match point.value {
                Some(number_data_point::Value::AsInt(value)) => value,
                _ => panic!("无损数量／状态必须是整数AsInt"),
            };
            assert!(value >= 0);
            assert!(actual.insert((metric.name.as_str(), id), (name, value)).is_none(),
                "同一指标同一身份不得重复数据点");
        }
    }
    assert_eq!(actual, expected, "实际报文必须与独立期望全集完全相同");
}

/// 三个唯一属性必须保持精确类型；返回借用数据，拒绝缺失／重复／额外标签和溢出系列。
fn identity(point: &NumberDataPoint) -> (u64, &str, &str) {
    assert_eq!(point.attributes.len(), 3, "每点恰好三个属性，不加类别／时间／错误标签");
    let mut attributes = BTreeMap::new();
    for attribute in &point.attributes {
        let value = attribute.value.as_ref().and_then(|value| value.value.as_ref())
            .expect("属性必须有值");
        assert!(attributes.insert(attribute.key.as_str(), value).is_none(), "属性键不得重复");
    }
    let id = match attributes.get("rt.id").copied() {
        Some(any_value::Value::IntValue(id)) if *id > 0 => *id as u64,
        _ => panic!("rt.id必须为正i64整数，不能编码为字符串或浮点"),
    };
    let name = match attributes.get("rt.name").copied() {
        Some(any_value::Value::StringValue(name)) => name.as_str(),
        _ => panic!("每点必须保留完整rt.name字符串"),
    };
    let process = match attributes.get("rt.process").copied() {
        Some(any_value::Value::StringValue(process)) => process.as_str(),
        _ => panic!("每点必须保留完整rt.process字符串"),
    };
    (id, name, process)
}
