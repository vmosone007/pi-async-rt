基于Future(MVP)，用于为外部提供基础的通用异步运行时和工具

# 主要特征

- 任务池: 可定制的任务池
- 任务ID: 外部使用任务ID可以很方便的唤醒和挂起
- 抽象接口: 可以自由实现自己的运行时
- 运行时推动：单线程运行时可以用自己的方式推动运行

# Examples

本地异步运行时:
```
 use pi_async_rt::rt::{AsyncRuntime, AsyncRuntimeExt, serial_local_thread::{LocalTaskRunner, LocalTaskRuntime}};
 let rt = LocalTaskRunner::<()>::new().into_local();
 let _ = rt.block_on(async move {});
```

多线程异步运行时使用:
```
 use pi_async_rt::rt::{AsyncRuntime, AsyncRuntimeExt};
 use pi_async_rt::rt::multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder, StealableTaskPool};

 let pool = StealableTaskPool::with(4,100000,[1, 254],3000);
 let builer = MultiTaskRuntimeBuilder::new(pool)
     .set_timer_interval(1)
     .init_worker_size(4)
     .set_worker_limit(4, 4);
 let rt = builer.build();
 let _ = rt.spawn(async move {});
```

<a id="runtime-metrics-progress-contract"></a>
<a id="runtime-metrics"></a>

# 全局运行时指标：显式注册、采集和上报

本功能为当前库副本中**已显式命名注册**的运行时提供原 `len()`／`wait_len()`
及采样状态；默认构造、`Clone`、启动和任务提交不会自动注册。不新增
`pending_len`，离开任务池的 Pending Future 不计入队列数量，也不按线程扫描补算。

基础单名称功能在原 0.5.13 装配下已完成相关标准回归、真实 SDK／OTLP、风险聚焦
TSan、小规模性能基线、严格作者终审和七方标准验收。既有 `AsyncRuntime`、`spawn`、`timeout`、
poll／wake／timer／close 的签名及正常语义保持；不是调度器重构。每个核心增加
冷身份槽，业务热路径和每任务本体没有新增指标计数／查表／分配操作；不能据此
承诺绝对零布局或性能影响。根依赖、feature 和版本号未改。

**当前扩展状态：** 工作树版本 0.5.16 已完成“一份核心身份、多个观测名称、每轮采样
一次、每个适用指标只记录一次”的扩展，以及宿主标记追加 PID。Linux x86_64 的
四配置 Debug 相关回归198次、指标文档12次及真实 SDK／OTLP 标准测试1次通过，
严格作者终审和七方标准验收通过。按本轮明确约定，不执行压力、基准及其它 Release
验证，不把历史性能数字当本轮结论。未发布工作树不代表此前发布的同版本产物已包含
修改，下游须核对实际使用的源码／发布版本。

原运行时 API 和调度流程未改；指标 API 新增只读 `names()`，原函数签名及 `name()`
首名保持。同核异名现在追加成功，`rt.name` 改为合并展示名、`rt.process` 改为宿主
标记加PID，是已明确约定的指标语义变化，使用指标的查询／告警应同步适配。

## 接口和支持范围

统一从 `pi_async_rt::runtime_metrics` 导入：

| 新公开入口 | 作用 |
| --- | --- |
| `register_runtime_metrics(&runtime, name)` | 必传名称，成功返回不透明 `RuntimeMetricId` 并强保活真实核心 |
| `unregister_runtime_metrics(id)` | 撤销活动登记，首次 `true`，未知或重复 `false`；不关闭或取消运行时 |
| `init_runtime_metrics_reporting(process_instance)` | 复用宿主已安装的 MeterProvider，一次初始化三 Gauge |
| `collect_and_report_runtime_metrics(options)` | 同一全局同步函数完成采样及 SDK record，返回逐实例只读报告 |

`RuntimeMetricRegistrable` 是本库新增的封闭登记能力，其 `register_metrics` 方法
与注册函数同合同；它不是原 `AsyncRuntime`，不限制原 trait 的外部实现。

| 具体运行时 | 注册／采样边界 |
| --- | --- |
| `rt::single_thread::SingleTaskRuntime<O, P>` | 支持，直接读取原线程安全 getter，`wait_len=Some(...)` |
| `rt::multi_thread::MultiTaskRuntime<O, P>` | 支持，同上，含可窃取池／计算型池；保留原 getter 数值与语义 |
| `rt::worker_thread::WorkerRuntime<O, P>` | 支持，与内部 Single 共享真实核心身份 |
| 原生 `rt::serial_local_thread::LocalTaskRuntime<O>` | 支持，经原 `send` 在原 owner 线程读 `len`；没有 `wait_len` 接口，返回 `None` |
| `serial_single_thread`／`serial_worker_thread` | 新注册返回 `ThreadBoundRuntimeRequiresOwnerBridge`，旧业务 API 不变 |
| `LocalAsyncRuntime` 原始门面／兼容 WASM 本地类型 | 不接入，不能作为注册参数 |
| `wasm32` | 暂不提供新增公开指标模块 |

开启 `serial` 不会把上表普通模块自动变为 serial 实现，但 prelude 可能选择
serial 类型；要按实际具体类型判断，不能仅凭类型短名称。不同输出 `O`／任务池
`P` 仍支持，自定义池继续遵守原线程安全、getter 和生命周期合同。

登记／注销不要求 `trace`；实际初始化和采集上报要求 `features = ["trace"]`。
未开 `trace` 时后二者优先返回 `ReportingDisabled`，不投递采样任务。
此时已成功登记的强引用仍存在，仍须注销，不要把“未上报”误当“未保活”。

## 身份、名称、容量和所有权

- 名称及传入初始化函数的宿主进程标记都必须为 1～256 个 UTF-8 字节、非纯空白且无
  控制字符。名称保留原文，不裁剪、不截断、不按字符数限制；追加 PID 后的
  `rt.process` 可以超过256字节，详见下节。
- 同一真实核心及其 Clone／Worker 包装共享 ID；同核心同名重复登记幂等，不同名称
  按首次成功发布顺序追加，非首名重复不重排。首次名称和类别固定，报告 `name()`
  保留首名，`names()` 返回原始去重名称列表。不同核心允许同名，不能只按名称区分。
- 合并展示名以 `-` 连接，连同分隔符最多256个UTF-8字节；超长返回 `NameTooLong`，
  即使总预算也已满仍优先此错误。原名称允许 `-`，合并结果不能反解或替代 `rt.id`。
  `NameConflict` 变体及原错误文字兼容保留，合法同核异名不再返回此错误。
- ID 是正 `i64` 范围的整数，可能有失败分配留下的空洞，不复用、不回绕。
  它不是旧 runtime ID，也不是裸指针；只能通过 `as_u64()` 读取，无公开构造或反查权限。
- 每个库副本进程生命周期内，累计最多1024份不同“核心ID＋原始名称”登记预算。
  同核同名重复免费，两个别名占两份预算但只保留一个活动Source；注销不返还预算，
  已满时合法新名／新核返回 `CapacityExceeded`，失败不追加或恢复已注销核心。
  一次注销整个核心及全部名称；任一历史名登记可整体恢复全部历史名称且不另收费。
  不提供逐名称注销或订阅租约，单名称用法的1024核心容量保持。
  这限制短命实例造成的 SDK 系列增长，不限制宿主本身创建其它 runtime／线程。
- 成功登记强保活核心，表示资源不能仅靠外部句柄 Drop 自动析构，**不禁止原
  `close()`**。注销不等于关闭、取消、排空或 SDK 历史点删除。
- 多个组件给同一核心命名不产生各自的租约或持有次数；任一处按ID注销都会使全部
  观测名一起退休。建议由宿主统一协调注销，组件退出时不要误停其它组件仍需要的观测。
- ID 域属于当前链接库副本；多个 crate 版本／动态副本不共享表或编号域。
  全进程观测应明确实际注册到哪一副本，不能把同版本号当同一个全局表。

最小登记示例只使用已验证的普通 Single；不推动任务、不初始化时钟或 SDK：

```rust
use pi_async_rt::{rt::single_thread::SingleTaskRunner, runtime_metrics::*};

let runner = SingleTaskRunner::<()>::default();
let runtime = runner.startup().unwrap();
let id = register_runtime_metrics(&runtime, "gateway-io").unwrap();
assert_eq!(register_runtime_metrics(&runtime.clone(), "gateway-io").unwrap(), id);
assert!(unregister_runtime_metrics(id));
assert!(!unregister_runtime_metrics(id));
```

同一核心再登记 `"rpc"`，仍返回相同ID，原名列表是 `["gateway-io", "rpc"]`，
上报 `rt.name="gateway-io-rpc"`；`len=7` 时仍只有一个值为7的len点，不是两份7。
两个不同核心即使合并名相同，也由不同 `rt.id` 区分，不能仅以展示名作主键。

## Provider、进程标识和远端格式

顺序必须是：宿主安装有效 MeterProvider → 调用初始化函数 → 已有外部线程周期采集。
`init_runtime_metrics_reporting(process_instance)` 的参数必须由启动器／宿主显式提供，
并且**明确代表当前宿主进程的业务身份**，不是运行时名称、采集线程名称或无区分度的
固定服务名。建议组合机器身份、灰度批次、服务角色及唯一启动实例，例如
`node-a/gray-42/gateway/start-唯一启动标记`；不要包含口令、令牌等敏感信息。

本库在初始化冷路径构造并保存：

```text
rt.process = 外部宿主进程标记 + "-" + std::process::id()的十进制值
```

例如输入 `node-a/gray-42/gateway/start-abc`、本进程PID为12345，则上报属性为
`node-a/gray-42/gateway/start-abc-12345`。外部不需要自行追加PID。输入标记的上限
仍是256个UTF-8字节，拼接结果最多267字节，完整保存，不按256字节截断或再次拒绝。
不同机器、PID命名空间及进程重启可能复用PID，**追加PID不替代宿主标记的区分责任**；
启动器不能把无法区分宿主环境的常量传给所有进程。本库无法验证字符串是否真实反映业务身份。

初始化后相同输入标记幂等，不同标记 `AlreadyInitialized`；并发／重入初始化
立即 `InitializationInProgress`。默认 no-op Provider 也可能让初始化返回成功，
**成功不证明 SDK 已安装或远端可达**。初始化后更换全局 Provider 不会重绑已有 Gauge；
不能先初始化再补装 SDK，也不提供清空／重置本库全局记录器的 API。
注册运行时不会隐式初始化上报。未开启 `trace` 时优先返回 `ReportingDisabled`，不读取
PID、不拼字符串、不创建仪器。继承到fork子进程的全局状态不提供重新初始化SDK的保证。

所有注册实例共享初始化后的 `Arc<str>` 进程字符串，属性缓存共享其字节；周期采集不
重复查询PID或拼接标记。同参数重复初始化仍可能构造临时比较字符串，但不替换已经发布
的字符串或重建Gauge。这不是网络去重保证：当前OTLP转换仍为各数据点编码该属性；
SDK按完整属性组合复用聚合状态，不能因为 `rt.process` 相同就合并不同 `rt.id`。
本轮保留 `rt.id` 的运行时核心身份语义，不新增或上报 `rt.thread_id`。

Meter 作用域为 `pi_async_rt`，版本取本库包版本。三项均为 `u64` Gauge：

| 指标名 | 单位 | 数据来源／含义 |
| --- | --- | --- |
| `pi_async_rt.runtime.len` | `{task}` | 原任务池 `len()`，不是全部存活或 Pending 任务数 |
| `pi_async_rt.runtime.wait_len` | `{task}` | 原 `wait_len()`，无此接口的本地来源不发该点 |
| `pi_async_rt.runtime.sample_state` | `1` | 本次采样／记录状态码，见下表 |

每点只有三个必需标签：`rt.id`（整数）、`rt.name`（全部原名以`-`连接的完整展示名）、`rt.process`
（完整进程实例标识）。不动态拼指标名称，不附加类型／错误／时间标签；
运行时类别保留在本地报告中。SDK、View、Collector、远端存储和查询都必须保留三标签，
每项仪器有效属性容量至少 1024，不能过滤掉名称或合并不同 ID。

同一核心一轮只读取一次来源；每个适用Gauge只调用一次record，共享同一组上述属性。
不是每个名称各发一份点，也不意味着三个Gauge只调用一次或网络只发一份请求。
原始名称列表保留在本地 `names()` 中，不增加 `rt.name0` 等动态属性。

| 状态码／公开状态 | 精确含义 |
| --- | --- |
| 0 `Unregistered` | 本轮快照中的历史退休实例，未采样 |
| 1 `RecordInvoked` | 合法原数量已调用 Gauge.record，不是网络回执 |
| 2 `RequestInFlight` | 上一份本地读取任务仍在途，本次未投递 |
| 3 `DeadlineExceeded` | 观察已达截止，本次不接受读数 |
| 4 `ReplyClosed` | 截止前观察单次回复通道无值关闭 |
| 5 `RoundDeadlineSkipped` | 整轮预算耗尽，本实例未尝试 |
| 6 `MetricValueOutOfRange` | 原样本成功，但有数量超出 i64 范围；整份数量不记录 |

状态失败／退休只记录状态，**不会把数量补零或补旧值**。官方 SDK／远端可能仍保留
之前的数量点，查询数量时须结合相同三标签下的状态 1 和新鲜度，不能只看旧 Gauge。
注销本身不 record 状态 0，后续采集轮才会记录；无后续轮时不能期待自动远端清零。
状态 6 的报告仍保留原 `usize` 样本，不截断、饱和或把错误量变成合法零。

**名称变化的历史系列边界：** 最好在首次采集前完成所有稳定名称登记。后续追加新名
会在下一轮使用新合并名，正在采集的旧快照仍用旧名；SDK／远端可能同时保留旧组合，
旧组合甚至仍显示state=1。注销不删除或逐一更新所有旧组合，单靠状态过滤也不保证
清除这些历史点；查询端须按自己的历史系列策略处理，不能把同一核心不同名称系列
直接相加。`-` 本来就是合法名称字符，不能拆分合并字符串来恢复精确原始列表。

本库只在同一采集函数里同步 record，复用宿主已有 SDK／OTLP／导出周期。
不新建导出器或隐式运输线程，不逐轮 flush、不等远端 ACK，不自行重试运输。
宿主的采集周期（建议 1～3 秒）与 SDK 导出周期是两回事，可以不同。

## 同步采集线程、预算和报告

调用 `collect_and_report_runtime_metrics` 的线程必须是宿主已有、允许等待且
**不负责推进目标运行时及其依赖工作**的外部线程，例如满足这些条件的进程主线程。

禁止从 Future、worker、本地 owner 或手动 Runner 的推进线程调用；`async` 块
包住同步函数也不会把它变成非阻塞。主线程若兼任 Runner 就不合法。
调用时不能持有采样任务、getter、关闭流程或 SDK 所需的锁／资源。
库不自动识别全部线程职责，这些是使用者必须遵守的合同。

预算为不可变 `RuntimeMetricCollectionOptions`：默认单实例 100ms、整轮 3s；
`new(instance_timeout, round_timeout)` 拒绝任一零值。实例截止裁剪到整轮余量；
预算无法表示为 `Instant` 截止时，在投递前报 `DeadlineOverflow`，不退化成无限等待。
全局只允许一轮活动采集，并发／重入立即 `CollectionInProgress`，不无限排队。

结果为只读 `RuntimeMetricCollectionReport`：

- `instances()` 返回按 ID 排序的不可变切片，含本轮活动和历史退休实例。
- 每项用 `id()/name()/names()/kind()/status()/counts()` 读取，不暴露 Source、runtime、锁或原子。
  名称列表来自本报告快照，后续追加不修改旧报告；多个别名仍只占一项核心报告。
- `counts()` 为 `Option<(usize, Option<usize>)>`：外层 `None` 为未得到样本；
  内层 `None` 仅表示没有 wait 接口，不是“不可用”或数量零。
- `active_count()/retired_count()/count(status)` 为精确汇总；
  `sampled_count()` 包含状态 1 和 6，不能直接当“成功上报数量”。
- 整轮返回 `Ok(report)` 不代表所有实例成功；必须检查各项状态。
  报告不保活运行时，数字不是跨实例同一时刻的原子快照。

公开错误都是可比较的类别，并实现 `Debug/Display/std::error::Error`：
登记还可能返回 `InvalidName/NameTooLong/CapacityExceeded/IdentityExhausted`，
线程受限类型返回明确拒绝；初始化还可能返回
`ReportingDisabled/InvalidProcessInstance/ProcessInstanceTooLong`；
采集启动还可能返回 `ReportingDisabled/ReportingNotInitialized/DeadlineOverflow`。
`InvalidTimeout` 由预算构造器返回；零预算不能通过正常公开接口进入采集。

## 本地推进、截止与关停责任

原生 Local 没有可由外部线程安全读取的 owner 队列。采集经原 `send` 投递一次
任务，原 owner 按原调度构造 `O::default()`、读取原 `len()`、回填一个整数：

```text
外部采集线程：原send投递 → 容量1回复recv_deadline → 再检查截止 → 同入口record
原owner线程：按原Runner执行 → Default／原len → 唯一发送一次 → 原执行器收尾
```

已有 crossbeam-channel 的容量为 1，不用容量 0 会合通道；唯一发送端只发一次，
不因等待接收者取值占住 owner。回复等待只占调用采集函数的外部线程，
不需要额外定时器线程，也不依赖被采样 runtime 的 timer。各自控制短锁不跨
提交／接收／getter／SDK／实际资源 Drop；第三方内部仍可能使用短锁。

注册方应保证正常推进，并在关闭前推动已经投递的采样实际结束。这是软进度约定，
不是“强引用保证 worker 一定运行”。`recv_deadline` 给缺失回复提供失败出口；
观察时已达截止不接受迟到成功，整轮到期后不投递剩余实例。

超时仅结束接收，不取消已发送任务；每核心最多一个存活读取动作，
未退出时后续报告状态 2，注销／恢复也不能绕过许可。迟到发送失败正常处理。
永久停推可能继续保活旧任务／核心，不能用截止承诺资源自动回收。
用户 Default／getter／Drop、SDK 同步代码和 OS 调度不可被硬抢占，
所以预算**不是整个函数严格耗时上限**，也不是严格无锁／完全无阻塞承诺。

建议关停顺序：

1. 停止启动新采集轮，并防止目标被并发重新注册。
2. 注销目标，阻止后续新快照保留活动来源。
3. 等已开始的同步采集返回，同时让 owner／worker 继续推动工作。
4. 协调确认旧快照可能投递的采样任务实际收尾，再按原 API 关闭。

注销不撤销旧快照，旧轮仍可能 send／record。收到数字、许可释放、超时返回、
注销成功或 `len()==0` 都不单独证明所有 Future 盒子／输出已经析构；
本库未新增采样取消／join／自动排空接口。上面的顺序不要求所有长期业务 Future
都 Ready，但需要宿主自己的关停协调，不能用近似队列长度替代。

`MultiTaskRuntime` 原 `close` 不提供停止 worker，本功能不修造新的关闭能力。
线程所有权、`Send/Sync`、本地队列和 V8 资源的硬边界不因登记或超时而放宽。
用户／SDK panic 自然传播，控制 guard 在 unwind 时释放；SDK record 不是事务，
中途 panic 可能已有部分记录，后续成功轮不回滚它们；abort 不保证执行 Drop。

## 完整 SDK／OTLP 接入示例

下列完整代码与 [已离线编译的示例](tests/fixtures/runtime_metrics_otlp/examples/downstream_metrics.rs)
一致。它是独立宿主演示：**显式**创建 SDK 导出设施、一个 Tokio 运输 worker、
两个各 2 worker 的业务 runtime 及原业务时钟；不是本库注册／采集暗中创建线程。
生产宿主已有 Provider／运输／时钟／业务 runtime 时直接复用，不重复创建。

夹具 [Cargo.toml](tests/fixtures/runtime_metrics_otlp/Cargo.toml) 提供已验证的最小直接
依赖／feature／本地库路径：本地库0.5.16，官方OpenTelemetry API／SDK／OTLP／proto
0.33.1、tonic0.14.6、Tokio1.50.0及tokio-stream0.1.19；生产接入无需复制服务端解码
依赖。独立锁的207个registry包版本均与根锁一致，传递feature仍可能合并，不能据此
声称整个依赖图已最小化。真实验收仅使用回环地址；示例只编译，未访问生产服务器。
运行前须由宿主提供两个环境变量：
`PI_RUNTIME_METRICS_PROCESS_INSTANCE`（明确代表宿主的进程标记，本库统一追加PID）和
`OTEL_EXPORTER_OTLP_ENDPOINT`（实际 OTLP gRPC 地址）。
远端保留三标签和足够属性容量是运行前提；宿主网络／证书／认证按已有部署配置。

```rust
//! 下游完整接入示例的编译载体，不由普通验收测试自动运行。
//!
//! 独立运行会显式创建宿主运输／SDK设施和两个业务runtime，并连接指定OTLP服务；
//! 生产宿主已经存在这些设施时应复用，不照搬重复创建。本库指标入口不创建它们。
//! 主线程不推动这些Multi的worker，因此可以同步采集；不使用block_on。

use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use pi_async_rt::{runtime_metrics::*, rt::{AsyncRuntime, multi_thread::{MultiTaskRuntimeBuilder, StealableTaskPool}}};
use std::{error::Error, thread, time::Duration};

/// 在宿主已经进入且持续运行的Tokio上下文中构建指标管道；生产中优先复用现有Provider。
/// SDK的导出线程和网络是宿主显式选择，不是pi-async-rt注册或采集的隐式副作用。
fn provider(endpoint: &str) -> Result<SdkMeterProvider, Box<dyn Error>> {
    let exporter = MetricExporter::builder().with_tonic().with_endpoint(endpoint)
        .with_timeout(Duration::from_secs(3)).with_temporality(Temporality::Cumulative).build()?;
    let reader = PeriodicReader::builder(exporter).with_interval(Duration::from_secs(3)).build();
    Ok(SdkMeterProvider::builder().with_reader(reader).build())
}

fn main() -> Result<(), Box<dyn Error>> {
    // 宿主标记须区分机器／灰度／角色／启动实例，最多256字节；本库统一追加系统PID。
    let process_instance = std::env::var("PI_RUNTIME_METRICS_PROCESS_INSTANCE")?;
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")?;
    let transport = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build()?;
    let sdk = { let _entered = transport.enter(); provider(&endpoint)? };
    opentelemetry::global::set_meter_provider(sdk.clone());
    init_runtime_metrics_reporting(&process_instance)?;

    // 本独立进程只启动一次原业务时钟；已有宿主已启动时不可重复初始化。
    let _clock = pi_async_rt::rt::startup_global_time_loop(1).expect("独立示例唯一业务时钟");
    let network = MultiTaskRuntimeBuilder::new(StealableTaskPool::<()>::with(2, 4096, [1, 1], 3000))
        .init_worker_size(2).set_worker_limit(2, 2).set_timer_interval(1).build();
    let jobs = MultiTaskRuntimeBuilder::new(StealableTaskPool::<()>::with(2, 4096, [1, 1], 3000))
        .init_worker_size(2).set_worker_limit(2, 2).set_timer_interval(1).build();
    let network_id = register_runtime_metrics(&network, "gateway-network")?;
    let jobs_id = register_runtime_metrics(&jobs, "gateway-jobs")?;
    assert_eq!(register_runtime_metrics(&network.clone(), "gateway-network")?, network_id);
    // 同核第二名称仅扩展观测属性；上报仍每指标一个点，不产生第二份任务读数。
    assert_eq!(register_runtime_metrics(&network, "gateway-rpc")?, network_id);
    network.spawn(async {})?;
    jobs.spawn(async {})?;

    let options = RuntimeMetricCollectionOptions::new(Duration::from_millis(100), Duration::from_secs(3))?;
    // 这里只演示三轮。长驻宿主应在已有、可等待且不承担runtime推进的线程按周期调用。
    for _ in 0..3 {
        let report = collect_and_report_runtime_metrics(options)?;
        for instance in report.instances() {
            println!("运行时={} 首名={} 全部原名={:?} 类别={:?} 状态={} 原读数={:?}", instance.id().as_u64(),
                instance.name(), instance.names().collect::<Vec<_>>(), instance.kind(), instance.status().code(), instance.counts());
        }
        thread::sleep(Duration::from_secs(1));
    }

    // 已停止所有新轮／重登记，本例同步调用已返回；没有登记本地runtime或其它采样调用者。
    assert!(unregister_runtime_metrics(network_id));
    assert!(unregister_runtime_metrics(jobs_id));
    // 可选的退休状态记录仍走同一入口；不会把SDK中旧数量点清成零。
    let retired = collect_and_report_runtime_metrics(options)?;
    assert_eq!((retired.active_count(), retired.retired_count()), (0, 2));
    // 仅宿主退出时显式刷新／关闭，不每轮flush；运输上下文要保持到SDK完成关闭。
    sdk.force_flush()?;
    sdk.shutdown()?;
    transport.shutdown_timeout(Duration::from_secs(2));
    // Multi的原close不提供worker停止；独立示例退出进程，不伪称注销等于关闭worker。
    Ok(())
}
```

例如已有独立宿主可在安装 SDK、登记完成后，用**已有合法外部线程**每 1～3 秒
调用相同同步函数；不要为了采集把代码放进业务 runtime 的无限异步循环。
示例里的 `force_flush/shutdown` 只用于宿主整体退出，不在每一轮采集调用。
业务线程、SDK 导出线程及运输上下文的创建／推进／关闭始终由宿主负责。

## 成本、验证和结论边界

注册冷写 O(C+A*n+L+n)，注销 O(C)，快照借用 O(1)；C为累计核心数、A为本核名称数、
L为旧合并名字节数、n为新输入字节数，C≤1024、成功合并≤256字节。名称原文比较和
新组／合并名只在冷登记发生；增加有界名称元数据／快照复制，不是零分配注册。
一轮控制及报告 O(C)，另加原 getter／本地调度／SDK 成本；与别名数量无逐名采样关系。
预热后直接来源每轮一份报告 Vec；本地另有原任务／通道／通知成本；
借用缓存三标签，不逐轮复制名称字节或构造属性 Vec。本库不初始化全局时钟。

本轮限定Linux／Debug功能验收：四配置198次顶层测试执行、12次指标rustdoc及1次
官方SDK／OTLP标准测试全部通过，共211次成功执行，不是211个独立用例。
SDK四份真实请求每份八点，共32个精确点；完整示例离线check通过。原始时钟仅由
需要真实runtime的隔离场景显式初始化一次；生产采集入口不初始化。
根manifest／锁及原rt／identity／Source字节不变；本轮没有热路径新增工作、没有压力／
基准性能结论，Release剩余项按最新要求取消，不据已产生的部分历史记录宣称其通过。

以下为原0.5.13单名称／旧SDK的**历史基线**，不代证当前0.5.16多名版本性能：
四 feature×Debug／Release 736 次顶层测试执行、96 次 rustdoc；
SDK 两 profile 共 8 份真实请求／64 个精确点；
原生命周期两配置全链 TSan 共 16 个子场景，未报告竞争；
24 个短性能场景共 93,000 个空业务任务精确完成，父子 RSS 观测峰值约 14.05MiB。
这些是执行次数而非不同测试数，/proc 采样不是硬资源隔离。

历史仪器装配的预热后样例：真实 SDK 采集 256 个直接来源，短名平均约 80.35μs/轮、
p99 约 140.90μs；8 个本地来源平均约 557.90μs/轮、p99 约 684.56μs。
8 worker／4 并发生产者的短任务轮波动较大，不据其宣称调度提升或旧版零退化。
完整多维原始基线、严格审查、七方验收和边界图保存在本地 `docs/RUNTIME_METRICS_*`
中，按仓库约定不加入 Git。源码交付保留中文契约、标准测试和独立验证夹具；
独立夹具不是生产依赖，发布包内容另遵循 Cargo 打包规则。

标准定向入口（均需串行，SDK 夹具仅绑定回环）：

```sh
cargo test -p pi-async-rt@0.5.16 --lib metrics:: --features trace --locked --offline -j1 -- --test-threads=1
cargo test -p pi-async-rt@0.5.16 --test runtime_metrics --features trace --locked --offline -j1 -- --test-threads=1
cargo test -p pi-async-rt@0.5.16 --test runtime_metrics_contract --features trace --locked --offline -j1 -- --test-threads=1
cargo test -p pi-async-rt@0.5.16 --test runtime_metrics_boundaries --features trace --locked --offline -j1 -- --test-threads=1
cargo test -p pi-async-rt@0.5.16 --test runtime_metrics_lifecycle --features trace --locked --offline -j1 -- --test-threads=1
cargo test -p pi-async-rt@0.5.16 --test runtime_metrics_multi_name --features trace --locked --offline -j1 -- --test-threads=1
cargo test --manifest-path tests/fixtures/runtime_metrics_otlp/Cargo.toml --test acceptance --locked --offline -j1 -- --test-threads=1
```

其它Debug配置按同样边界串行执行；本轮不再进行Release、压力或基准，不从示例推定新
验证授权。Cargo编译预算与实际子场景截止须分开，不把15秒场景限额套在整个编译上。
不要裸 `cargo test` 带入旧无界／人工观测目标。
TSan 和基准是单独明确选择且有门禁的夹具，不在普通测试中自动运行；
未运行 ASan／Miri、百万／长压、Windows／aarch64／WASM／真实 V8 或生产公网验收。
不把上述证据称作全库无 UB／无泄漏认证。已归档的原 getter 计数偏差和 raw／
TaskId／serial unsafe 风险仍独立保留，本功能严格使用原 getter，不擅自修正原逻辑。

<a id="computational-default-workers"></a>

# 计算型多线程池的默认 worker 数

`ComputationalTaskPool` 按全部固定槽位分配任务，每个 worker 只消费自己的槽位。
因此把自定义容量的计算型池交给 `MultiTaskRuntimeBuilder::new(pool)` 时，默认会启动
与池槽位数相同的 worker；`init_worker_size(0)` 也按该池槽位数回退。此前默认取
物理核数加一，池槽位更多时可能有任务永远不被消费。

```rust
use pi_async_rt::rt::AsyncRuntime;
use pi_async_rt::rt::multi_thread::{ComputationalTaskPool, MultiTaskRuntimeBuilder};

let pool = ComputationalTaskPool::<()>::new(4);
let runtime = MultiTaskRuntimeBuilder::new(pool).build();
assert_eq!(runtime.worker_len(), 4);
runtime.spawn(async {}).unwrap();
```

公开 API 签名不变；只有内置计算型池的默认值和零值回退的可观察 worker 数发生
上述修正。显式正数配置仍保留原行为：`build` 会把超出池槽位的数量收敛到容量，
但不会自动补足小于容量的数量。计算型池若需保证全部任务有进度，应让实际启动
的 worker 覆盖全部槽位。可窃取池及 `default_multi_thread` 的默认物理核数加一
规则、`Some(0)` 兜底和显式正数规则均未改变。

本次只在构建期增加 O(1) 类型选择，不改变任务提交、poll、wake、timer 或 worker
循环的热路径；自定义大容量计算型池会按其容量启动更多线程，这是恢复完整进度
所需的资源成本，不代表吞吐或 CPU 占用有已测得的改善。本轮未执行基准性能测试。
专项回归入口（普通入口会串行运行其隔离子进程用例）：

```sh
cargo test -p pi-async-rt --test multi_thread_default_workers --locked --offline -j1 -- --test-threads=1
```

<a id="single-task-pool-owner"></a>

# 单线程任务池所有者修复

默认及 `serial` 的 `SingleTaskPool` 已修复外部线程误认领 owner 的并发安全问题。
原 `get_thread_id()` 会在陌生线程写入目标运行时编号，使外部 `spawn_local`、高优先级
提交或已经 Pending 的任务 Waker 错误访问无同步的本地队列。一个消费者加一个合法
外部发送者即可触发，不需要多个消费者，也不是最近 timer/多线程安全修复引入。

这是内部安全 Bug 修复，**公开函数/trait 签名、泛型约束和任务布局不变，但不是所有
可观察行为都不变**：

- 构造、`startup`、提交及查询不认领线程；首次实际消费绑定真实线程，之后不允许
  消费者迁移，即使原线程已退出。构造在 A、首次驱动在 B 仍受支持。
- 内置池的运行时 ID 来自池自身，不继承构造线程上另一个池的展示编号。
- `get_thread_id()` 只读当前线程已有 packed 编号，未初始化返回 `usize::MAX`。
  它不是授权接口，不应依赖查询副作用取得本地访问权。
- 绑定前及外部线程的 local/priority/wake 走现成的公共队列。真实 owner 保留原本地
  FIFO/栈路径，公共回退不承诺外部最高优先级抢占。
- 异线程 `run/run_once/block_on` 在访问 timer 或入队前返回 `PermissionDenied`；
  低层 `try_pop/try_pop_all` 则在接触私有容器前 panic。未启动的原错误顺序保持。
- `block_on` 的预检在提交捕获结果栈地址的任务之前，拒绝时不遗留该任务。它仍是
  同步驱动 API，不是异步等待原语，也不新增任意用户 panic 后的取消保证。

常规外部提交/回填无需修改调用代码。自定义 `AsyncTaskPool` 仍遵守自己的线程/编号
协议，没有新增必需 hook；`pi_v8::VmTaskPool` 不被内置池的授权逻辑接管。Worker
包装使用相同底层修复，其后台 loop 应是唯一实际消费者，外部不要同时 `run/block_on`。
`serial` 下的 `!Send` 值必须在合法 owner 域内创建、使用和销毁；本修复不赋予它们
任意跨线程移动的能力。

```rust
use pi_async_rt::prelude::{AsyncRuntime, SingleTaskRunner};

let runner = SingleTaskRunner::<()>::default();
let runtime = runner.startup().unwrap();
runtime.spawn(async {}).unwrap();
std::thread::spawn(move || runner.run_once().unwrap()).join().unwrap();
```

调度主流程、任务状态机、timer 注册/到期顺序、timeout/yield 输出、context 生命周期、
权重选择及 worker 通知逻辑均保持。`len()` 仍是可运行队列快照，不是所有 Pending
任务数；历史 `try_pop_all()` 不含栈且不扣消费计数，不能把它当成关闭清空接口。

成本与性能：授权新增 O(1) TLS/原子读取及比较，首次绑定一次强 CAS；没有新增热路径
锁、自旋等待、每任务分配或 Arc clone。每池增加一个原子字、每消费者线程一个 TLS 字，
任务本体增量为 0，不能按百万任务乘以该池字段大小。预热后空驱动、本地 FIFO/栈
操作的独立测试在 default/serial、Debug/Release 下均断言 0 次分配请求。

2026-09-06，WSL2/Ryzen 7 H 255，有界 A/B 样本（每场景15个样本、最多1个在途任务，
不是饱和或百万任务基准）：

| 正常CPU放置场景 | 修复前 ns/op 中位 | 修复后 ns/op 中位 | 修复后 ops/s 中位 |
| --- | ---: | ---: | ---: |
| default public Ready | 106.779 | 132.824 | 7,528,775 |
| default local Ready | 97.141 | 123.489 | 8,097,870 |
| serial public Ready | 134.188 | 131.150 | 7,624,834 |
| serial local Ready | 128.509 | 144.125 | 6,938,445 |

这些 ops 包含提交、分配、计时、断言和真实驱动。授权有有限成本，不承诺零性能回退：
serial local 在两组CPU放置中增加约12.15%/20.62%；default public/local 的相对变化
随CPU放置明显波动。完整正/负变化、延迟/CPU/RSS和原始数据保留于本地
`docs/SINGLE_TASK_POOL_OWNER_PERFORMANCE.md`，不得只挑有利数字或外推多worker吞吐。

本轮验收：除多线程运行时外的全量标准回归 Debug/Release 各101项（default54、serial47）
通过；有界 TSan 并发及helper、ASan 合同/并发、统计器5项、聚焦lint均通过。
LSan 明确未启用；历史 TaskId/本地适配器生命周期及 serial unsafe 泛化并未全面重构。
不把本轮结果称作全库无UB/无泄漏认证。多线程运行时、真实V8/其它宿主、非Linux平台
未执行端到端复验；本轮不运行人工观测、无界旧测试或大规模基准。

顺序验证入口（本节范围独立于其它历史章节）：

```sh
bash scripts/test_single_task_pool_owner.sh debug
bash scripts/test_single_task_pool_owner.sh release
env PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p test_single_task_pool_owner_analysis.py -v
cargo bench -p pi-async-rt --locked --offline -j 1 --bench single_task_pool_owner
cargo bench -p pi-async-rt --locked --offline -j 1 --features serial --bench single_task_pool_owner
```

回归脚本验证每个目标的实际通过数量及 rustdoc 清单，改名/少跑不能静默通过；不会自动
执行基准或检测器。完整设计、API摘要/大纲、审查和验收在本地 `docs/SINGLE_TASK_POOL_OWNER_*`
归档中，`docs/` 按仓库约定不加入 Git，不随发布交付。源码中文注释和标准测试随代码保留。

# timeout 等待句柄

`runtime.timeout(ms).await` 使用 timeout 专用等待句柄，不再为每次 timeout 分配普通任务 `TaskId/TaskHandle`。该实现保持公开 API 不变，并保留当前 timer 不支持取消的语义：

- timeout 到期后唤醒等待任务，并在 future 完成后释放等待句柄。
- timeout future 被提前 drop 时会清理 waker，timer 到期后释放内部等待句柄。
- 不修改普通 `spawn`、`spawn_timing`、任务池和 worker loop 的核心语义。
- 内部等待状态使用原子到期标记和 `AtomicWaker`，不引入自旋等待或阻塞锁。

建议验证命令：

```
cargo test --lib timeout_waiter_tests
cargo test --test timeout_waiter
cargo test --test timeout_waiter test_multi_thread_timeout_churn_rss_diagnostic -- --ignored --nocapture
cargo test --features serial --test timeout_waiter
cargo bench --bench timeout_waiter_pi_async -- --nocapture
```

# AsyncValue

`AsyncValue<V>` 是同步非阻塞、只允许设置一次的 single-shot future。当前实现保持公开 API 不变：

- `AsyncValue::new()`、`AsyncValue::set(self, value)` 和 `Future<Output = V>` 签名不变。
- `set()` 保持旧语义：第一次设置成功，后续设置静默失败且不会覆盖已设置值。
- pending 后允许重复 poll，重复 poll 会更新最新 waker，不会 panic。
- set 后唤醒最新 waker；never set 的 future 会继续 pending。
- 当前 API 不表达 sender/receiver 拆分、关闭或取消语义。

建议验证命令：

```
cargo test --test async_value -- --nocapture --test-threads=1
cargo test --features serial --test async_value -- --nocapture --test-threads=1
cargo bench --bench async_value_pi_async -- --nocapture
cargo bench --features serial --bench async_value_pi_async -- --nocapture
```

本地专项基准样例（WSL2 Ubuntu 22.04）：

| 场景 | 样例结果 | 折算指标 |
| --- | --- | --- |
| default pending poll | 4.57 ns/iter | 约 218.82M polls/s |
| default set then ready | 26.92 ns/iter | 约 37.15M ops/s |
| default single-thread runtime 内部 set/await | 147,983.84 ns/iter，128 pairs/iter | 约 864,959 pairs/s |
| default multi-thread runtime 外部 set/await | 651,453.15 ns/iter，256 pairs/iter | 约 392,968 pairs/s |
| default multi-thread runtime 内部 set/await | 952,500.35 ns/iter，256 pairs/iter | 约 268,766 pairs/s |
| default single-thread wait + multi-thread set 跨 runtime | 1,150,093.09 ns/iter，64 pairs/iter | 约 55,648 pairs/s |
| serial pending poll | 4.56 ns/iter | 约 219.30M polls/s |
| serial set then ready | 25.20 ns/iter | 约 39.68M ops/s |
| serial single-thread runtime 内部 set/await | 125,086.93 ns/iter，128 pairs/iter | 约 1.023M pairs/s |

# worker wake/sleep 唤醒协议

多线程 runtime 的 worker 空闲休眠路径会在任务入队或任务 waker 被外部线程触发后即时唤醒 sleeping worker，不再依赖 `worker_sleep_timeout` 超时兜底。该修复保持公开 API 不变：

- 不修改 `AsyncRuntime` trait。
- 不修改 `spawn`、`spawn_local`、`timeout`、`yield_now` 的函数签名和返回语义。
- 每次入队或 wake 最多唤醒一个 worker，避免广播式唤醒风暴。
- worker 休眠注册和外部唤醒使用同一个 condvar predicate，避免 “任务已入队但 worker 继续睡到 timeout” 的 lost wake。
- waits 队列使用有限扫描和 stale entry 清理，不进行无界循环。
- direct worker thread 和 serial worker thread 同步使用二次检查协议。
- `AsyncRuntimeBuilder::default_multi_thread(..., Some(0), ...)` 保持原有 builder fallback 语义，不会构建 0 worker pool；`Some(n > 0)` 仍创建同尺寸 `StealableTaskPool`，避免 worker 数大于 pool slot。

建议验证命令：

```
cargo test --lib worker_waker -- --nocapture --test-threads=1
cargo test --test worker_wakeup -- --nocapture --test-threads=1
cargo test --features serial --lib worker_waker -- --nocapture --test-threads=1
cargo bench --bench worker_wakeup_pi_async -- --nocapture
```

本地专项基准样例（WSL2 Ubuntu 22.04，8 worker）：

- `AsyncValue` 外部 wake：p50 66.705us，p99 137.103us，max 169.398us。
- 外部 `spawn`：p50 65.402us，p99 153.818us，max 213.274us。
- 8 个外部 producer 并发 `spawn` 到 8 worker runtime，1,000,000 个空任务：约 7,139,442 tasks/sec。
- 百万并发空任务资源观测：最大 RSS 80,236 KiB，Swaps 0。

# 多线程任务池并发安全

多线程 runtime 的 `StealableTaskPool` 保持每个 worker 独立的 timer、local queue、stack 和
selector，同时安全共享 pool 级权重刷新时间。该修复不修改任何公开 API 或合法调用语义：

- pool 级刷新时间使用 `AtomicCell<QInstant>`，消除多个 worker 在
  `try_pop_by_weight` 中对 `UnsafeCell<QInstant>` 的无同步读写。
- worker 启动时在私有 TLS 绑定当前 pool 身份；访问 local stack、selector、worker queue 或
  thread waker 前验证当前 worker 确实属于该 pool。
- 合法的跨 runtime `spawn_local` 仍回退到目标 runtime 的 public queue，不会被 owner guard
  拒绝；从错误 runtime worker 直接调用另一 pool 的 owner-only pop/waker API 属于非法上下文，
  现在会在访问 owner-only 状态前 panic，而不是进入潜在 data race/UB。
- 不改变 task 优先级、internal/external steal 顺序、worker wake/sleep、timeout、每 worker
  timer 或 Future poll 流程；热路径没有新增 clone、堆分配、循环、系统调用、同步 mutex 或自旋。
- x86_64 上 `AtomicCell<QInstant>` 由专项测试固定为 lock-free。其它 target 保证线程安全，
  但 crossbeam 可能使用平台 fallback，因此不承诺与 x86_64 相同的性能。

建议验证命令：

```
cargo test --test stealable_task_pool_concurrency -- --nocapture --test-threads=1
cargo test --release --test stealable_task_pool_concurrency -- --nocapture --test-threads=1
cargo bench --bench worker_wakeup_pi_async bench_multi_thread_empty_task_throughput_8_workers -- --nocapture
cargo bench --bench worker_wakeup_pi_async bench_multi_thread_internal_empty_task_throughput_8_workers -- --nocapture
```

本地修复前后交错专项基准样例（WSL2 Ubuntu 22.04，8 worker、8 producer、1,000,000 个空任务，
5 轮中位数）：

| 场景 | 修复前吞吐 | 修复后吞吐 | p50 变化 | p90 变化 | p99 变化 | RSS 变化 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 外部 OS 线程并发 spawn | 5.947M tasks/s | 5.970M tasks/s | -2.42% | -2.64% | -2.72% | +1.58% |
| runtime 内 8 个 producer 并发 spawn_local | 9.233M tasks/s | 9.811M tasks/s | -12.81% | +1.53% | -25.28% | -0.60% |

所有交错样本 `Swap=0`。这些数字用于当前机器的回归基线，不是跨硬件的吞吐或延迟承诺；
完整原始样本、TSan/ASan 和下游真实数据库复验记录保存在本地 `docs/` 验收归档中。

# AsyncTask 调度生命周期

默认 `AsyncTask` 使用私有 managed 调度状态，修复 Future 在 poll 内被唤醒并随后
`Ready` 时，残留队列引用被 worker 永久 `pop -> push` 的问题。旧实现无法区分
“另一个 worker 正在 poll”和“Future 已完成”，可能让多个多线程 runtime worker
在业务负载结束后仍持续占用 CPU。

当前实现保持全部公开 API 签名和合法调用语义不变：

- `AsyncTask::new`、`with_context`、`with_runtime_and_context`、`get_inner`、
  `set_inner` 及 `AsyncTaskPool` trait 均未修改签名。
- 运行时托管任务使用私有 `MANAGED/SCHEDULED/RUNNING/COMPLETED` 状态合并重复
  wake、排除并发 poll，并把完成后的迟到 wake 转为空操作。
- Future 返回 `Pending` 时，运行时先恢复 Future，再发布状态；poll 期间发生的任意
  次 wake 只生成一个后续队列项。
- Future 返回 `Ready` 或 poll 发生 panic 展开时进入终态，不再被陈旧队列项重排。
- 每次真实入队最多通知一个 worker；重复 wake 不 clone、不入队、不通知，避免
  queue/wake 风暴。
- 公开 `get_inner/set_inner` 继续进入兼容手工驱动模式，保留外部自定义 driver 的
  既有取出、恢复和显式复用能力。不得与本库运行时并发手工驱动同一任务。
- default single-thread、WorkerRuntime、StealableTaskPool、ComputationalTaskPool
  和通过 `SingleTaskRunner` 驱动的 `pi_v8::VmTaskPool` 使用该生命周期；独立
  `serial::AsyncTask` 未修改。
- managed 任务池的 `push_keep` 必须成功接收可运行任务；返回 `Err` 时和旧实现一样
  无法保证该次 wake 的执行进度，本库不会在唤醒热路径增加阻塞重试或广播。

状态处理使用内联原子操作，不新增 mutex、condvar wait、自旋锁、系统调用、用户代码
重入或独立堆分配。Future mutex 只覆盖 `Option::take/replace`，不会跨
`Future::poll`、析构、任务池入队或 worker 通知。修复没有新增或修改 `unsafe`、
裸指针、手工引用计数或 FFI。

热路径和内存边界：

- Ready 任务增加一次 poll claim 原子读改写和一次完成状态存储。
- Pending 任务再增加一次收尾原子读改写。
- wake 增加一次状态读取/读改写；重复或迟到 wake 会省去原有 Arc clone、队列 push
  和 worker notify。
- x86_64 上 `AsyncTask<StealableTaskPool<()>, ()>` 从 80B 对齐到 96B；一百万个
  同时存活任务的任务本体净增 16,000,000B，约 15.26MiB。该数字不包含 Arc 头、
  Future、TaskHandle、context、队列和分配器成本。
- 对命中旧永久重排问题的进程，完成任务会被释放，worker 可以重新进入已有 idle
  wait。下游生产环境已经复验：部署修复后，压测结束时多线程 runtime worker 的异常
  CPU 占用恢复正常。该反馈未提供统一数值样本，因此不作为吞吐、延迟或跨机器性能
  基准；正常负载下的具体表现仍取决于硬件、任务形态和竞争程度。

建议验证命令：

```
cargo test --test async_task_scheduling -- --nocapture --test-threads=1
cargo test --test async_task_scheduling_runtime_matrix -- --nocapture --test-threads=1
cargo test --release --test async_task_scheduling -- --nocapture --test-threads=1
cargo test --release --test async_task_scheduling_runtime_matrix -- --nocapture --test-threads=1
cargo test --doc -- --test-threads=1
cargo test --features serial --doc -- --test-threads=1
cargo check --bench async_task_scheduling_pi_async
```

本轮纠偏后 8 worker/8 producer、100,000 个 external 任务的资源和语义探针全部
精确完成，最大 RSS 约 16MiB、swap 0。相对性能信号没有收敛：Ready 报告阶段吞吐
变化为 -16.31%，同一轮 harness 耗时变化为 -0.54%；YieldOnce 报告阶段吞吐变化为
+5.78%，harness 耗时变化为 +10.75%。这些矛盾数据不能证明稳定回退或稳定提升，
本轮也未继续执行纠偏后的 1M/internal/WakeBurst。它们只作为后续同源交错复验基线，
不应表述为性能已经通过或没有影响。

<a id="one-due-timer-runner"></a>
<a id="v058-retained-changes"></a>

# v0.5.8 基线保留项

单到期驱动候选已按用户决定撤回，不提供 `run_once_with_one_due_timer()`、
`SingleTaskRunReport` 或异常消费记账守卫。旧 `run_once()` / `run()` 保持
v0.5.8 的批量到期处理、优先入队、分支内出队、批末计数和异常传播行为。

生产侧仅保留默认及 serial Single 两个旧驱动方法的零增量优化：没有登记或消费
定时项时，跳过 `fetch_add(0, Relaxed)`；非零计数仍在原位置批量提交。
每个空定时阶段最多省去两次原子读改写，不增加任务/运行时字段、锁、Clone 或堆分配。
不改变公共 API 签名、任务调度/唤醒协议或 timeout 语义，也不承诺具体吞吐提升比例。

中文注释按恢复后的实际链路校准。`local_async_runtime::<O>()` 的 `O` 必须与绑定时
一致，当前类型擦除实现不会检查类型，不可通过不同 `O` 探测运行时或期待返回 `None`。

保留验证脚本的有界进程组回收、延迟处理 SIGTERM/SIGINT、绝对截止及成功后复核，
并保留历史证据分析器的严格截止检查和 Python 3.8 兼容测试。它们不进入生产库。
这些工具只保留在不跟踪的 `docs/rollback_validation/`，用于当前工作区本地验证，
不随crate发布。`scripts/` 已恢复v0.5.8基线，没有新增脚本、测试清单或缓存文件。
候选构建、采样、变体和检测器入口已撤回，不能据旧候选记录声明当前版本性能通过。

标准复验严格串行，排除人工观测、ignored RSS和千万任务旧例；默认/serial的
迁移后Debug与Release各135项通过。本次没有执行基准，不提供零增量优化的量化性能承诺。

# 基准测试

## 云服务平台
- 16核(vCPU) 2.5 GHz主频、3.2 GHz睿频的Intel ® Xeon ® Platinum 8269CY（Cascade Lake）
- 内存:64G
- CentOS 7.3 64位


|项目|pi_async|async_std|tokio|备注|
|---|---|---|---|---|
|bench_async_mutex|3,266 ns/iter (+/- 136)|149,332 ns/iter (+/- 7,212)|6,374,238 ns/iter (+/- 861,432)||
|contention|338,786 ns/iter (+/- 68,222)|901,779 ns/iter (+/- 28,380)|2,157,495 ns/iter (+/- 38,100)||
|create|257 ns/iter (+/- 2)|61 ns/iter (+/- 0)|63 ns/iter (+/- 0)||
|no_contention|215,515 ns/iter (+/- 1,121)|225,034 ns/iter (+/- 740)|550,285 ns/iter (+/- 2,313)||
|await_empty_many|605,232 ns/iter (+/- 125,354)|394,823 ns/iter (+/- 19,107)|393,625 ns/iter (+/- 4,459)||
|chained_spawn|504,570 ns/iter (+/- 24,166)|1,090,176 ns/iter (+/- 27,817)|251,943 ns/iter (+/- 1,412)||
|ping_pong|1,176,361 ns/iter (+/- 197,786)|3,859,845 ns/iter (+/- 73,410)|1,193,711 ns/iter (+/- 20,376)||
|spawn_empty_many|4,187,949 ns/iter (+/- 587,053)|18,887,015 ns/iter (+/- 347,589)|9,941,412 ns/iter (+/- 659,722)||
|spawn_many|3,436,761 ns/iter (+/- 279,137)|19,001,495 ns/iter (+/- 380,355)|7,615,952 ns/iter (+/- 210,639)||
|spawn_one_to_one|6,205,756 ns/iter (+/- 826,745)|36,189,628 ns/iter (+/- 357,690)|16,620,075 ns/iter (+/- 589,085)||
|yield_many|23,757,528 ns/iter (+/- 4,110,213)|52,304,694 ns/iter (+/- 519,928)|17,746,497 ns/iter (+/- 550,878)||
|block_on|83 ns/iter (+/- 0)|2,593 ns/iter (+/- 48)|178 ns/iter (+/- 1)||
|local_run|666,627 ns/iter (+/- 5,476)||||
|local_send_many|5,885,537 ns/iter (+/- 98,251)||||
|local_spawn_many|1,260,102 ns/iter (+/- 5,423)|20,201,034 ns/iter (+/- 692,642)|1,553,246 ns/iter (+/- 49,815)||

# 贡献指南

# License

This project is licensed under the [MIT license].

[MIT license]: LICENSE

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in pi_async by you, shall be licensed as MIT, without any additional
terms or conditions.
