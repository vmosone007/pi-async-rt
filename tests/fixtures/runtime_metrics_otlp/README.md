# 运行时指标真实 OTLP 验收夹具

独立标准测试crate，仅为`pi-async-rt`的真实采集上报补SDK聚合／网络运输证据。
状态：2026-10-10，v1.1离线解析、编译及唯一标准测试通过，四个验收阶段全部完成；
本夹具作者终审及SDK运输子项标准验收通过。首次缺缓存和沙箱禁止绑定端口的失败均保留，
不把环境失败冒充生产缺陷，也不据本夹具通过宣称完整指标功能已经验收。

采用官方OpenTelemetry SDK／OTLP／proto 0.30.0、tonic 0.13.1、Tokio 1.50.0及
tokio-stream 0.1.19，全部仅在此夹具声明必要测试依赖；不修改根manifest、根锁、
生产依赖或线程模型。不直接引入prost、serde、实验Reader、手写协议或mock上报器。
根trace传递feature可能合并，不能把直接选择误称为完整依赖图最小化证明。
tonic仅显式启用server，官方Server直接承载唯一MetricsServiceServer，不引入
不需要的多服务router功能；历史失败和首次锁在本地docs归档，未联网下载。

## 验收内容

唯一标准测试`test_registered_runtime_metrics_reach_otlp_receiver`严格顺序运行
一个隔离子进程，通过真实公开注册／注销／采集入口，观测官方导出器向真实
`127.0.0.1`随机端口发送的请求，由官方gRPC服务解码。接收端不造点、不补生产能力。

- 两个普通Single实例使用相同中文名称、不同注册ID，另有一个原生Local实例。
- 原spawn建立3／2个即时任务；原spawn_timing建立两个一小时定时项，覆盖非零
  `wait_len`，而A的`Some(0)`与本地的`None`必须严格区分。
- 三轮核验注册成功、注销后状态0但SDK保留旧数量、同ID恢复和最新Gauge覆盖。
- 每份报文严格核验三个固定仪器、八个唯一数据点、三个精确属性、名称／进程
  中文字符串、正整数ID、整数值、单位、描述、作用域及版本，不能只找到一个正确点。
- 最后注销全部，不再采集；官方SDK关闭的最终导出必须仍为最后记录，然后关闭
  服务端和运输runtime。权限／异常／并发／安全／性能矩阵不并入此夹具。

## 资源与调用边界

父进程不创建SDK／runtime／时钟，子进程一次原全局时钟初始化，保留守卫。
下文主线程指拥有三个Runner的场景测试线程，不是libtest框架的主线程。
主线程只推动原Runner，每轮另有一个外部同步采集线程；采集不在Runner线程
执行，不由夹具手动回填生产通道／清许可／改截止。只在生产采集返回后由宿主
显式SDK刷新，本库采集函数仍不flush／等待网络。

夹具显式创建一个Tokio运输worker和一个官方SDK PeriodicReader线程；SDK间隔
一小时，大于子进程60秒硬截止。这些是测试宿主设施，不给生产库新增线程要求。
本夹具不调用block_on；官方SDK内部实现使用futures_executor::block_on，未被修改。
子进程移除继承的`OTEL_*`配置，只连接回环地址，不影响父进程环境或生产服务。

生产实例预算2秒／整轮3秒，夹具观测4秒；导出器请求超时3秒、接收／服务停止
各3秒、Tokio关闭预算2秒。官方SDK0.30的force_flush使用无截止同步接收，不能
把导出器超时冒称整个调用的硬上界；父进程60秒成功截止、失败后最多5秒回收
提供整例兜底。异常Drop尽力清理，不将清理错误伪报为验收通过；极端OS不可
回收或被强杀等情形不保证完整析构。SDK关闭成功不冒称其线程已经由夹具join。

## 执行入口

本批锁生成／编译／唯一测试已经执行；下面为可复验入口，不自动授权下一次运行。
实际命令、原始日志和退出结果已归档；后续其它验证仍须另行授权。
先生成并核验独立锁，再锁定离线串行执行；不要用根目录裸cargo test带入旧无界目标。

```sh
cargo generate-lockfile --manifest-path tests/fixtures/runtime_metrics_otlp/Cargo.toml --offline
cargo check --manifest-path tests/fixtures/runtime_metrics_otlp/Cargo.toml --test acceptance --locked --offline -j1
cargo test --manifest-path tests/fixtures/runtime_metrics_otlp/Cargo.toml --test acceptance --locked --offline -j1 -- --nocapture --test-threads=1
```

每条Cargo命令还须外部300秒截止和日志归档，不自动重试／扩大预算。本夹具不
运行压力、基准、sanitizer或其它旧测试；单项通过不等于整个指标设计已验收。
本地TCP绑定需要执行环境允许回环套接字；沙箱拒绝时应保留失败并取得相应权限，
不能删去真实网络、改用直接调用接收方法或放宽断言来绕过限制。
外部强杀不执行Rust Drop，执行驱动须回收该命令自己的整个进程组，不能只杀父PID
并留下子进程，也不能借此终止其它工作。不要以正常Drop兜底代替外部终止清理。

## 本批实际结果

- 独立锁380包；本机目标依赖图314节点、一个workspace成员，直接被测版本为本地0.5.13。
- 离线check通过，无编译警告；唯一标准测试及其隔离子进程均正常退出。
- A01～A04全部通过：三实例、三轮刷新及一次SDK最终导出，共四份真实请求，
  每份八个数据点；同名不同ID、完整中文名称、None与零、非零wait_len、最新值
  覆盖、注销状态和同ID恢复均通过严格全集断言。
- 首次metadata因axum 0.8缓存缺失失败；用户确认v1.1最小修正后继续。随后沙箱
  拒绝127.0.0.1绑定，测试在场景开始前失败；获执行权限后同命令离线复跑通过。
  两次环境失败都保留，没有修改生产代码、根manifest／锁、场景或断言。
- check耗时237.89秒；通过的测试命令229.81秒含冷构建，子场景0.05秒、父入口
  0.13秒。这些是执行记录，不是吞吐、延迟或资源性能基准。
- 两次进入测试的隔离子进程各初始化一次原全局时钟；父进程和生产指标入口不初始化。
  本批未运行其它测试、Release、压力、基准或sanitizer，不证明公网配置或全部平台。

正式范围／选型／三图／审查与证据边界见
[MET-OTLP-FIXTURE-001](../../../docs/RUNTIME_METRICS_OTLP_ACCEPTANCE_FIXTURE_DESIGN.md#metrics-otlp-fixture)，
实际记录见[本批JSON](../../../docs/runtime_metrics_otlp_records/validation_completed.json)，
当前总体验证状态见[开发上下文](../../../docs/DEVELOPMENT_CONTEXT.md#ctx-runtime-metrics)。
