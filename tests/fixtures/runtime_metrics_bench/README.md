# 全局运行时指标有界性能夹具

独立非发布workspace，只经正式指标和原运行时API验证／观测，不提供采样、唤醒、
许可、截止或调度替代品；不加入根workspace，不由普通cargo test自动运行。
直接使用本地pi-async-rt0.5.13／trace及官方SDK／协议，根依赖不变。

## 场景和成本

- 直接来源1／8／64／256，名称8／256字节；原生Local1／8；每项冷1／暖1000轮。
- 64个真实核心的1000次同名重复登记及1000次注销／恢复，不无限创建新核心。
- 8worker／4外部生产者，未登记／仅登记／并发采集三模式；各预热1000加三轮
  各10000空业务任务，合计93000。观测在途最多1920加8个执行中任务，低于2048。
- 任务业务为空，仅统一编号回填和延迟观测；独立全集严格核验每任务一次完成。
  时间含运输／join，不是裸poll极限；全局System分配观察器的原子统计有额外成本。
- SDK导出只在计时外flush，官方聚合／协议转换／prost编码解码核验精确点及三标签。
  不启动OTLP网络服务，不把protobuf长度称为网络总字节或远端回执。

父严格串行运行24个独立子进程；子场景30秒／整批180秒，每10ms观察父子RSS与
可用内存，超过1GiB或低于4GiB即失败。只管理自己拥有的Child；RSS缺失不填零，
必须在原10ms间隔内确认成功退出，否则失败。退出确认不是一个有效RSS样本；
短采样会漏瞬间峰值，不是硬资源隔离。父不初始化时钟，每实际子例只初始化一次。

## 明确授权后执行

先单独离线编译，不在编译命令中自动运行；每次编译外层300秒截止：

```sh
timeout --signal=TERM --kill-after=5s 300s cargo bench --manifest-path tests/fixtures/runtime_metrics_bench/Cargo.toml --bench metrics --no-run --message-format=json --locked --offline -j1
```

成功后从compiler-artifact取得实际executable，另套180秒外层截止执行；不要猜测
文件hash或复用旧产物。全部验证必须串行，外部高负载存在时不执行。任何失败即停，
不自动重试、放宽断言、扩大资源或截止；百万／长压另行讨论，不属于本夹具。

输出为逐行JSON：phase为API耗时／分配／CPU窗，tasks为任务墙钟吞吐，samples
区分成功／在途，protobuf为官方消息字节，resource为有效采样与退出标志。
任务phase的inverse_mean_latency_per_second不是并发吞吐；真实吞吐看tasks_per_second。
CPU实际CLK_TCK通常有粗粒度，0刻度不等于零消耗；净保留还可能包含观测Vec和后台收尾。
三轮短窗口不证明旧版非劣、硬实时或生产峰值；完整原始基线及限制本地归档于
docs/RUNTIME_METRICS_PERFORMANCE_RESULTS.md及标准验收记录，不跟踪docs。
