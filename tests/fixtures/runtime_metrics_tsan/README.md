# 运行时指标有界TSan编译载体

此独立非发布workspace直接引用根测试`tests/runtime_metrics_lifecycle.rs`及其四个
原support模块，不复制测试、不更改任何断言、场景、线程关系、生产能力或清理。
只选择此目标；trace／serial+trace各8个真实隔离子例，原每子例15秒、父180秒。

载体版本必须与直接被测`pi-async-rt`版本一致，当前均0.5.13；原测试会严格检查
生产Meter作用域的版本，不能删除断言或用环境变量伪装版本。升级时同步复核此
装配，并核验Cargo实际本地package ID及源码哈希，不用相同版本号冒充同一来源。

## 唯一依赖特例

nightly2026-07-19的`-Zsanitizer=thread`有严格ABI一致性检查。原indexmap1.9.3
在未显式开启std时由autocfg1.5.0直接调用rustc探测sysroot；该探测未取得Cargo
重建的插桩std，因ABI不匹配失败，随后tower0.4.13缺少默认哈希器类型而编译失败。
正常Debug／Release装配实际会设置has_std，不应把工具探测失败当成平台没有std。

载体只对已经锁定的indexmap1.9.3显式开启其官方std特性，跳过这次错误的能力
探测。独立锁初始继承根锁版本，其余实际解析版本逐项核验；根manifest／锁不改。
实际目标、依赖及标准库继续完整插桩，**禁止**unsafe-allow-abi-mismatch、关闭
ABI检查、手动伪造全局cfg、修改registry源码、屏蔽竞态或减少原8个场景。
其它直接依赖只是原测试合法装配所需，不引入SDK网络或新的生产依赖。

## 执行约束

只在明确授权后严格串行执行，每个编译步骤外层300秒截止，-j1／--locked／
--offline；成功后从Cargo的compiler-artifact取得真实测试产物，实际运行外层
180秒并保留原子例15秒截止、--test-threads=1／--nocapture。编译或运行失败
即停止，不自动重试、不延长截止。可复用匹配的独立TSan target缓存，不混用普通
std或其它toolchain的库；不用此载体运行基准、压力、覆盖率或CRAP。

```sh
env CARGO_TARGET_DIR=/tmp/pi-async-rt-metrics-tsan RUSTFLAGS=-Zsanitizer=thread \
  cargo +nightly-2026-07-19 test -Zbuild-std --target x86_64-unknown-linux-gnu \
  --manifest-path tests/fixtures/runtime_metrics_tsan/Cargo.toml --features trace \
  --test runtime_metrics_lifecycle --no-run --message-format=json --locked --offline -j1
```

命令示例必须套上述外层截止；serial+trace按同一流程另执行，不能并行。原全局
时钟只在每个实际子场景进程初始化一次，父、编译和生产指标模块不初始化。
TSan无报告也不等于全库无UB／无泄漏认证；其它平台、实际V8、完整OTLP运输和
生产远端仍由各自验证入口承担，不由此载体代证。
