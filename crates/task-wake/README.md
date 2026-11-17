# pi-async-task-wake

固定任务目标的本地与跨线程唤醒（wake-up）交付。

本库连接 [pi-async-task](../task/README.md) 与 [pi-async-task-queue](../task-queue/README.md)，供执行器（executor）或运行时（runtime）的实现者组合使用。它不创建业务线程，不推动事件循环，也不提供输入输出驱动（I/O driver）。

## 当前状态与接入

清单版本为 `0.1.0`。唤醒路由已有实现和阶段验证；当前依赖库及上层集成仍在开发，本文不声明整条运行时链已经验收或发布。

```toml
[dependencies]
pi-async-task-wake = { path = "../pi-async-rt/crates/task-wake", default-features = false }
```

| 特性（feature） | 默认 | 作用 |
| --- | --- | --- |
| `pool` | 开启 | 启用队列库保留的任务池适配。 |
| `observe` | 关闭 | 启用任务库的可选观测接入。 |

选择 `pool` 不表示新阻塞运行时池已经实现，也不保证旧适配无锁。完整仓库的本地路径前置条件见 [队列库](../task-queue/README.md)。

## 主要对象

| 对象 | 用途 |
| --- | --- |
| `WakeTarget<E, R>` | 固定所属域与外部来源的唤醒目标；实现任务库的 `Schedule`。 |
| `LocalQueueGuard` | 当前所属线程内的本地队列绑定；退出时撤销这次访问。 |
| `DispatchRecovery` | 投递失败时接管同一原任务及后续合法处理责任。 |
| `DeliveryError` | 显式提交被拒绝时返回原 `Runnable` 与 `DispatchFailure`。 |
| `BindError / LocalAccessError` | 区分错误所属域、无合法本地访问等情况。 |
| `ImportOutcome` | 一次外部导入的结果，不等于任务已经执行。 |

`WakeTarget::new(owner, external, recovery)` 组合原所属域、外部来源和恢复实现。调用者须先准备三者；本库不偷偷建立另一份任务池或无主缓存。

## 路由规则

同一目标可被克隆，仍指向原所属域：

1. 在正确线程且存在有效本地绑定时，投递到本地队列尾部。
2. 从其它线程唤醒时，使用配置的外部来源。
3. 本地访问或外部投递被拒绝时，按显式返回或恢复接口保留原任务。
4. 唤醒不会重新创建任务，不迁移已经绑定的本地计算，也不直接轮询（poll）计算。

`ControlHandle` 取得的标准唤醒器（Waker）可以由同线程或其它线程持有者使用。能跨线程唤醒不等于能跨线程读取、轮询或析构本地非 `Send` 计算。

本库只知道配置的原目标和来源；具体运行时若需要另一种共享内部来源，须由其合法组合提供，不能把外部队列中的重新交付误解为任意迁移执行器。

## 本地绑定与外部导入

`bind_local` 核对原所属域及本地队列，在合法范围内返回 `LocalQueueGuard`。绑定不是提交成功或取得业务结果的证明。

守卫（guard）提供本地队首／队尾插入、队首获取、数量及 `import_external_once`。这些操作仍遵守原任务归属和唯一持有；导入只是移动原值，不执行它。

不能保存本地队列的可变借用后跨越异步等待点（await），也不能绕过守卫把线程内队列公开给其它线程使用。

## 投递失败必须有承担者

`Waker::wake` 和 `Schedule::schedule` 没有返回错误的通道，所以恢复实现不能只记录日志后丢弃原任务。

`DispatchRecovery::retain(task, reason)` 正常返回意味着实现已经接管：

- 原任务的唯一可执行持有；
- 尚未处理的唤醒需求；
- 重试投递或按原所属线程合法退出的责任。

恢复入口不能等待其它线程或容量，不能在调用者线程直接轮询原计算，不能把责任转回已经返回的唤醒调用者。恢复回调自身的恐慌（panic）不由本库另行捕获。

显式 `try_submit` 被拒绝时，由调用者处理 `DeliveryError.task`；失败原因不是原任务已经完成的证明。

## 关闭

`WakeTarget::close` 关闭该目标的新投递，不等于整个运行时或它关联的全部队列已经清理完成。

普通目标克隆不取得来源的管理权限；移除本地绑定、关闭唤醒入口、取消原任务和完成原计算清理是不同步骤。

## 构建与验证

```sh
cargo +nightly-2026-07-19 build -p pi-async-task-wake --no-default-features
cargo +nightly-2026-07-19 test -p pi-async-task-wake --no-default-features -- --test-threads=1
cargo +nightly-2026-07-19 test -p pi-async-task-wake --all-features -- --test-threads=1
cargo +nightly-2026-07-19 doc -p pi-async-task-wake --no-default-features --no-deps
```

这些是验证入口，本文未重新执行，也不把历史测试扩大为当前所有依赖配置的通过结论。未提供吞吐、延迟、无等待（wait-free）或固定耗时保证。

源码入口：[公开导出](src/lib.rs)、[目标与本地绑定](src/target.rs)、[失败与恢复](src/recovery.rs)。

许可证（License）：`MIT OR Apache-2.0`。
