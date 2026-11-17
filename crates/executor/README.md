# pi-async-executor

默认单线程执行器（executor）：把任务、来源、计时、策略和线程通知组合成可显式推动的事件循环。

本库面向需要定制运行时（runtime）的实现者。应用通常直接使用上层 [pi-async-runtime](../runtime/README.md)。

## 当前状态与接入

清单版本为 `0.1.0`。当前是包含完整声明和详细未实现占位（todo!）的执行器骨架，不能作为可运行服务。构建通过只证明类型与连接可编译。

```toml
[dependencies]
pi-async-executor = { path = "../pi-async-rt/crates/executor" }
```

默认特性（feature）为空；`observe` 启用运行时核心的任务观测接入。默认组合依赖 `pi-clock-manual` 及开启可取消定时来源的队列库。

当前仓库依赖外部兄弟目录中的时钟实现；包版本声明和本地构建证据不证明注册表发布可独立使用。

## 主要对象

| 对象或接口 | 职责 |
| --- | --- |
| `DefaultExecutor` | 独占本地执行状态，按已选接纳规则推进一次事件循环。 |
| `DriveLease` | 排他的推进持有；同一执行器不能被两个调用者同时推进。 |
| `ExecutorHandle` | 原服务的受限共享业务访问；不拥有本地事件循环。 |
| `ExecutorLocal` | 当前所属线程内的受限操作，不把线程内队列变成任意线程可访问。 |
| `ExecutorClock / ManualClock` | 统一单调时间读取及默认pi_clock接入。 |
| `ThreadNotifier / ThreadParker` | 工作通知与限时停放；线程唤醒不同于计算的标准唤醒器。 |
| `IdlePolicy / DefaultIdlePolicy` | 根据只读状态同步判断空闲及休眠建议。 |
| `ExecutorHooks / NoHooks` | 观察事件及轮询前同步控制，不转移任务执行持有。 |
| `DefaultTimerAccess / DefaultTimerDriver` | 核心计时登记与默认可取消定时来源接入。 |

组件通过公开窄接口组合；用户不能通过共享句柄直接操作内部队列或原计算。

## 推进与接纳

执行器一次轮转（turn）处理维护、来源交付、可执行任务和必要的休眠判断。普通来源、内部来源、定时来源和空闲来源分别承担自己的任务保管责任。

`AdmissionRule` 区分串行和并发接纳：

- 串行：本地已接纳任务族未全部结束前，不再接纳下一批普通新业务。
- 并发：已有任务挂起时仍可以尝试接纳普通来源。
- 挂起（Pending）不表示根计算已经结束，也不解除原线程归属。
- 返回的 `TurnResult` 描述本轮结果与后续建议，不替代每个任务或来源的实际状态。

两种规则都不让执行器自动取得操作系统线程的所有权，也不提供非阻塞I/O驱动。若任务直接执行阻塞I/O，推进该执行器的线程会被阻塞。

## 时间与到期交付

默认 `ManualClock` 使用pi_clock的全局单调时间。宿主须按时钟库公开接口安装全局时间轴；执行器构造不擅自安装、重置或改用标准库的另一条时间轴。

每轮取出的统一时间显式交给定时来源推进，再尝试获取到期项。`TimerDelivery::One/All` 选择单项或当前固定范围全部到期交付；不能把单次发现的前缀伪装成全部。

队列或时钟返回的错误保持原报告；`WouldBlock` 与明确无工作不同。暂停等待发布者不是允许无界自旋的理由。

## 空闲、休眠与通知

空闲判断由 `IdlePolicy` 决定，事件循环保留固定的安全与责任检查。空闲来源不自己判断空闲；已接纳的空闲任务之后按普通任务处理。

线程主动通知用于工作、任务唤醒、取消和关闭；被动限时唤醒用于重新检查时间及状态。主动通知必须与休眠前重查配合，不能依靠超时弥补主动通知遗漏。

默认策略不因所有挂起任务本身就阻止空闲或休眠；自定义策略可以改变建议，但不能越过固定接纳、线程归属和清理规则。

## 钩子

`on_event` 用于同步观察，应快速返回，不承担可阻塞的业务处理。`before_poll` 可以同步等待返回控制决定，执行器据 `TaskAction` 在合法边界处理。

钩子可读取规定的任务信息与附加数据，不取得原 `Runnable` 所有权，不借出任意原计算引用，不把已接纳任务迁移到另一个服务目标。用户回调的耗时、重入和恐慌（panic）均须纳入调用边界。

## 查询与关闭

状态与数量通过核心接口观察，各项独立取样。默认等待唤醒及待取消查询按需扫描登记，成本为O(n)，n为登记规模；默认推进不因存在查询接口而持续采集。

关闭先停止新业务，再处理来源、已接纳计算和合法退出。`Closed` 与 `CloseState::Completed` 是不同事实；清理失败不能报告成功完成。线程内非 `Send` 原值仍在原所属线程释放。

## 验证入口

```sh
cargo +nightly-2026-07-19 build -p pi-async-executor --all-features
cargo +nightly-2026-07-19 doc -p pi-async-executor --all-features --no-deps
cargo +nightly-2026-07-19 test -p pi-async-executor --all-features -- --test-threads=1
```

本README未运行这些命令。尚未完成的构造与配对释放路径不应被当成可用能力；行为、安全专项和性能需要真实实现后的证据。

源码入口：[公开导出](src/lib.rs)、[执行器](src/engine.rs)、[接纳与配置](src/drive.rs)、[空闲策略](src/policy.rs)、[钩子](src/hooks.rs)、[线程通知](src/parking.rs)、[时钟](src/clock.rs)。

许可证（License）：`MIT OR Apache-2.0`。
