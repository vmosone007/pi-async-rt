# pi-async-executor

默认单线程执行器（executor）把任务（Task）、来源、计时、策略和线程通知（thread notification）组合成可显式推进的事件循环（event loop）。

## 目标、适用范围与支持条件

本库面向组合单线程运行时（runtime）的实现者，供调用方选择任务来源、时钟（clock）、空闲策略及钩子（hook）。线程（thread）的创建和调度由调用方负责。应用通常从 `pi-async-runtime` 使用具体运行时（runtime）。

清单版本为 `0.1.1`，采用 Rust 2021。相关配置在 Linux x86_64 和 Windows x64/MSVC 上使用过 `nightly-2026-07-19`。最低支持 Rust 版本（MSRV）、Android 和 WebAssembly（Wasm）尚未确认。线程通知和多项基础能力已有实现，默认定时驱动和完整事件循环（event loop）仍在集成验证（verification）。

## 使用前须了解的特性和术语

- **接纳（admission）**把来自来源的任务（Task）交给同一个执行器（executor）继续推进。来源已接收任务和执行器已接纳任务分别观察。
- **轮转（turn）**是对执行器（executor）的一次推进。结果反映本轮及下一次检查的建议。
- **线程通知（thread notification）**安排所属线程（owner thread）重新检查工作，允许合并同一目标的多个通知。

## 公开特性（feature）及组合

| 特性（feature） | 默认 | 作用 |
| --- | --- | --- |
| `observe` | 关闭 | 转发运行时核心（runtime core）的观测配置，使任务配置可接入观测者。 |

`observe` 可独立启用。清单未声明互斥组合。默认定时来源由依赖库的 `cancel-timer` 提供，路径构建需可访问清单指定的 `pi-clock`。

## 能力、类型与接口（API）

### 接入和选择组件

```toml
[dependencies]
pi-async-executor = { path = "../pi-async-rt/crates/executor" }
```

使用上述路径时保留完整工作区（workspace）和根清单。

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

### 推进与接纳

执行器一次轮转（turn）处理维护、来源交付、可执行任务和必要的休眠判断。普通来源、内部来源、定时来源和空闲来源分别承担自己的任务保管责任。

`AdmissionRule` 区分串行和并发接纳：

- 串行：本地已接纳任务族未全部结束前，不再接纳下一批普通新业务。
- 并发：已有任务挂起时仍可以尝试接纳普通来源。
- 挂起（Pending）不表示根计算已经结束，也不解除原线程归属。
- 返回的 `TurnResult` 描述本轮结果与后续建议，不替代每个任务或来源的实际状态。

两种规则都不让执行器自动取得操作系统线程的所有权，也不提供非阻塞I/O驱动。若任务直接执行阻塞I/O，推进该执行器的线程会被阻塞。

### 时间与到期交付

默认 `ManualClock` 使用pi_clock的全局单调时间。宿主须按时钟库公开接口安装全局时间轴；执行器构造不擅自安装、重置或改用标准库的另一条时间轴。

每轮取出的统一时间显式交给定时来源推进，再尝试获取到期项。`TimerDelivery::One/All` 选择单项或当前固定范围全部到期交付；不能把单次发现的前缀伪装成全部。

队列或时钟返回的错误保持原报告；`WouldBlock` 与明确无工作不同。暂停等待发布者不是允许无界自旋的理由。

### 空闲、休眠与通知

空闲判断由 `IdlePolicy` 决定，事件循环保留固定的安全与责任检查。空闲来源不自己判断空闲；已接纳的空闲任务之后按普通任务处理。

线程主动通知用于工作、任务唤醒、取消和关闭；被动限时唤醒用于重新检查时间及状态。主动通知必须与休眠前重查配合，不能依靠超时弥补主动通知遗漏。

默认策略不因所有挂起任务本身就阻止空闲或休眠；自定义策略可以改变建议，但不能越过固定接纳、线程归属和清理规则。

下例演示取得一次线程通知（thread notification）。示例还直接使用 `pi-async-task-queue` 的 `QueueNotifier`。

```rust
use pi_async_executor::ThreadParker;
use pi_async_task_queue::QueueNotifier;

let mut parker = ThreadParker::new();
assert!(parker.take_notification()); // 创建时安排首次检查。
let notifier = parker.notifier();
notifier.notify(); // 发布工作之后，可通过此句柄通知所属线程。
assert!(parker.take_notification());
// 取得通知后，调用方重新检查来源，再决定是否休眠。
```

### 钩子

`on_event` 用于同步观察，应快速返回。`before_poll` 同步返回控制决定，执行器（executor）据 `TaskAction` 在合法边界处理。两个入口都应避免等待会阻碍所属线程（owner thread）推进的业务。

钩子可读取规定的任务信息与附加数据，不取得原 `Runnable` 所有权，不借出任意原计算引用，不把已接纳任务迁移到另一个服务目标。用户回调的耗时、重入和恐慌（panic）均须纳入调用边界。

### 查询与关闭

状态与数量通过核心接口观察，各项独立取样。默认 `waiting_for_wake_count` 和 `pending_cancel_task_count` 只统计已接纳任务，按需扫描登记，成本为 O(n)，n 为登记规模。默认推进不会主动采集这两项数量。

上述两项查询与关闭并发时，已开始的查询可能在调用线程释放任务附加数据（task metadata）。启用 `observe` 且配置观测接收者（TaskObserver）时，同一次释放也可能报告 `TaskEvent::ReclaimStarted`。析构（drop）或该回调发生恐慌（panic）时，原载荷沿查询传播。已绑定任务的原异步计算（Future）仍由所属线程清理。

关闭先停止新业务，再处理来源、已接纳计算和合法退出。`Closed` 与 `CloseState::Completed` 是不同事实；清理失败不能报告成功完成。线程内非 `Send` 原值仍在原所属线程释放。

许可证（License）为 `MIT OR Apache-2.0`。
