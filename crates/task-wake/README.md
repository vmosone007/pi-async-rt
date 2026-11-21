# pi-async-task-wake

固定任务目标的本地与跨线程唤醒（wake-up）交付。

## 目标、适用范围与支持条件

本库连接 `pi-async-task` 与 `pi-async-task-queue`，供执行器（executor）和运行时（runtime）把已绑定的任务（Task）交回原所属线程（owner thread）。创建线程（thread）、推进事件循环（event loop）及选择来源由使用者负责。

清单版本为 `0.1.1`，采用 Rust 2021。相关配置在 Linux x86_64 和 Windows x64/MSVC 上使用过 `nightly-2026-07-19`。最低支持 Rust 版本（MSRV）、Android 和 WebAssembly（Wasm）尚未确认。唤醒（wake-up）交付已有阶段验证，上层运行时（runtime）仍在集成验证（verification）。

## 使用前须了解的特性和术语

- **唤醒目标（WakeTarget）**关联原所属域、共享外部来源和失败恢复能力，同一个目标的克隆（Clone）保持此关联。
- **本地绑定（local binding）**让所属线程（owner thread）暂时操作同域队列（queue）。调用方必须持有有效的本地绑定才能取得相应权限。
- **恢复（recovery）**接管未能交付的原可执行句柄（Runnable）及其后续处理责任。

## 公开特性（feature）及组合

| 特性（feature） | 默认 | 作用 |
| --- | --- | --- |
| `observe` | 关闭 | 启用任务库的可选观测接入。 |

默认无可选特性（feature）。需要任务观测（task observation）时启用 `observe`。

## 能力、类型与接口（API）

### 接入和本地绑定

```toml
[dependencies]
pi-async-task-wake = { path = "../pi-async-rt/crates/task-wake", default-features = false }
```

保留完整工作区（workspace）。下例还直接使用 `pi-async-task` 和 `pi-async-task-queue`。由使用者接收恢复队列（recovery queue）的原值，并安排重试或在所属线程（owner thread）清理。

```rust
use std::{cell::RefCell, rc::Rc, sync::mpsc};
use pi_async_task::{Runnable, TaskThread};
use pi_async_task_queue::{ExternalQueueManager, MpscQueueOptions, MpscTaskQueue, TaskQueue};
use pi_async_task_wake::{DispatchFailure, DispatchRecovery, WakeTarget};

struct Recovery(mpsc::Sender<(Runnable, DispatchFailure)>);

impl DispatchRecovery for Recovery {
    fn retain(&self, task: Runnable, reason: DispatchFailure) {
        // 本例保证接收端在目标之后释放。实际服务要在退出前结清接收的原值。
        self.0.send((task, reason)).unwrap_or_else(|_| panic!("恢复接收端已关闭"));
    }
}

let owner = TaskThread::new();
let external = MpscTaskQueue::new(MpscQueueOptions::default())
    .unwrap_or_else(|_| panic!("创建外部来源失败"));
let (sender, receiver) = mpsc::channel();
let target = WakeTarget::new(&owner, external.handle(), Recovery(sender));
let queue = Rc::new(RefCell::new(TaskQueue::new(&owner)));
let guard = target.bind_local(&owner, queue).expect("同源且处于所属线程");
assert_eq!(guard.len().expect("本地队列可借用"), 0);
drop(guard); // 先撤销本次本地访问。
target.close(); // 只关闭该目标的新投递。
drop(target);
assert!(receiver.try_recv().is_err()); // 本例未提交任务。
```

### 主要对象

| 对象 | 用途 |
| --- | --- |
| `WakeTarget<E, R>` | 固定所属域与外部来源的唤醒目标；实现任务库的 `Schedule`。 |
| `LocalQueueGuard` | 当前所属线程内的本地队列绑定；退出时撤销这次访问。 |
| `DispatchRecovery` | 投递失败时接管同一原任务及后续合法处理责任。 |
| `DeliveryError` | 显式提交被拒绝时返回原 `Runnable` 与 `DispatchFailure`。 |
| `BindError / LocalAccessError` | 区分错误所属域、无合法本地访问等情况。 |
| `ImportOutcome` | 一次外部导入的结果，不等于任务已经执行。 |

`WakeTarget::new(owner, external, recovery)` 组合调用方提供的原所属域、外部来源和恢复实现。

### 路由规则

同一目标可被克隆，仍指向原所属域：

1. 在正确线程且存在有效本地绑定时，投递到本地队列尾部。
2. 从其它线程唤醒时，使用配置的外部来源。
3. 本地访问或外部投递被拒绝时，按显式返回或恢复接口保留原任务。
4. 唤醒不会重新创建任务，不迁移已经绑定的本地计算，也不直接轮询（poll）计算。

`ControlHandle` 取得的标准唤醒器（Waker）可以由同线程或其它线程持有者使用。能跨线程唤醒不等于能跨线程读取、轮询或析构本地非 `Send` 计算。

本库使用配置的原目标和来源。外部来源的再次交付仍受原执行器（executor）和所属线程（owner thread）约束。

### 本地绑定与外部导入

`bind_local` 核对原所属域及本地队列，在合法范围内返回 `LocalQueueGuard`。绑定不是提交成功或取得业务结果的证明。

守卫（guard）提供本地队首／队尾插入、队首获取、数量及 `import_external_once`。这些操作仍遵守原任务归属和唯一持有；导入只是移动原值，不执行它。

不能保存本地队列的可变借用后跨越异步等待点（await），也不能绕过守卫把线程内队列公开给其它线程使用。

### 投递失败与原值

`Waker::wake` 和 `Schedule::schedule` 没有返回错误的通道，所以恢复实现不能只记录日志后丢弃原任务。

`DispatchRecovery::retain(task, reason)` 正常返回意味着实现已经接管：

- 原任务的唯一可执行持有；
- 尚未处理的唤醒需求；
- 重试投递或按原所属线程合法退出的责任。

恢复入口不能等待其它线程或容量，不能在调用者线程直接轮询原计算，不能把责任转回已经返回的唤醒调用者。恢复回调自身的恐慌（panic）不由本库另行捕获。

显式 `try_submit` 被拒绝时，由调用者处理 `DeliveryError.task`；失败原因不是原任务已经完成的证明。

### 关闭

`WakeTarget::close` 关闭该目标的新投递，不等于整个运行时或它关联的全部队列已经清理完成。

普通目标克隆不取得来源的管理权限；移除本地绑定、关闭唤醒入口、取消原任务和完成原计算清理是不同步骤。

许可证（License）为 `MIT OR Apache-2.0`。
