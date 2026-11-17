# pi-async-task-queue

任务队列（task queue）的抽象、默认本地队列与共享来源接入。

本库只保管、交付和合法清理原任务，不负责接纳到执行器（executor）、业务轮询（poll）或决定何时空闲。

## 当前状态

清单版本为 `0.1.0`。本地双端队列、普通MPSC、空闲及定时来源的公开定义已经建立，各模块的实现和验收进度不同；默认定时来源等仍含未实现占位（todo!），新空闲来源的工作区实现仍须随整批交付核验。本包整体尚不能作为完整可用的发布版本，不能把本地未提交实现当成远端已交付内容。

本文说明当前公开接口（public API）及其使用边界，不把接口已声明、能够编译或局部测试通过等同于所有队列已经可用。

## 接入与特性

```toml
[dependencies]
pi-async-task-queue = { path = "../pi-async-rt/crates/task-queue", default-features = false }
```

| 特性（feature） | 默认 | 含义 |
| --- | --- | --- |
| `pool` | 开启 | 保留的 `pi_task_pool` 适配 `TaskPoolQueue`。 |
| `cancel-timer` | 关闭 | 默认可取消定时来源，依赖 `pi_cancel_timer = "0.7"`、`pi_slot_wheel`、`indexmap` 和 `pi-clock-manual`。 |

现有 `pool` 适配内部使用 `parking_lot::Mutex`，不能据包名或任务池原生实现宣称该适配无锁。它不是尚在设计中的新阻塞运行时池实现。两种新单线程运行时不启用这个默认特性。

本包继承根工作区配置；完整仓库解析还涉及仓库外的 `pi_task_pool`、`pi-clock` 本地路径。不要把当前路径依赖当成已核验的独立注册表发布方案。

## 四类来源及权限

| 对象或接口 | 责任 |
| --- | --- |
| `TaskQueue` | 绑定所属线程的本地侵入式双端队列；支持队首／队尾插入和取出。 |
| `ExternalTaskQueue` | 可跨线程的普通提交与批次交付协议。 |
| `ExternalQueueManager` | 原普通来源的唯一管理权限，负责共享句柄、清理和关闭。 |
| `IdleTaskQueue / IdleQueueManager` | 空闲来源的共享访问与管理；队列本身不判断空闲。 |
| `TimerTaskQueue / TimerQueueHandle` | 本地定时管理与线程安全共享请求分离；共享句柄不获得推进、取出或关闭权限。 |
| `QueueNotifier` | 来源发布后通知所属服务继续检查；不直接执行任务或实现事件循环。 |

`MpscTaskQueue` 是普通MPSC来源的唯一管理对象；`MpscTaskQueueHandle` 可克隆并共享。空闲来源分别使用 `MpscIdleTaskQueue` 与其句柄。

## 本地队列

```rust
use pi_async_task::TaskThread;
use pi_async_task_queue::TaskQueue;

let owner = TaskThread::new();
let mut queue = TaskQueue::new(&owner);
assert!(queue.is_empty());
// 实际入队值必须保留本地侵入式链接并满足原线程归属。
let _ = &mut queue;
```

在独立应用中编译此例，还需要直接依赖 `pi-async-task`。

`push_front/push_back` 拒绝时返回原 `Runnable`；`append_batch` 保留未接受部分。取出仅交付执行持有，不隐式轮询。不要同时把同一任务放进本地链接和MPSC链接。

## 普通MPSC来源

`MpscQueueOptions` 默认管理额度为64个节点，接收比较交换（CAS）尝试额度为3；这不是任务容量，也不是每个取出批次的硬上限。

共享入口提供单项和批次获取。必须保留结果分类：

- `Ready`：已经取得原任务或独立批次。
- `Empty`：本次确认的取出结果，不证明整个运行时空闲或关闭完成。
- `Busy/WouldBlock`：当前权限或发布交接尚不能交付；不能冒充 `Empty`，也不能无界等待。

数量查询是对应口径的独立样本，不能用一次 `len() == 0` 替代完整退出证明。

## 定时来源

默认实现为 `CancelTimerQueue<Item, N>`，需要 `cancel-timer` 特性；当前该实现仍含未实现占位。

- 构造、提交和公开推进接口使用显式的同源单调时间。
- 拥有者固定在原线程；本地提交不额外要求内容 `Send/Sync`，共享提交要求 `Send + 'static`。
- 先 `advance(now)` 推进，再尝试获取到期内容；队列不自行另取时钟。
- `try_take_due_one` 最多交付一项；`try_take_due_all` 交付本次固定范围内当前全部到期项，顺序由实现说明。
- 全部范围尚不能确认时返回 `WouldBlock`，不把已发现前缀冒充全部。
- 取消接口只接收请求，不承诺指定登记一定被取消；已到期交付和合法清理仍按原责任处理。

## 清理、关闭与原值

`clear` 处理调用确定的范围，后来的提交不无限扩展它；`close` 永久停收并接续清理，重复调用幂等（idempotent）。

队列在内部移除、清理或合法移交原值，不要求外部接回一批未到期值自行释放。`QueueCleanupProgress::MoreWork` 表示还有来源责任，应继续同一操作；`Complete` 不等于所有原计算已经确认析构（drop）。

用户析构或调度回调必须发生在正确的线程和权限范围。管理对象释放不能把尾部责任遗留给最后一个普通共享句柄。接收拒绝、已经接收后的责任以及原计算清理确认是不同事实。

拒绝提交的错误保留同一原值；普通报告使用pi_result，带原值错误沿 `RawResult` 返回，不以字符串代替原错误。

## 验证入口

```sh
cargo +nightly-2026-07-19 build -p pi-async-task-queue --no-default-features
cargo +nightly-2026-07-19 build -p pi-async-task-queue --all-features
cargo +nightly-2026-07-19 test -p pi-async-task-queue --no-default-features -- --test-threads=1
cargo +nightly-2026-07-19 doc -p pi-async-task-queue --no-default-features --no-deps
```

占位尚在时，构建成功不代表可运行；不要把因todo!退出的行为测试当成功。本README未新增运行上述验证，也不提供安全专项或性能达标结论。

源码入口：[公开导出](src/lib.rs)、[本地队列](src/local.rs)、[普通MPSC来源](src/mpsc.rs)、[空闲来源](src/idle.rs)、[定时协议](src/timer.rs)、[清理进度](src/management.rs)。

许可证（License）：`MIT OR Apache-2.0`。
