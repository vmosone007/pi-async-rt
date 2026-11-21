# pi-async-task-queue

提供外部任务队列（external task queue）、定时任务队列（timer task queue）、空闲任务队列（idle task queue）的特征（trait）与默认类型，以及内部任务队列（internal task queue）的具体类型。

## 目标、适用范围与支持条件

供执行器（executor）和运行时（runtime）实现者保管、取出和清理原任务（Task），也可独立保管定时登记的原值。接纳（admission）、业务轮询（poll）和空闲判断由调用方负责。

当前清单版本为 `0.1.2`，采用 Rust 2021。相关配置在 Linux x86_64 和 Windows x64/MSVC 上使用过 `nightly-2026-07-19`。最低支持 Rust 版本（MSRV）、Android 和 WebAssembly（Wasm）尚未确认。各队列（queue）的验证进度不同。

## 使用前须了解的特性和术语

- **原值（original value）**由来源或调用者唯一保管。正常取出交接原值和后续责任，接收前拒绝返还同一原值。
- **共享句柄（shared handle）**保留对原来源的有限访问。清理、停止接收等能力由相应管理者掌握。
- **外部和空闲任务队列的共享访问**须支持克隆（Clone）及线程安全（thread safety）。副本访问同一来源。通用接口使用具象类型或泛型（generic）边界。
- **定时登记键（CancelTimerKey）**关联原定时来源的那次登记。取消请求（cancellation request）、队列清理完成和原异步计算（Future）析构确认分别发生。

## 公开特性（feature）及组合

| 特性（feature） | 默认 | 含义 |
| --- | --- | --- |
| `cancel-timer` | 关闭 | 提供 `CancelTimerQueue`、配套共享句柄（shared handle）、选项及定时登记键（CancelTimerKey）。抽象 `TimerTaskQueue` 和 `TimerQueueHandle` 始终可用。 |

默认无可选特性（feature）。需要默认可取消定时队列（CancelTimerQueue）时启用 `cancel-timer`。使用本地工作区（workspace）时准备清单引用的 `pi-clock` 兄弟目录。

## 能力、类型与接口（API）

### 接入和选择队列

```toml
[dependencies]
pi-async-task-queue = { path = "../pi-async-rt/crates/task-queue", default-features = false }
```

使用路径依赖（path dependency）时保留完整工作区（workspace）及根清单。由实际任务来源决定下列权限分工。

| 对象或接口 | 责任 |
| --- | --- |
| `TaskQueue` | 内部任务队列（internal task queue）的具体类型。与所属线程绑定，支持队首／队尾插入和取出。 |
| `ExternalTaskQueue` | 可跨线程的普通提交与批次交付协议。 |
| `ExternalQueueManager` | 原普通来源的唯一管理权限，负责共享句柄、清理和关闭。 |
| `IdleTaskQueue / IdleQueueManager` | 空闲来源的共享访问与管理；队列本身不判断空闲。 |
| `TimerTaskQueue / TimerQueueHandle` | 本地定时管理与线程安全共享请求分离；共享句柄不获得推进、取出或关闭权限。 |
| `QueueNotifier` | 来源发布后通知所属服务继续检查；不直接执行任务或实现事件循环。 |

`MpscTaskQueue` 是普通MPSC来源的唯一管理对象；`MpscTaskQueueHandle` 可克隆并共享。空闲来源分别使用 `MpscIdleTaskQueue` 与其句柄。

### 本地队列

```rust
use pi_async_task::TaskThread;
use pi_async_task_queue::TaskQueue;

let owner = TaskThread::new();
let queue = TaskQueue::new(&owner);
// 队列绑定 owner，后续操作须在合法所属线程上进行。
assert!(queue.is_empty());
```

在独立应用中编译此例，还需要直接依赖 `pi-async-task`。

`push_front/push_back` 拒绝时返回原 `Runnable`；`append_batch` 保留未接受部分。取出仅交付执行持有，不隐式轮询。不要同时把同一任务放进本地链接和MPSC链接。

### 普通及空闲来源

`MpscQueueOptions` 默认管理额度为64个节点，接收比较交换（CAS）尝试额度为3；这不是任务容量，也不是每个取出批次的硬上限。

共享入口提供单项和批次获取。必须保留结果分类：

- `Ready` 已经取得原任务或非空批次，按原顺序交付。
- `Empty` 本次未取到任务。整个运行时是否空闲或关闭完成仍需另行确认。
- `WouldBlock` 本次未取到任务，来源仍有在途责任，例如发布或清理。调用方可以按自己的推进策略有限重试。
- `Busy` 本次未取得取出权限。调用方可在后续推进时再尝试。
- `Closed` 表示普通取出已停用，管理者仍须结清剩余责任。

```rust
use pi_async_task_queue::{ExternalQueueManager, MpscQueueOptions, MpscTake, MpscTaskQueue};

let source = MpscTaskQueue::new(MpscQueueOptions::default())
    .unwrap_or_else(|_| panic!("创建外部来源失败"));
let handle = source.handle();
// 管理者仍在。通过共享句柄从当前空来源尝试一次单项取出。
assert!(matches!(handle.try_take_one(), MpscTake::Empty));
```

数量查询是对应口径的独立样本，不能用一次 `len() == 0` 替代完整退出证明。

空闲来源由 `IdleTaskQueue` 提供共享提交和取出，由 `IdleQueueManager` 掌管清理和关闭。调用者在空闲条件成立时尝试普通取出，`process_cleanup` 可独立运行。

运行时（runtime）还可用 `process_cleanup_with_readiness` 在同一次有限清理中判断是否已确认可继续推进。其第一项表示本次范围是否有余项，第二项为 `true` 表示已确认本线程可继续推进。自定义来源只实现 `process_cleanup` 时，默认给出第一项，第二项返回 `false`。调用者可按原休眠上限复查。

### 定时来源

默认实现 `CancelTimerQueue<Item, N>` 由 `cancel-timer` 提供。用同一个时钟（clock）的时间点构造、登记和推进。

- 构造、提交和公开推进接口使用显式的同源单调时间。
- 拥有者固定在原线程；本地提交不额外要求内容 `Send/Sync`，共享提交要求 `Send + 'static`。
- 先 `advance(now)` 推进，再尝试获取到期内容；队列不自行另取时钟。
- `try_take_due_one` 最多交付一项；`try_take_due_all` 交付本次固定范围内当前全部到期项，顺序由实现说明。
- 全部范围尚不能确认时返回 `WouldBlock`，不把已发现前缀冒充全部。
- 取消接口只接收请求，不承诺指定登记一定被取消；已到期交付和合法清理仍按原责任处理。

下例启用 `cancel-timer`，并直接依赖同一份 `pi-clock-manual`。以显式零时刻交付一次本地到期值。此示例按该配置单独核验。

```rust,ignore
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use pi_async_task_queue::{
    CancelTimerOptions, CancelTimerQueue, QueueNotifier, TimerTake, TimerTaskQueue,
};
use pi_clock_manual::global::GlobalManualInstant;

struct Notice(Arc<AtomicBool>);
impl QueueNotifier for Notice {
    fn notify(&self) {
        self.0.store(true, Ordering::Release); // 实际服务还需唤醒可能休眠的所属线程。
    }
}

let now = GlobalManualInstant::ZERO;
let signalled = Arc::new(AtomicBool::new(false));
let mut queue = CancelTimerQueue::new(now, Notice(Arc::clone(&signalled)), CancelTimerOptions::default())
    .unwrap_or_else(|_| panic!("创建定时队列失败"));
let _key = queue.try_push_local(now, 7_u32)
    .unwrap_or_else(|_| panic!("提交定时原值失败"));
assert!(signalled.load(Ordering::Acquire));
queue.advance(&now).unwrap_or_else(|_| panic!("推进失败"));
assert!(matches!(queue.try_take_due_one(), Ok(TimerTake::Ready(7))));
```

### 清理、关闭与原值

`clear` 处理调用确定的范围，后来的提交不无限扩展它；`close` 永久停收并接续清理，重复调用幂等（idempotent）。

队列在内部移除、清理或合法移交原值，不要求外部接回一批未到期值自行释放。`QueueCleanupProgress::MoreWork` 表示还有来源责任，应继续同一操作；`Complete` 不等于所有原计算已经确认析构（drop）。

用户析构或调度回调必须发生在正确的线程和权限范围。管理对象释放不能把尾部责任遗留给最后一个普通共享句柄。接收拒绝、已经接收后的责任以及原计算清理确认是不同事实。

拒绝提交的错误保留同一原值；普通报告使用pi_result，带原值错误沿 `RawResult` 返回，不以字符串代替原错误。

许可证（License）为 `MIT OR Apache-2.0`。
