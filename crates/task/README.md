# pi-async-task

异步任务（Task）的创建、唯一执行持有、共享控制、结果等待与所属线程清理。

## 目标、适用范围与支持条件

本库面向执行器（executor）和运行时（runtime）的实现者。调用者提供所属线程（owner thread）和调度（scheduling），负责推进任务（Task）。普通应用可从 `pi-async-runtime` 获取运行时（runtime）入口。

当前清单版本为 `0.1.1`，采用 Rust 2021。已用 `nightly-2026-07-19` 在 Linux x86_64 和 Windows x64/MSVC 上执行相关构建或用例，证据限各自检查的版本和配置。最低支持 Rust 版本（MSRV）、Android 和 WebAssembly（Wasm）尚未确认。基础任务（Task）流程已有实现，本库新增查询能力和上层集成仍在验证（verification）。

## 使用前须了解的特性和术语

- **任务（Task）**持有一份原异步计算（Future）。创建同时返回可执行句柄（Runnable）、控制句柄（ControlHandle）和结果句柄（JoinHandle），各自承担独立权限。
- **接纳（admission）**把任务（Task）的推进及退出责任登记到所属线程（owner thread）。创建、接纳与实际轮询（poll）分别发生。
- **挂起（Pending）**表示本轮尚未得到结果。唤醒（wake-up）之后，调度（scheduling）负责把同一任务交给所属线程（owner thread）继续推进。

## 公开特性（feature）及组合

| 特性（feature） | 默认 | 作用 |
| --- | --- | --- |
| `observe` | 关闭 | 提供 `TaskObserver` 和 `TaskEvent` 等观测入口。使用者按需在创建选项中提供观测者。 |

`observe` 可独立启用。清单未声明互斥组合。

## 能力、类型与接口（API）

### 接入和执行一项任务

在使用本地源码的应用中按实际位置指定库（crate）路径。

```toml
[dependencies]
pi-async-task = { path = "../pi-async-rt/crates/task" }
```

使用路径依赖（path dependency）时保留完整工作区（workspace）及根清单。本例的一次性计算在首轮完成。

```rust
use std::{future::Future, pin::Pin, task::{Context, Poll, Waker}};
use pi_async_task::definitions::{RunOutcome, TerminationKind};
use pi_async_task::{create, Runnable, Schedule, TaskOptions, TaskThread};

struct ReadyOnly;

impl Schedule for ReadyOnly {
    fn schedule(&self, _: Runnable) {
        // 这个示例的计算首轮就绪。可能挂起的任务需要真正的调度。
        panic!("本示例不产生后续调度");
    }
}

let owner = TaskThread::new();
let parts = create(async { 42_u32 }, ReadyOnly, (), TaskOptions::plain())
    .unwrap_or_else(|_| panic!("创建失败"));
let registration = parts.runnable.admit(&owner)
    .unwrap_or_else(|_| panic!("接纳失败"));
assert_eq!(registration.snapshot().id, parts.control.id());
assert!(matches!(
    parts.runnable.run(&owner),
    Ok(RunOutcome::Finished(TerminationKind::Completed))
));

// 结果句柄仍对应原任务。示例已经驱动任务完成，可立即领取结果。
assert_eq!(parts.join.id(), parts.control.id());
let mut result = parts.join;
let mut context = Context::from_waker(Waker::noop());
assert!(matches!(
    Pin::new(&mut result).poll(&mut context),
    Poll::Ready(Ok(42))
));
```

可能挂起（Pending）的任务（Task）须使用能保留并重新交付原可执行句柄（Runnable）的调度（scheduling）。结果通过原结果句柄（JoinHandle）异步等待（await）。

### 创建、执行和查询权限

| 入口或对象 | 用途与边界 |
| --- | --- |
| `create` | 创建可跨线程提交的任务；原异步计算（Future）及输出要求 `Send + 'static`，不要求 `Sync` 或 `Unpin`。 |
| `create_local` | 在指定 `TaskThread` 上创建本地任务；计算及输出不要求 `Send/Sync`，仍要求 `'static`。 |
| `TaskParts` | 同时交付 `runnable`、`control`、`join` 三类权限；创建不隐式提交或执行。 |
| `Runnable` | 唯一执行持有，不可克隆；`admit` 建立接纳关系，`run` 消耗本份持有并至多轮询（poll）一次。 |
| `Schedule` | 接管重新可执行的原 `Runnable`，负责真正入队；不应在回调中直接轮询它。 |
| `ControlHandle` | 可克隆的查询、唤醒（wake-up）与取消请求入口，不拥有结果领取权。 |
| `JoinHandle` | 唯一结果等待值，实现标准 `Future`；释放它不自动取消原任务。 |
| `TaskThread / TaskRegistration` | 所属线程责任与已接纳任务登记；用于合法清理，不是操作系统线程句柄。 |
| `TaskSnapshot` | 生命周期与执行状态样本，不表示物理队列位置。 |

使用顺序为：创建 → 把原 `Runnable` 交给合法来源 → 在所属线程接纳 → 取出后执行一次 → 挂起时由调度回调接续。结果和取消分别通过对应句柄处理。

### 为任务选择队列访问能力

`TaskOptions` 在创建时选择能力：

| 构造入口 | 预留链接 |
| --- | --- |
| `plain()` | 无侵入式链接。 |
| `intrusive()` | 本地侵入式双端队列（intrusive deque）链接。 |
| `intrusive_mpsc()` | cordyceps侵入式多生产者单消费者队列（intrusive MPSC queue）链接。 |
| `intrusive_local_and_mpsc()` | 同时预留两套链接，供同一任务在不同时刻使用。 |

```rust
use pi_async_task::{TaskOptions, try_current_task_id};

let options = TaskOptions::intrusive_local_and_mpsc();
let current = try_current_task_id();
// options只是创建配置；current在本库任务的poll范围之外为None。
let _ = (options, current);
```

同一任务不能同时进入两种队列。链接转换保留原任务编号（TaskId）、原固定地址与唯一执行责任，不创建第二个任务。

`MpscEntryHandle` 提供受限的原值转换和独立空节点构造；转换失败必须处理返回的原句柄。不要通过裸指针恢复任意任务持有。

### 唤醒、取消与清理

标准唤醒器（Waker）可由同线程或其它线程调用；它通知原任务重新可执行，不直接执行计算，也不授予跨线程访问本地计算的权限。

`ControlHandle::cancel()` 发布取消请求。`cancel_and_wait()` 创建取消等待（cancellation wait），由实际推进它的受管任务（Task）提供等待者身份。进程内的外部宿主可在可信边界用 `ExternalWaiter::new` 和 `ExternalWaitSource` 提供外部等待方（external waiter）身份，再调用 `cancel_and_wait_from(&waiter)`。宿主须负责该身份的退出或放弃。`ExternalWaitSource::is_alive` 在轮询（poll）时同步调用，必须线程安全（thread-safe）且不阻塞。受管任务内始终使用实际当前身份。首次轮询未取得有效身份时返回 `UnknownWaiter`，这次等待不发布取消请求。

取消请求已记录、任务停止业务轮询、原异步计算（Future）确认析构（drop）是不同事实。

本地非 `Send` 计算必须在原所属线程清理。向其它线程转交执行句柄或释放共享控制句柄，不改变这一限制。所属线程必须继续处理自己已经接纳的退出责任。

放弃唯一执行持有会按原归属走合法退出路径，保留 `Abandoned` 等结束分类；不能通过泄漏句柄规避清理。

### 当前任务与附加数据

- `try_current_task_id()` 只在本库实际轮询原任务的同步范围内返回身份。
- 同一根计算直接推进的子计算沿用该身份；独立派发的任务有自己的身份。
- `try_with_current_metadata::<M, R>(...)` 同步借用原附加数据；无当前任务或类型不符时返回明确错误。
- 附加数据要求 `Send + Sync + 'static`。返回的是共享引用；更新须通过数据自身的安全接口，例如原子字段。
- 借用不能逃逸或跨越异步等待点（await）。独立 `block_on` 根计算不因此自动成为本库任务。

### 错误与原值

创建失败的 `BuildError` 保留原计算、调度对象、附加数据及选项；执行拒绝的 `RunError` 保留同一 `Runnable`。

结果失败使用 `TaskError`，区分取消、放弃、轮询恐慌（panic）、所属域关闭及清理未确认等情况。原恐慌载荷不保证是字符串，也不保证 `Sync`。

带原值的错误沿 `pi_result::RawResult` 返回；需要报告链时使用pi_result的标准入口，不为方便包装而丢失原值或放宽线程约束。

许可证（License）为 `MIT OR Apache-2.0`。
