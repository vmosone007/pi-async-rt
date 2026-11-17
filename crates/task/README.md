# pi-async-task

异步任务（Task）的创建、唯一执行持有、共享控制、结果等待与所属线程清理。

本库面向执行器（executor）和运行时（runtime）的实现者。普通应用通常应使用上层 [pi-async-runtime](../runtime/README.md)。

## 当前状态

清单版本为 `0.1.0`。本库的基础生命周期已有实现；当前工作区仍在进行新增能力及上层集成的实现与验收。工作区中的实现、Git提交和注册表发布物是不同对象，本文不声明已经发布可用版本。

本库不创建线程，不提供事件循环（event loop），也不替调用者实现调度（schedule）。

## 接入

在完整仓库旁的应用中使用本地路径：

```toml
[dependencies]
pi-async-task = { path = "../pi-async-rt/crates/task" }
```

`observe` 是可选观测特性（feature），默认关闭。它提供任务观测接入；开启不等于自动安装日志、指标或全局收集器。

本包继承根工作区的语言版本、许可证与依赖配置。不要只复制本目录而丢掉根清单。

## 权限与主要接口

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

## 任务布局

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

## 唤醒、取消与清理

标准唤醒器（Waker）可由同线程或其它线程调用；它通知原任务重新可执行，不直接执行计算，也不授予跨线程访问本地计算的权限。

`cancel()` 发布取消请求；`cancel_and_wait()` 等待取消及清理的规定结果。取消请求已记录、任务停止业务轮询、原计算确认析构是不同事实，不能互相替代。

本地非 `Send` 计算必须在原所属线程清理。向其它线程转交执行句柄或释放共享控制句柄，不改变这一限制。所属线程必须继续处理自己已经接纳的退出责任。

放弃唯一执行持有会按原归属走合法退出路径，保留 `Abandoned` 等结束分类；不能通过泄漏句柄规避清理。

## 当前任务与附加数据

- `try_current_task_id()` 只在本库实际轮询原任务的同步范围内返回身份。
- 同一根计算直接推进的子计算沿用该身份；独立派发的任务有自己的身份。
- `try_with_current_metadata::<M, R>(...)` 同步借用原附加数据；无当前任务或类型不符时返回明确错误。
- 附加数据要求 `Send + Sync + 'static`。返回的是共享引用；更新须通过数据自身的安全接口，例如原子字段。
- 借用不能逃逸或跨越异步等待点（await）。独立 `block_on` 根计算不因此自动成为本库任务。

## 错误

创建失败的 `BuildError` 保留原计算、调度对象、附加数据及选项；执行拒绝的 `RunError` 保留同一 `Runnable`。

结果失败使用 `TaskError`，区分取消、放弃、轮询恐慌（panic）、所属域关闭及清理未确认等情况。原恐慌载荷不保证是字符串，也不保证 `Sync`。

带原值的错误沿 `pi_result::RawResult` 返回；需要报告链时使用pi_result的标准入口，不为方便包装而丢失原值或放宽线程约束。

## 构建与验证

在准备好完整工作区依赖后，从仓库根逐库执行：

```sh
cargo +nightly-2026-07-19 build -p pi-async-task
cargo +nightly-2026-07-19 test -p pi-async-task -- --test-threads=1
cargo +nightly-2026-07-19 test -p pi-async-task --all-features -- --test-threads=1
cargo +nightly-2026-07-19 doc -p pi-async-task --no-deps
```

这些是验证入口，不是本文新增执行的通过记录。未声明稳定工具链的最低支持版本，也没有由本README证明并发、内存安全或性能达标。

源码入口：[公开导出](src/lib.rs)、[创建与执行](src/runnable.rs)、[控制](src/control.rs)、[结果等待](src/join.rs)、[状态定义](src/definitions.rs)。

许可证（License）：`MIT OR Apache-2.0`，与清单声明一致。
