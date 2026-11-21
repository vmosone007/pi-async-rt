# pi-async-runtime

由外部线程（thread）推进的串行运行时（serial runtime）与单线程并发运行时（single-threaded concurrent runtime）。

## 目标、适用范围与支持条件

本库组合任务（Task）、队列（queue）、计时和默认执行器（executor），面向由所属线程（owner thread）推进异步业务的应用。应用可从本库或 `prelude` 导入常用公开接口（API）。

清单版本为 `0.1.1`，采用 Rust 2021。相关配置在 Linux x86_64 和 Windows x64/MSVC 上使用过 `nightly-2026-07-19`。最低支持 Rust 版本（MSRV）、Android 和 WebAssembly（Wasm）尚未确认。当前完整的构建、派发（dispatch）、推进与关闭组合仍在开发及验证（verification）中。

## 使用前须了解的特性和术语

- **拥有者（owner）**是在所属线程（owner thread）推进原服务的完整运行时（runtime）对象。
- **共享句柄（shared handle）**指向同一个运行时（runtime）。可克隆（Clone）、跨线程提交和查询，推进业务仍由拥有者（owner）负责。
- **接纳（admission）**是执行器（executor）取得任务（Task）并承担后续推进的环节。串行与并发模式的区别在于上一项任务（Task）挂起期间怎样接纳新的外部任务。

## 公开特性（feature）及组合

| 特性（feature） | 默认 | 作用 |
| --- | --- | --- |
| `observe` | 关闭 | 转发执行器（executor）和运行时核心（runtime core）的观测能力，允许任务配置携带观测者。 |

`observe` 可独立启用。清单未声明互斥组合。默认定时来源由依赖库的 `cancel-timer` 提供。

## 能力、类型与接口（API）

### 接入和选择运行时

```toml
[dependencies]
pi-async-runtime = { path = "../pi-async-rt/crates/runtime" }
```

使用路径依赖（path dependency）时保留完整工作区（workspace）和根清单，并准备清单引用的 `pi-clock` 兄弟目录。

| 名称 | 行为 |
| --- | --- |
| `SerialRuntime` | 已接纳任务族尚未结束时暂停接纳下一普通外部批次；本地任务可继续交替推进。 |
| `ConcurrentRuntime` | 每轮积极尝试普通来源；已有任务挂起时仍可接纳其它任务。 |
| `Runtime` | `ConcurrentRuntime` 的默认类型别名。 |

两种实现都是单线程执行：并发（concurrency）指多个异步计算交替推进，不代表CPU并行（parallelism）。它们不创建后台工作线程，不提供动态线程池或I/O反应器。

串行运行时仍允许外部线程持续并发提交。暂停的是下一批普通任务的接纳，不是外部来源的接收。

### 常用入口与组合约束

```rust
use pi_async_runtime::{RuntimeCapabilities, RuntimeStatus};

fn runtime_is_open<R: RuntimeCapabilities>(runtime: &R) -> bool {
    // 通用调用方只依赖标准能力。状态采样不保证后来提交仍可接收。
    runtime.status() == RuntimeStatus::Open
}
```

这段代码展示通用调用所需的接口（API）约束。`RuntimeCapabilities` 重导出核心的 `Runtime` 特征（trait），具体类型（type）使用 `Runtime` 别名。只用部分能力的调用者可使用对应的特征（trait）约束。

| 需求 | 入口 |
| --- | --- |
| 普通派发 | `TaskDispatch::{create, submit, spawn}` 及配置版本。 |
| 本地快速派发 | `LocalTaskDispatch::{create_local, submit_local, spawn_local}`，静态关联入口。 |
| 空闲派发 | `IdleDispatch::{idle, idle_local, submit_idle}`。 |
| 定时派发 | `TimedTaskDispatch::{spawn_timing, spawn_timing_local}`。 |
| 计时等待 | `RuntimeTime::{timeout, timeout_on, sleep}`。 |
| 普通交值等待 | 自由函数 `wait_one/wait_any/wait_all`。 |
| 带超时交值等待 | `RuntimeWait::{wait_one_timeout, wait_any_timeout, wait_all_timeout}`。 |
| 主动让出一次 | 自由异步函数 `yield_now`，需要实际等待它。 |
| 查询与关闭请求 | `RuntimeQuery` 与 `RuntimeControl`。 |
| 当前服务访问 | `RuntimeContext`；公共选择与本地操作权限分别核对。 |
| 当前任务信息 | `try_current_task_id/try_with_current_metadata`。 |

普通派发要求计算与输出 `Send + 'static`，不额外要求 `Sync/Unpin`。本地派发允许非 `Send` 计算，但需要当前有效的所属线程权限。`idle_local` 的内容仍要求 `Send`，不要求 `Sync`。

### 构造、配置与推进

`new()` 使用默认配置；`with_options(RuntimeOptions)` 设置四组工作配置：普通来源、空闲来源、定时来源及执行器。`with_components` 支持静态组合时钟、策略和钩子，由参数推导具体类型，不必在常规调用处手写内部泛型链。

当前默认时钟要求宿主事先按pi_clock公开接口安装全局时间轴；运行时构造不重置时钟、不自动创建线程。

配置可以先独立准备，再交给所选的运行时（runtime）构造入口。

```rust
use std::num::NonZeroUsize;
use pi_async_runtime::RuntimeOptions;

let mut options = RuntimeOptions::default();
// 只调整普通来源的每步管理工作额度。
options.external_queue.cleanup_batch_size = NonZeroUsize::new(32).unwrap();
assert_eq!(options.external_queue.cleanup_batch_size.get(), 32);
```

#### 分别选择三类队列

两种单线程运行时（single-threaded runtime）均可用 `with_queues` 指定队列（queue）。每个缺项使用该运行时的默认队列。已有具象实例的配置随实例保留。构造入口按值接收选配。构造失败时，创建线程负责释放已移交的管理者和未调用的定时回调（callback），调用方收到原报告（Report）。`RuntimeOptions` 中对应的队列选项只用于未指定的来源。

此例直接使用下列依赖。路径按实际工作区布局填写。

```toml
[dependencies]
pi-async-runtime = { path = "../pi-async-rt/crates/runtime" }
pi-async-task-queue = { path = "../pi-async-rt/crates/task-queue" }
pi_result = "0.2"
```

```no_run
use pi_async_runtime::{RuntimeCapabilities, RuntimeOptions, RuntimeQueues, SerialRuntime};
use pi_async_task_queue::MpscTaskQueue;

fn serial_with_external_queue() -> pi_result::Result<impl RuntimeCapabilities> {
    let options = RuntimeOptions::default();
    // 将此实例移交给运行时，空闲和定时任务队列各自沿用默认。
    let external = MpscTaskQueue::new(options.external_queue)?;
    let queues = RuntimeQueues::default().with_external(external);
    SerialRuntime::with_queues(options, queues)
}
```

`with_idle` 接收选定的空闲任务队列（idle task queue）管理者。`with_timer` 的构造回调（callback）接收本次最终的运行时编号（RuntimeId）、时钟（clock）和线程通知句柄（ThreadNotifier），返回同源的定时设施。原已准备实例须先确认原通知配对、时间和接收状态。需要同时指定时钟（clock）、策略和钩子（hook）时使用 `with_components_and_queues`。内部任务队列（internal task queue）由原执行器（executor）创建，不配置。

选配后通过原运行时的派发（dispatch）入口提交，运行时会核对任务归属并安排通知（notification）。构造前保留的原队列共享句柄（shared handle）仍可按该队列的约定使用，直接提交者负责核实任务归属和通知责任。本地操作仍遵守原所属线程的权限。运行此构造前，宿主须先安装默认时钟所用的全局时间轴。

| 推动方式 | 作用与权限 |
| --- | --- |
| 完整对象的 `run_once(&mut self)` | 推进一次事件循环，返回 `TurnResult`。 |
| 完整对象的 `run(&mut self)` | 在原合法线程持续推进服务。 |
| 完整对象的 `block_on(&self, future)` | 排他推进原服务及调用方根计算，根计算完成后返回。 |
| 共享 `Handle` 的 `block_on` | 只推进调用线程上的根计算，不代替拥有者推进原服务。 |

完整运行时绑定原线程，不支持 `Send/Sync`；`Handle` 支持 `Clone + Send + Sync`，克隆不复制服务，也不延长本地管理所有权。

不要在正在轮询（poll）的本地任务内嵌套 `block_on`。非法嵌套、目标冲突或非法推进会在首次轮询根计算前恐慌（panic）。

串行任务等待自己产生的子任务时应使用有效的本地派发；若把子任务提交到被暂停接纳的普通外部来源再等待它，可能形成互相等待。

### 计时、空闲与结果

正时长 `timeout` 在首次轮询才开始登记；`timeout_on` 同轮优先观察业务完成，业务完成后处理不再需要的计时等待。正超时支持至少1ms，也允许超过1s；`sleep` 的正时长范围为1ms至1s。

零超时等价于让出一次，不建立正时长登记，不能成功取消。`yield_now` 是惰性异步函数，只有被轮询才让出；它不承诺运行所有其它任务或等待外部通知。

空闲任务只承诺提交，不保证被接纳或执行；初次接纳由外部空闲策略决定。接纳之后与普通任务一样轮询、挂起、唤醒和取消。

派发成功不代表任务已完成；通过结果句柄等待完成。取消请求不等于原计算已经析构（drop），失败提交须处理返回的原值和对应句柄。

### 查询、关闭及成本

查询分别采样身份、运行状态、是否正在推进、最近空闲判断、关闭进度及七类数量，不承诺所有值对应同一个全局瞬间。

默认 `waiting_for_wake_count` 和 `pending_cancel_task_count` 只统计已接纳任务，按需扫描登记，成本为 O(n)，n 为登记规模。其它查询沿各自约定取样。

上述两项查询与关闭并发时，已开始的查询可能在调用线程释放任务附加数据（task metadata）。启用 `observe` 且配置观测接收者（TaskObserver）时，同一次释放也可能报告 `TaskEvent::ReclaimStarted`。析构（drop）或该回调发生恐慌（panic）时，原载荷沿查询传播。已绑定任务的原异步计算（Future）仍由所属线程清理。

`request_close` 同步非阻塞，只登记请求并返回即时结果。共享句柄释放不自动关闭服务；完整拥有者负责撤销本地权限及合法清理。

`Closed` 表示服务不可用。`Closed/Completed` 表示规定清理成功确认，`Closed/Requested` 仍可能表示清理失败或尚未确认；重复关闭请求不能把失败变成成功。

许可证（License）为 `MIT OR Apache-2.0`。
