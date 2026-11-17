# pi-async-runtime

由外部线程推动的单线程串行运行时（serial runtime）与单线程并发运行时（concurrent runtime）。

本库组合任务、队列、计时、运行时核心和默认执行器，并重导出常用公开接口（public API）。应用通常从本库或其 `prelude` 导入能力接口。

## 当前状态

清单版本为 `0.1.0`。目前仍是含未实现占位（todo!）的开发版本，运行时及其上游完整实现、测试和验收尚未结束。不能作为已可用的运行时版本部署。

GitHub开发分支、工作区中的未提交实现和注册表发布物不等同。本文的接口说明不能替代真实可运行版本，也不沿用旧 `pi_async_rt` 的历史测试或性能结论。

## 接入与依赖

```toml
[dependencies]
pi-async-runtime = { path = "../pi-async-rt/crates/runtime" }
```

默认特性（feature）为空；`observe` 转发执行器和运行时核心的观测接入，不自动安装全局日志或指标。

本库依赖以下项目内库，均位于同一Git仓库：

| 库 | 职责 |
| --- | --- |
| [pi-async-task](../task/README.md) | 原任务、结果与清理责任。 |
| [pi-async-task-queue](../task-queue/README.md) | 本地、普通、空闲和定时来源。 |
| [pi-async-task-wake](../task-wake/README.md) | 经执行器间接接入的固定目标唤醒路由。 |
| [pi-async-wait](../wait/README.md) | 经核心接入的普通交值等待。 |
| [pi-async-runtime-core](../runtime-core/README.md) | 统一能力接口与通用组合。 |
| [pi-async-executor](../executor/README.md) | 默认事件循环、策略、线程通知及计时接入。 |

使用完整工作区需保留根 `Cargo.toml`。当前根清单还把 `pi-clock-manual` 指向仓库旁的 `../pi-clock/crates/manual`；工作区的可选任务池适配引用 `../pi_task_pool`。克隆本仓库本身不自动带来这些兄弟目录。

## 三个常用名称

| 名称 | 行为 |
| --- | --- |
| `SerialRuntime` | 已接纳任务族尚未结束时暂停接纳下一普通外部批次；本地任务可继续交替推进。 |
| `ConcurrentRuntime` | 每轮积极尝试普通来源；已有任务挂起时仍可接纳其它任务。 |
| `Runtime` | `ConcurrentRuntime` 的默认类型别名。 |

两种实现都是单线程执行：并发（concurrency）指多个异步计算交替推进，不代表CPU并行（parallelism）。它们不创建后台工作线程，不提供动态线程池或I/O反应器。

串行运行时仍允许外部线程持续并发提交。暂停的是下一批普通任务的接纳，不是外部来源的接收。

## 常用入口

```rust
use pi_async_runtime::prelude::*;
use pi_async_runtime::{RuntimeCapabilities, RuntimeStatus};

fn runtime_is_open<R: RuntimeCapabilities>(runtime: &R) -> bool {
    runtime.status() == RuntimeStatus::Open
}
```

这只是类型约束示例，不实例化尚未实现的服务。完整聚合接口在本库重导出为 `RuntimeCapabilities`，以区分默认具体类型 `Runtime`。只需部分能力时可用更窄的接口约束。

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

## 构造、配置与推进

`new()` 使用默认配置；`with_options(RuntimeOptions)` 设置四组工作配置：普通来源、空闲来源、定时来源及执行器。`with_components` 支持静态组合时钟、策略和钩子，由参数推导具体类型，不必在常规调用处手写内部泛型链。

当前默认时钟要求宿主事先按pi_clock公开接口安装全局时间轴；运行时构造不重置时钟、不自动创建线程。

| 推动方式 | 作用与权限 |
| --- | --- |
| 完整对象的 `run_once(&mut self)` | 推进一次事件循环，返回 `TurnResult`。 |
| 完整对象的 `run(&mut self)` | 在原合法线程持续推进服务。 |
| 完整对象的 `block_on(&self, future)` | 排他推进原服务及调用方根计算，根计算完成后返回。 |
| 共享 `Handle` 的 `block_on` | 只推进调用线程上的根计算，不代替拥有者推进原服务。 |

完整运行时绑定原线程，不支持 `Send/Sync`；`Handle` 支持 `Clone + Send + Sync`，克隆不复制服务，也不延长本地管理所有权。

不要在正在轮询（poll）的本地任务内嵌套 `block_on`。非法嵌套、目标冲突或非法推进会在首次轮询根计算前恐慌（panic）。

串行任务等待自己产生的子任务时应使用有效的本地派发；若把子任务提交到被暂停接纳的普通外部来源再等待它，可能形成互相等待。

## 计时、空闲与结果

正时长 `timeout` 在首次轮询才开始登记；`timeout_on` 同轮优先观察业务完成，业务完成后处理不再需要的计时等待。正超时支持至少1ms，也允许超过1s；`sleep` 的正时长范围为1ms至1s。

零超时等价于让出一次，不建立正时长登记，不能成功取消。`yield_now` 是惰性异步函数，只有被轮询才让出；它不承诺运行所有其它任务或等待外部通知。

空闲任务只承诺提交，不保证被接纳或执行；初次接纳由外部空闲策略决定。接纳之后与普通任务一样轮询、挂起、唤醒和取消。

派发成功不代表任务已完成；通过结果句柄等待完成。取消请求不等于原计算已经析构（drop），失败提交须处理返回的原值和对应句柄。

## 查询、关闭及成本

查询分别采样身份、运行状态、是否正在推进、最近空闲判断、关闭进度及七类数量，不承诺所有值对应同一个全局瞬间。

默认 `waiting_for_wake_count/pending_cancel_task_count` 按需O(n)扫描登记；其它查询沿对应接口约定。不要在高频路径无条件采集全部统计。

`request_close` 同步非阻塞，只登记请求并返回即时结果。共享句柄释放不自动关闭服务；完整拥有者负责撤销本地权限及合法清理。

`Closed` 表示服务不可用。`Closed/Completed` 表示规定清理成功确认，`Closed/Requested` 仍可能表示清理失败或尚未确认；重复关闭请求不能把失败变成成功。

## 构建与验证

```sh
cargo +nightly-2026-07-19 build -p pi-async-runtime
cargo +nightly-2026-07-19 build -p pi-async-runtime --all-features
cargo +nightly-2026-07-19 doc -p pi-async-runtime --no-deps
cargo +nightly-2026-07-19 test -p pi-async-runtime --all-features -- --test-threads=1
```

这些是验证入口，本README没有执行。已有骨架构建记录不证明业务实现、安全、并发或性能通过；当前也没有稳定工具链最低版本承诺。

源码入口：[公开导出与预导入](src/lib.rs)、[实例与句柄](src/instance.rs)、[配置](src/options.rs)、[推进](src/driving.rs)、[能力组合](src/capabilities.rs)。

许可证（License）：`MIT OR Apache-2.0`。
