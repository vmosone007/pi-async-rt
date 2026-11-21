# pi-async-runtime-core

与具体执行器、队列和定时轮实现分离的运行时核心（runtime core）接口及组合能力。

## 目标、适用范围与支持条件

对于需要接收多种运行时（runtime）的库（crate），本库提供可组合的能力特征（trait）。应用使用默认组合时，可从 `pi-async-runtime` 导入部分常用能力。输入输出驱动（I/O driver）、线程池（thread pool）和实际队列（queue）由具体实现选择。

清单版本为 `0.1.1`，采用 Rust 2021。相关配置在 Linux x86_64 和 Windows x64/MSVC 上使用过 `nightly-2026-07-19`。最低支持 Rust 版本（MSRV）、Android 和 WebAssembly（Wasm）尚未确认。部分运行时（runtime）组合及完整计时取消接续还在开发。以下接口（API）可用于约束调用方，行为以实际运行时已提供的能力为准。

## 使用前须了解的特性和术语

- **运行时编号（RuntimeId）**标识本进程内同一个逻辑服务。克隆（Clone）共享访问时保持原编号。
- **待提交值（UnsubmittedTask）**拥有一次合法提交的原任务（Task）。结果句柄（TaskHandle）可以独立等待结果。
- **根异步计算（root Future）**直接交给 `block_on` 推进。另行派发（dispatch）的任务（Task）有各自身份和结果。

## 公开特性（feature）及组合

| 特性（feature） | 默认 | 作用 |
| --- | --- | --- |
| `observe` | 关闭 | 启用任务（Task）库的观测入口，使任务配置可携带观测者。 |

`observe` 可独立启用。清单未声明互斥组合。

## 能力、类型与接口（API）

### 接入和组合能力

```toml
[dependencies]
pi-async-runtime-core = { path = "../pi-async-rt/crates/runtime-core" }
```

使用路径依赖（path dependency）时保留完整工作区（workspace）及根清单。

| 接口 | 主要能力 |
| --- | --- |
| `TaskDispatch` | `create/submit/spawn` 及带 `TaskConfig` 的版本。 |
| `LocalTaskDispatch` | 静态关联的 `create_local/submit_local/spawn_local`。 |
| `IdleDispatch` | `idle/idle_local/submit_idle`。 |
| `TimedTaskDispatch` | `spawn_timing/spawn_timing_local` 及配置版本。 |
| `RuntimeTime` | `timeout/timeout_on/sleep`。 |
| `RuntimeWait` | 三种带超时等待，由普通等待和计时能力组合。 |
| `RuntimeQuery` | 身份、状态、推进样本、最近空闲判断及七项任务数量。 |
| `RuntimeControl` | 同步非阻塞地请求关闭。 |
| `RuntimeContext` | 取得／同步借用当前共享访问及显式选择原服务。 |
| `RuntimeBlockOn` | 在合法上下文推动一个根计算直到返回。 |
| `Runtime` | 聚合上述共同能力；不包含执行器的 `run/run_once`。 |

只需要某一项能力时使用最小约束；不必强制所有调用者实现整个聚合接口。

```rust
use pi_async_runtime_core::{RuntimeQuery, RuntimeStatus};

fn is_open<R: RuntimeQuery + ?Sized>(runtime: &R) -> bool {
    // 这里只需查询能力。新的提交须独立检查自己的结果。
    runtime.status() == RuntimeStatus::Open
}
```

这是接口使用示例；状态样本不保证之后的提交一定成功。

### 创建、提交及任务句柄

`create` 返回待提交值与结果句柄，创建不执行也不隐式入队；`submit` 只接受创建它的原运行时目标。拒绝返回同一待提交原值和原因。

`spawn` 组合创建与提交。创建失败返还原输入；提交失败保留待提交值和对应结果句柄，不能把“已经创建”当成“已经接收”。

普通计算及输出要求 `Send + 'static`，不要求 `Sync/Unpin`。本地计算和输出不额外要求 `Send/Sync`，但须存在有效原线程本地权限。`idle_local` 仍要求其计算和输出可以跨线程转交。

`TaskHandle` 组合结果等待及任务控制；`TaskControl`、`TaskNotifier`、`TaskQuery` 等提供对应窄能力。句柄释放、取消请求与原计算清理分别遵守任务库约定。

### 计时、等待和主动让出

| 能力 | 使用语义 |
| --- | --- |
| `timeout(duration)` | 返回等待值；正时长首次轮询（poll）才固定截止时间并登记，通过 `TimeoutControl` 请求取消。 |
| `timeout_on(future, duration)` | 组合业务计算与超时；同轮都就绪时业务结果优先，完成或放弃时处理另一份等待兴趣。 |
| `sleep(duration)` | 短时让出后等待唤醒；正时长范围为1ms至1s，不交付独立控制句柄。 |
| `wait_one_timeout/wait_any_timeout` | 区分等待错误、超时和计时设施错误；失败关闭内部等待，后续未接受交值返还原值。 |
| `wait_all_timeout` | 正常返回完整逐项结果向量；超时不交付部分向量。 |

正超时支持至少1ms，也可超过1s。零超时让出一次，与 `yield_now` 保持对应语义，不创建正时长登记，其取消不成功。等待值是惰性的，不在仅构造时启动计时。

`yield_now` 是自由异步函数。首次轮询（poll）安排再次唤醒（wake-up）并挂起（Pending），随后一次轮询完成。普通 `wait_one/wait_any/wait_all` 重导出自独立等待库。

计时组合只依赖 `TimerAccess` 等抽象；不为实现方便给外部计算添加 `Unpin/Send/Sync/'static` 约束。

下例展示通用调用的类型（type）约束。调用这段异步函数需要一个提供实际计时服务的运行时（runtime）。

```rust
use std::time::Duration;
use pi_async_runtime_core::{RuntimeTime, TimeoutError};

async fn answer_before_deadline<R: RuntimeTime>(runtime: &R) -> Result<u32, TimeoutError> {
    // 等待由调用方推进。当业务与定时在同一轮都就绪时，优先返回业务结果。
    runtime.timeout_on(async { 42 }, Duration::from_millis(20)).await
}
```

### 查询、当前访问与关闭

七项数量分别是外部、内部、定时登记、空闲任务、在途、等待唤醒和待取消。各项独立取样，不承诺全局同一瞬间；抽象接口不统一规定所有实现都扫描或都是O(1)。

`last_idle_state` 读取最近一次判断，不主动调用用户策略。`is_driving` 反映推进权的实际样本，不等于线程或任务数量。

`RuntimeContext::enter` 只选择公共目标，不授予本地队列访问或推进权限；同步闭包返回异步计算不会让本次选择范围跨越等待点。当前访问应核身份、类型、原线程及有效性。

`request_close` 只报告 `Requested/AlreadyRequested/AlreadyClosed`，不等待完整清理。`RuntimeStatus::Closed` 说明服务不可用；只有对应 `CloseState::Completed` 才说明规定清理已完成。失败时允许 `Closed/Requested`，不能用重复请求或数量归零伪造成功。

`block_on<F: Future>(&self, F) -> F::Output` 不额外要求 `Unpin/Send/Sync/'static`。完整运行时与共享句柄能推进的范围不同，具体实现必须说明；非法嵌套与推进冲突在首次轮询前拒绝。

### 错误与原值

普通错误报告使用pi_result；需要返还原计算、待提交值或输出时使用 `RawResult` 及带原值的错误，不只返回文字。

许可证（License）为 `MIT OR Apache-2.0`。
