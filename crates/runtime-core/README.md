# pi-async-runtime-core

与具体执行器、队列和定时轮实现分离的运行时核心（runtime core）接口及组合能力。

下游若希望接收任意符合合同的运行时，应依赖本库的能力接口（trait），而不是绑定某个默认执行器类型。应用使用默认组合时，也可从 [pi-async-runtime](../runtime/README.md) 导入其重导出的常用接口。

## 当前状态与接入

清单版本为 `0.1.0`。当前公开形态及组合壳已建立，但大量核心操作仍是未实现占位（todo!）；不能把该包作为已可运行的通用运行时。

```toml
[dependencies]
pi-async-runtime-core = { path = "../pi-async-rt/crates/runtime-core" }
```

默认不启用特性（feature）；`observe` 转发任务库的观测接入。没有内置I/O驱动、线程池或全局默认运行时。

本包继承完整工作区的配置，依赖任务、队列、普通等待、pi_result及atomic-waker。

## 能力接口与可组合性

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
    runtime.status() == RuntimeStatus::Open
}
```

这是接口使用示例；状态样本不保证之后的提交一定成功。

## 创建、提交及任务句柄

`create` 返回待提交值与结果句柄，创建不执行也不隐式入队；`submit` 只接受创建它的原运行时目标。拒绝返回同一待提交原值和原因。

`spawn` 组合创建与提交。创建失败返还原输入；提交失败保留待提交值和对应结果句柄，不能把“已经创建”当成“已经接收”。

普通计算及输出要求 `Send + 'static`，不要求 `Sync/Unpin`。本地计算和输出不额外要求 `Send/Sync`，但须存在有效原线程本地权限。`idle_local` 仍要求其计算和输出可以跨线程转交。

`TaskHandle` 组合结果等待及任务控制；`TaskControl`、`TaskNotifier`、`TaskQuery` 等提供对应窄能力。句柄释放、取消请求与原计算清理分别遵守任务库约定。

## 计时与等待

| 能力 | 使用语义 |
| --- | --- |
| `timeout(duration)` | 返回等待值；正时长首次轮询（poll）才固定截止时间并登记，通过 `TimeoutControl` 请求取消。 |
| `timeout_on(future, duration)` | 组合业务计算与超时；同轮都就绪时业务结果优先，完成或放弃时处理另一份等待兴趣。 |
| `sleep(duration)` | 短时让出后等待唤醒；正时长范围为1ms至1s，不交付独立控制句柄。 |
| `wait_one_timeout/wait_any_timeout` | 区分等待错误、超时和计时设施错误；失败关闭内部等待，后续未接受交值返还原值。 |
| `wait_all_timeout` | 正常返回完整逐项结果向量；超时不交付部分向量。 |

正超时支持至少1ms，也可超过1s。零超时让出一次，与 `yield_now` 保持对应语义，不创建正时长登记，其取消不成功。等待值是惰性的，不在仅构造时启动计时。

`yield_now` 是自由异步函数，通过当前有效本地能力让出一次；它不等于永久挂起。普通 `wait_one/wait_any/wait_all` 重导出自独立等待库，不依赖本库选定某个执行器。

计时组合只依赖 `TimerAccess` 等抽象；不为实现方便给外部计算添加 `Unpin/Send/Sync/'static` 约束。

## 查询、当前访问与关闭

七项数量分别是外部、内部、定时登记、空闲任务、在途、等待唤醒和待取消。各项独立取样，不承诺全局同一瞬间；抽象接口不统一规定所有实现都扫描或都是O(1)。

`last_idle_state` 读取最近一次判断，不主动调用用户策略。`is_driving` 反映推进权的实际样本，不等于线程或任务数量。

`RuntimeContext::enter` 只选择公共目标，不授予本地队列访问或推进权限；同步闭包返回异步计算不会让本次选择范围跨越等待点。当前访问应核身份、类型、原线程及有效性。

`request_close` 只报告 `Requested/AlreadyRequested/AlreadyClosed`，不等待完整清理。`RuntimeStatus::Closed` 说明服务不可用；只有对应 `CloseState::Completed` 才说明规定清理已完成。失败时允许 `Closed/Requested`，不能用重复请求或数量归零伪造成功。

`block_on<F: Future>(&self, F) -> F::Output` 不额外要求 `Unpin/Send/Sync/'static`。完整运行时与共享句柄能推进的范围不同，具体实现必须说明；非法嵌套与推进冲突在首次轮询前拒绝。

## 错误与验证

普通错误报告使用pi_result；需要返还原计算、待提交值或输出时使用 `RawResult` 及带原值的错误，不只返回文字。

```sh
cargo +nightly-2026-07-19 build -p pi-async-runtime-core --all-features
cargo +nightly-2026-07-19 test -p pi-async-runtime-core --all-features -- --test-threads=1
cargo +nightly-2026-07-19 doc -p pi-async-runtime-core --no-deps
```

命令是未来验证入口；本README未执行，当前占位不会因构建成功变成可运行实现。未提供本轮行为、安全专项或性能通过结论。

源码入口：[能力导出](src/lib.rs)、[派发](src/dispatch.rs)、[计时](src/time.rs)、[普通与计时等待组合](src/wait.rs)、[当前环境](src/context/access.rs)、[查询](src/query.rs)。

许可证（License）：`MIT OR Apache-2.0`。
