# pi-async-wait

独立于运行时（runtime）的单次交值等待，可等待一个值、竞争一个值，或收集全部参与方的结果。

## 目标、适用范围与支持条件

构造函数（constructor）同步返回实现了标准异步计算（Future）的等待值与交值句柄（sender handle）。执行器（executor）和异步组合由调用方选择，计时由上层提供。

清单版本为 `0.1.1`，采用 Rust 2021。本仓以 Linux x86_64 和 Windows x64/MSVC 为验证目标，相关构建或用例使用 `nightly-2026-07-19`。最低支持 Rust 版本（MSRV）、Android 和 WebAssembly（Wasm）尚未确认。三种等待已有实现和定向测试（test），整批仍待完成合并验证（verification）。

## 使用前须了解的特性和术语

- **交值句柄（sender handle）**只能交值一次，`send(self, value)` 消耗这一份句柄（handle）。
- **等待值（wait value）**实现异步计算（Future），由调用方推进。交值可先于首次轮询（poll），交值成功表示值已经接收。
- **登记顺序（registration order）**决定 `wait_all` 的结果位置。并发交值（concurrent sending）的到达顺序不改变这些位置。

## 公开特性（feature）及组合

本库没有可选特性（feature）。三种等待在同一配置可同时使用。

## 能力、类型与接口（API）

### 接入和选择等待方式

```toml
[dependencies]
pi-async-wait = { path = "../pi-async-rt/crates/wait" }
```

使用上述路径时保留完整工作区（workspace）及根清单。

| 构造函数（constructor） | 等待值 | 交值句柄（sender handle） | 等待结果 |
| --- | --- | --- | --- |
| `wait_one<T>()` | `WaitOne<T>` | 唯一 `OneSender<T>`，不可克隆 | `RawResult<T, WaitError>` |
| `wait_any<T>()` | `WaitAny<T>` | 可克隆 `AnySender<T>` | 首个被接受的值，或全部无值结束错误 |
| `wait_all<T>()` | `WaitAll<T>` | 可克隆 `AllSender<T>` | `Vec<RawResult<T, WaitError>>`，按登记顺序排列 |

`send(self, value)` 同步交值并消耗当前句柄；成功表示已经接受，不承诺等待方已经观察到值。发送可以早于等待端的首次轮询（poll）。

下面的异步函数由调用方已有的执行器（executor）推进。

### 唯一交值

```rust
use pi_async_wait::wait_one;

async fn one_value() {
    let (wait, sender) = wait_one();
    // 在首次轮询等待值之前交值，等待端随后领取同一值。
    sender.send(42_u32).unwrap();
    assert_eq!(wait.await.unwrap(), 42);
}
```

### 多方竞争一个值

```rust
use pi_async_wait::{wait_any, SendErrorKind};

async fn first_accepted_value() {
    let (wait, first) = wait_any();
    let second = first.clone();

    // 先交出首个值。后来尝试会收到未接收的原值。
    first.send("first").unwrap();
    let error = second.send("second").unwrap_err();
    let (kind, original_value) = error.into_parts();

    assert_eq!(kind, SendErrorKind::ValueAlreadyAccepted);
    assert_eq!(original_value, "second");
    assert_eq!(wait.await.unwrap(), "first");
}
```

并发竞争的胜者由接受许可决定，不承诺最早调用 `send` 的线程获胜；胜出后不需要等待其它交值句柄释放。

### 等待全部参与方

```rust
use pi_async_wait::{wait_all, WaitError};

async fn all_values() {
    let (wait, first) = wait_all();
    let second = first.clone();
    let third = first.clone();

    // 故意逆序交值，结果仍按最初的登记顺序排列。
    second.send(20).unwrap();
    first.send(10).unwrap();
    drop(third);

    assert_eq!(
        wait.await,
        vec![Ok(10), Ok(20), Err(WaitError::SenderDropped)]
    );
}
```

每份有效克隆登记一个结果位置；初始句柄在首位，并发克隆按登记成功顺序排列，不按交值到达时间排列。所有有效句柄都必须交值或释放；保存一份永远不结束的有效句柄会使 `wait_all` 继续等待。

### 关闭、错误与并发

- 放弃等待值立即停止接收；外部交值句柄仍可安全释放。
- 唯一交值方无值释放，使 `wait_one` 返回 `WaitError::SenderDropped`。
- `wait_any` 只有全部有效交值方均无值结束且没有胜出者时才返回该错误。
- `wait_all` 的无值释放只影响自己的结果位置，不使其它位置失败。
- 接收关闭后的 `send` 返回 `SendErrorKind::ReceiverClosed` 和同一个原输入值。
- `wait_any` 已有胜者时保留 `ValueAlreadyAccepted`，不被后续关闭改写。
- 关闭后的克隆只保留相应拒绝原因，不新增有效参与责任；关闭竞争不会重新打开接收。
- 同一等待值只允许一个轮询者；完成后再次轮询会恐慌（panic）。

失败时用 `SendError::kind()` 或消耗错误的 `into_parts()` 取得原因和原值。不会为了描述失败而要求值支持 `Debug/Display/Clone`。

等待和交值不额外要求 `T: Unpin`、`Sync` 或 `'static`；跨线程转交仍要求 `T: Send`。不向其它线程借出共享的 `&T`。

### 与超时和组合子的关系

本库提供原值交接，选择组合子可以竞争多个等待，聚合组合子可以组合不同异步计算；二者互补。

带超时的 `wait_one_timeout/wait_any_timeout/wait_all_timeout` 由使用者选用的运行时（runtime）提供。调用方可通过 `pi-async-runtime-core` 的 `RuntimeWait` 和 `RuntimeTime` 约束这些能力。

### 成本边界

`wait_one/wait_any` 构造各需要一次共享分配；库自身的正常 `send` 不新增分配。`wait_any` 的有效克隆只增加共享持有和条件计数。

`wait_all` 初始需要组与首个结果槽，之后每次有效克隆分配一个槽；正常交值不额外分配，最终收集与清理为O(n)，结果向量另有空间成本。

同步交值不等待接收者或其它交值者；条件原子更新可能因真实竞争重试。通知回调和原值析构可以执行用户代码，其耗时或恐慌不属于固定耗时保证。上述是实现约定，不是基准测试结论。

许可证（License）为 `MIT OR Apache-2.0`。
