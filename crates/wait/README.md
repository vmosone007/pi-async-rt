# pi-async-wait

独立于运行时（runtime）的单次交值等待：等待一个值、竞争一个值，或收集全部参与方的结果。

构造函数同步返回标准异步等待值（Future）及交值句柄（sender handle）。本库不创建任务、线程或定时器，可以和调用方的执行器（executor）及异步组合子共同使用。

## 当前状态与接入

清单版本为 `0.1.0`。公开接口已经建立，等待实现及集成验收正在推进；实际能力须按选中的源码提交或发布物核对，不能把本地未提交工作区当成远端已交付内容。本README没有重新执行行为测试，也不宣称正式可用版本已经发布。

```toml
[dependencies]
pi-async-wait = { path = "../pi-async-rt/crates/wait" }
```

没有可选特性（feature）。本包通过工作区继承 `atomic-waker` 与 `pi_result` 依赖；只复制本目录不能替代完整工作区或正式打包产物。

## 三种等待

| 构造函数 | 等待值 | 交值句柄 | 等待结果 |
| --- | --- | --- | --- |
| `wait_one<T>()` | `WaitOne<T>` | 唯一 `OneSender<T>`，不可克隆 | `RawResult<T, WaitError>` |
| `wait_any<T>()` | `WaitAny<T>` | 可克隆 `AnySender<T>` | 首个被接受的值，或全部无值结束错误 |
| `wait_all<T>()` | `WaitAll<T>` | 可克隆 `AllSender<T>` | `Vec<RawResult<T, WaitError>>`，按登记顺序排列 |

`send(self, value)` 同步交值并消耗当前句柄；成功表示已经接受，不承诺等待方已经观察到值。发送可以早于等待端的首次轮询（poll）。

## 示例

示例函数由调用方已有执行器推进；本库没有自己的 `block_on`。

### 唯一交值

```rust
use pi_async_wait::wait_one;

async fn one_value() {
    let (wait, sender) = wait_one();
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

## 关闭、错误与并发

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

## 与超时和组合子的关系

本库提供原值交接，选择组合子可以竞争多个等待，聚合组合子可以组合不同异步计算；二者互补。

本库不内置计时器，也不让普通等待偷偷关联某个运行时。带超时的 `wait_one_timeout/wait_any_timeout/wait_all_timeout` 由 [运行时核心](../runtime-core/README.md) 的 `RuntimeWait` 组合 `timeout_on` 提供。

## 成本边界

`wait_one/wait_any` 构造各需要一次共享分配；库自身的正常 `send` 不新增分配。`wait_any` 的有效克隆只增加共享持有和条件计数。

`wait_all` 初始需要组与首个结果槽，之后每次有效克隆分配一个槽；正常交值不额外分配，最终收集与清理为O(n)，结果向量另有空间成本。

同步交值不等待接收者或其它交值者；条件原子更新可能因真实竞争重试。通知回调和原值析构可以执行用户代码，其耗时或恐慌不属于固定耗时保证。上述是实现约定，不是基准测试结论。

## 验证入口

```sh
cargo +nightly-2026-07-19 build -p pi-async-wait
cargo +nightly-2026-07-19 test -p pi-async-wait -- --test-threads=1
cargo +nightly-2026-07-19 doc -p pi-async-wait --no-deps
```

本README未执行这些命令，也不宣称稳定工具链最低版本、并发安全专项或性能已通过。

源码入口：[公开导出](src/lib.rs)、[唯一等待](src/one.rs)、[任一等待](src/any.rs)、[全部等待](src/all.rs)、[错误](src/definitions.rs)。

许可证（License）：`MIT OR Apache-2.0`。
