基于Future(MVP)，用于为外部提供基础的通用异步运行时和工具

# 主要特征

- 任务池: 可定制的任务池
- 任务ID: 外部使用任务ID可以很方便的唤醒和挂起
- 抽象接口: 可以自由实现自己的运行时
- 运行时推动：单线程运行时可以用自己的方式推动运行

# Examples

本地异步运行时:
```
 use pi_async_rt::rt::{AsyncRuntime, AsyncRuntimeExt, serial_local_thread::{LocalTaskRunner, LocalTaskRuntime}};
 let rt = LocalTaskRunner::<()>::new().into_local();
 let _ = rt.block_on(async move {});
```

多线程异步运行时使用:
```
 use pi_async_rt::rt::{AsyncRuntime, AsyncRuntimeExt};
 use pi_async_rt::rt::multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder, StealableTaskPool};

 let pool = StealableTaskPool::with(4,100000,[1, 254],3000);
 let builer = MultiTaskRuntimeBuilder::new(pool)
     .set_timer_interval(1)
     .init_worker_size(4)
     .set_worker_limit(4, 4);
 let rt = builer.build();
 let _ = rt.spawn(async move {});
```

<a id="single-task-pool-owner"></a>

# 单线程任务池所有者修复

默认及 `serial` 的 `SingleTaskPool` 已修复外部线程误认领 owner 的并发安全问题。
原 `get_thread_id()` 会在陌生线程写入目标运行时编号，使外部 `spawn_local`、高优先级
提交或已经 Pending 的任务 Waker 错误访问无同步的本地队列。一个消费者加一个合法
外部发送者即可触发，不需要多个消费者，也不是最近 timer/多线程安全修复引入。

这是内部安全 Bug 修复，**公开函数/trait 签名、泛型约束和任务布局不变，但不是所有
可观察行为都不变**：

- 构造、`startup`、提交及查询不认领线程；首次实际消费绑定真实线程，之后不允许
  消费者迁移，即使原线程已退出。构造在 A、首次驱动在 B 仍受支持。
- 内置池的运行时 ID 来自池自身，不继承构造线程上另一个池的展示编号。
- `get_thread_id()` 只读当前线程已有 packed 编号，未初始化返回 `usize::MAX`。
  它不是授权接口，不应依赖查询副作用取得本地访问权。
- 绑定前及外部线程的 local/priority/wake 走现成的公共队列。真实 owner 保留原本地
  FIFO/栈路径，公共回退不承诺外部最高优先级抢占。
- 异线程 `run/run_once/block_on` 在访问 timer 或入队前返回 `PermissionDenied`；
  低层 `try_pop/try_pop_all` 则在接触私有容器前 panic。未启动的原错误顺序保持。
- `block_on` 的预检在提交捕获结果栈地址的任务之前，拒绝时不遗留该任务。它仍是
  同步驱动 API，不是异步等待原语，也不新增任意用户 panic 后的取消保证。

常规外部提交/回填无需修改调用代码。自定义 `AsyncTaskPool` 仍遵守自己的线程/编号
协议，没有新增必需 hook；`pi_v8::VmTaskPool` 不被内置池的授权逻辑接管。Worker
包装使用相同底层修复，其后台 loop 应是唯一实际消费者，外部不要同时 `run/block_on`。
`serial` 下的 `!Send` 值必须在合法 owner 域内创建、使用和销毁；本修复不赋予它们
任意跨线程移动的能力。

```rust
use pi_async_rt::prelude::{AsyncRuntime, SingleTaskRunner};

let runner = SingleTaskRunner::<()>::default();
let runtime = runner.startup().unwrap();
runtime.spawn(async {}).unwrap();
std::thread::spawn(move || runner.run_once().unwrap()).join().unwrap();
```

调度主流程、任务状态机、timer 注册/到期顺序、timeout/yield 输出、context 生命周期、
权重选择及 worker 通知逻辑均保持。`len()` 仍是可运行队列快照，不是所有 Pending
任务数；历史 `try_pop_all()` 不含栈且不扣消费计数，不能把它当成关闭清空接口。

成本与性能：授权新增 O(1) TLS/原子读取及比较，首次绑定一次强 CAS；没有新增热路径
锁、自旋等待、每任务分配或 Arc clone。每池增加一个原子字、每消费者线程一个 TLS 字，
任务本体增量为 0，不能按百万任务乘以该池字段大小。预热后空驱动、本地 FIFO/栈
操作的独立测试在 default/serial、Debug/Release 下均断言 0 次分配请求。

2026-09-06，WSL2/Ryzen 7 H 255，有界 A/B 样本（每场景15个样本、最多1个在途任务，
不是饱和或百万任务基准）：

| 正常CPU放置场景 | 修复前 ns/op 中位 | 修复后 ns/op 中位 | 修复后 ops/s 中位 |
| --- | ---: | ---: | ---: |
| default public Ready | 106.779 | 132.824 | 7,528,775 |
| default local Ready | 97.141 | 123.489 | 8,097,870 |
| serial public Ready | 134.188 | 131.150 | 7,624,834 |
| serial local Ready | 128.509 | 144.125 | 6,938,445 |

这些 ops 包含提交、分配、计时、断言和真实驱动。授权有有限成本，不承诺零性能回退：
serial local 在两组CPU放置中增加约12.15%/20.62%；default public/local 的相对变化
随CPU放置明显波动。完整正/负变化、延迟/CPU/RSS和原始数据保留于本地
`docs/SINGLE_TASK_POOL_OWNER_PERFORMANCE.md`，不得只挑有利数字或外推多worker吞吐。

本轮验收：除多线程运行时外的全量标准回归 Debug/Release 各101项（default54、serial47）
通过；有界 TSan 并发及helper、ASan 合同/并发、统计器5项、聚焦lint均通过。
LSan 明确未启用；历史 TaskId/本地适配器生命周期及 serial unsafe 泛化并未全面重构。
不把本轮结果称作全库无UB/无泄漏认证。多线程运行时、真实V8/其它宿主、非Linux平台
未执行端到端复验；本轮不运行人工观测、无界旧测试或大规模基准。

顺序验证入口（本节范围独立于其它历史章节）：

```sh
bash scripts/test_single_task_pool_owner.sh debug
bash scripts/test_single_task_pool_owner.sh release
env PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p test_single_task_pool_owner_analysis.py -v
cargo bench -p pi-async-rt --locked --offline -j 1 --bench single_task_pool_owner
cargo bench -p pi-async-rt --locked --offline -j 1 --features serial --bench single_task_pool_owner
```

回归脚本验证每个目标的实际通过数量及 rustdoc 清单，改名/少跑不能静默通过；不会自动
执行基准或检测器。完整设计、API摘要/大纲、审查和验收在本地 `docs/SINGLE_TASK_POOL_OWNER_*`
归档中，`docs/` 按仓库约定不加入 Git，不随发布交付。源码中文注释和标准测试随代码保留。

# timeout 等待句柄

`runtime.timeout(ms).await` 使用 timeout 专用等待句柄，不再为每次 timeout 分配普通任务 `TaskId/TaskHandle`。该实现保持公开 API 不变，并保留当前 timer 不支持取消的语义：

- timeout 到期后唤醒等待任务，并在 future 完成后释放等待句柄。
- timeout future 被提前 drop 时会清理 waker，timer 到期后释放内部等待句柄。
- 不修改普通 `spawn`、`spawn_timing`、任务池和 worker loop 的核心语义。
- 内部等待状态使用原子到期标记和 `AtomicWaker`，不引入自旋等待或阻塞锁。

建议验证命令：

```
cargo test --lib timeout_waiter_tests
cargo test --test timeout_waiter
cargo test --test timeout_waiter test_multi_thread_timeout_churn_rss_diagnostic -- --ignored --nocapture
cargo test --features serial --test timeout_waiter
cargo bench --bench timeout_waiter_pi_async -- --nocapture
```

# AsyncValue

`AsyncValue<V>` 是同步非阻塞、只允许设置一次的 single-shot future。当前实现保持公开 API 不变：

- `AsyncValue::new()`、`AsyncValue::set(self, value)` 和 `Future<Output = V>` 签名不变。
- `set()` 保持旧语义：第一次设置成功，后续设置静默失败且不会覆盖已设置值。
- pending 后允许重复 poll，重复 poll 会更新最新 waker，不会 panic。
- set 后唤醒最新 waker；never set 的 future 会继续 pending。
- 当前 API 不表达 sender/receiver 拆分、关闭或取消语义。

建议验证命令：

```
cargo test --test async_value -- --nocapture --test-threads=1
cargo test --features serial --test async_value -- --nocapture --test-threads=1
cargo bench --bench async_value_pi_async -- --nocapture
cargo bench --features serial --bench async_value_pi_async -- --nocapture
```

本地专项基准样例（WSL2 Ubuntu 22.04）：

| 场景 | 样例结果 | 折算指标 |
| --- | --- | --- |
| default pending poll | 4.57 ns/iter | 约 218.82M polls/s |
| default set then ready | 26.92 ns/iter | 约 37.15M ops/s |
| default single-thread runtime 内部 set/await | 147,983.84 ns/iter，128 pairs/iter | 约 864,959 pairs/s |
| default multi-thread runtime 外部 set/await | 651,453.15 ns/iter，256 pairs/iter | 约 392,968 pairs/s |
| default multi-thread runtime 内部 set/await | 952,500.35 ns/iter，256 pairs/iter | 约 268,766 pairs/s |
| default single-thread wait + multi-thread set 跨 runtime | 1,150,093.09 ns/iter，64 pairs/iter | 约 55,648 pairs/s |
| serial pending poll | 4.56 ns/iter | 约 219.30M polls/s |
| serial set then ready | 25.20 ns/iter | 约 39.68M ops/s |
| serial single-thread runtime 内部 set/await | 125,086.93 ns/iter，128 pairs/iter | 约 1.023M pairs/s |

# worker wake/sleep 唤醒协议

多线程 runtime 的 worker 空闲休眠路径会在任务入队或任务 waker 被外部线程触发后即时唤醒 sleeping worker，不再依赖 `worker_sleep_timeout` 超时兜底。该修复保持公开 API 不变：

- 不修改 `AsyncRuntime` trait。
- 不修改 `spawn`、`spawn_local`、`timeout`、`yield_now` 的函数签名和返回语义。
- 每次入队或 wake 最多唤醒一个 worker，避免广播式唤醒风暴。
- worker 休眠注册和外部唤醒使用同一个 condvar predicate，避免 “任务已入队但 worker 继续睡到 timeout” 的 lost wake。
- waits 队列使用有限扫描和 stale entry 清理，不进行无界循环。
- direct worker thread 和 serial worker thread 同步使用二次检查协议。
- `AsyncRuntimeBuilder::default_multi_thread(..., Some(0), ...)` 保持原有 builder fallback 语义，不会构建 0 worker pool；`Some(n > 0)` 仍创建同尺寸 `StealableTaskPool`，避免 worker 数大于 pool slot。

建议验证命令：

```
cargo test --lib worker_waker -- --nocapture --test-threads=1
cargo test --test worker_wakeup -- --nocapture --test-threads=1
cargo test --features serial --lib worker_waker -- --nocapture --test-threads=1
cargo bench --bench worker_wakeup_pi_async -- --nocapture
```

本地专项基准样例（WSL2 Ubuntu 22.04，8 worker）：

- `AsyncValue` 外部 wake：p50 66.705us，p99 137.103us，max 169.398us。
- 外部 `spawn`：p50 65.402us，p99 153.818us，max 213.274us。
- 8 个外部 producer 并发 `spawn` 到 8 worker runtime，1,000,000 个空任务：约 7,139,442 tasks/sec。
- 百万并发空任务资源观测：最大 RSS 80,236 KiB，Swaps 0。

# 多线程任务池并发安全

多线程 runtime 的 `StealableTaskPool` 保持每个 worker 独立的 timer、local queue、stack 和
selector，同时安全共享 pool 级权重刷新时间。该修复不修改任何公开 API 或合法调用语义：

- pool 级刷新时间使用 `AtomicCell<QInstant>`，消除多个 worker 在
  `try_pop_by_weight` 中对 `UnsafeCell<QInstant>` 的无同步读写。
- worker 启动时在私有 TLS 绑定当前 pool 身份；访问 local stack、selector、worker queue 或
  thread waker 前验证当前 worker 确实属于该 pool。
- 合法的跨 runtime `spawn_local` 仍回退到目标 runtime 的 public queue，不会被 owner guard
  拒绝；从错误 runtime worker 直接调用另一 pool 的 owner-only pop/waker API 属于非法上下文，
  现在会在访问 owner-only 状态前 panic，而不是进入潜在 data race/UB。
- 不改变 task 优先级、internal/external steal 顺序、worker wake/sleep、timeout、每 worker
  timer 或 Future poll 流程；热路径没有新增 clone、堆分配、循环、系统调用、同步 mutex 或自旋。
- x86_64 上 `AtomicCell<QInstant>` 由专项测试固定为 lock-free。其它 target 保证线程安全，
  但 crossbeam 可能使用平台 fallback，因此不承诺与 x86_64 相同的性能。

建议验证命令：

```
cargo test --test stealable_task_pool_concurrency -- --nocapture --test-threads=1
cargo test --release --test stealable_task_pool_concurrency -- --nocapture --test-threads=1
cargo bench --bench worker_wakeup_pi_async bench_multi_thread_empty_task_throughput_8_workers -- --nocapture
cargo bench --bench worker_wakeup_pi_async bench_multi_thread_internal_empty_task_throughput_8_workers -- --nocapture
```

本地修复前后交错专项基准样例（WSL2 Ubuntu 22.04，8 worker、8 producer、1,000,000 个空任务，
5 轮中位数）：

| 场景 | 修复前吞吐 | 修复后吞吐 | p50 变化 | p90 变化 | p99 变化 | RSS 变化 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 外部 OS 线程并发 spawn | 5.947M tasks/s | 5.970M tasks/s | -2.42% | -2.64% | -2.72% | +1.58% |
| runtime 内 8 个 producer 并发 spawn_local | 9.233M tasks/s | 9.811M tasks/s | -12.81% | +1.53% | -25.28% | -0.60% |

所有交错样本 `Swap=0`。这些数字用于当前机器的回归基线，不是跨硬件的吞吐或延迟承诺；
完整原始样本、TSan/ASan 和下游真实数据库复验记录保存在本地 `docs/` 验收归档中。

# AsyncTask 调度生命周期

默认 `AsyncTask` 使用私有 managed 调度状态，修复 Future 在 poll 内被唤醒并随后
`Ready` 时，残留队列引用被 worker 永久 `pop -> push` 的问题。旧实现无法区分
“另一个 worker 正在 poll”和“Future 已完成”，可能让多个多线程 runtime worker
在业务负载结束后仍持续占用 CPU。

当前实现保持全部公开 API 签名和合法调用语义不变：

- `AsyncTask::new`、`with_context`、`with_runtime_and_context`、`get_inner`、
  `set_inner` 及 `AsyncTaskPool` trait 均未修改签名。
- 运行时托管任务使用私有 `MANAGED/SCHEDULED/RUNNING/COMPLETED` 状态合并重复
  wake、排除并发 poll，并把完成后的迟到 wake 转为空操作。
- Future 返回 `Pending` 时，运行时先恢复 Future，再发布状态；poll 期间发生的任意
  次 wake 只生成一个后续队列项。
- Future 返回 `Ready` 或 poll 发生 panic 展开时进入终态，不再被陈旧队列项重排。
- 每次真实入队最多通知一个 worker；重复 wake 不 clone、不入队、不通知，避免
  queue/wake 风暴。
- 公开 `get_inner/set_inner` 继续进入兼容手工驱动模式，保留外部自定义 driver 的
  既有取出、恢复和显式复用能力。不得与本库运行时并发手工驱动同一任务。
- default single-thread、WorkerRuntime、StealableTaskPool、ComputationalTaskPool
  和通过 `SingleTaskRunner` 驱动的 `pi_v8::VmTaskPool` 使用该生命周期；独立
  `serial::AsyncTask` 未修改。
- managed 任务池的 `push_keep` 必须成功接收可运行任务；返回 `Err` 时和旧实现一样
  无法保证该次 wake 的执行进度，本库不会在唤醒热路径增加阻塞重试或广播。

状态处理使用内联原子操作，不新增 mutex、condvar wait、自旋锁、系统调用、用户代码
重入或独立堆分配。Future mutex 只覆盖 `Option::take/replace`，不会跨
`Future::poll`、析构、任务池入队或 worker 通知。修复没有新增或修改 `unsafe`、
裸指针、手工引用计数或 FFI。

热路径和内存边界：

- Ready 任务增加一次 poll claim 原子读改写和一次完成状态存储。
- Pending 任务再增加一次收尾原子读改写。
- wake 增加一次状态读取/读改写；重复或迟到 wake 会省去原有 Arc clone、队列 push
  和 worker notify。
- x86_64 上 `AsyncTask<StealableTaskPool<()>, ()>` 从 80B 对齐到 96B；一百万个
  同时存活任务的任务本体净增 16,000,000B，约 15.26MiB。该数字不包含 Arc 头、
  Future、TaskHandle、context、队列和分配器成本。
- 对命中旧永久重排问题的进程，完成任务会被释放，worker 可以重新进入已有 idle
  wait。下游生产环境已经复验：部署修复后，压测结束时多线程 runtime worker 的异常
  CPU 占用恢复正常。该反馈未提供统一数值样本，因此不作为吞吐、延迟或跨机器性能
  基准；正常负载下的具体表现仍取决于硬件、任务形态和竞争程度。

建议验证命令：

```
cargo test --test async_task_scheduling -- --nocapture --test-threads=1
cargo test --test async_task_scheduling_runtime_matrix -- --nocapture --test-threads=1
cargo test --release --test async_task_scheduling -- --nocapture --test-threads=1
cargo test --release --test async_task_scheduling_runtime_matrix -- --nocapture --test-threads=1
cargo test --doc -- --test-threads=1
cargo test --features serial --doc -- --test-threads=1
cargo check --bench async_task_scheduling_pi_async
```

本轮纠偏后 8 worker/8 producer、100,000 个 external 任务的资源和语义探针全部
精确完成，最大 RSS 约 16MiB、swap 0。相对性能信号没有收敛：Ready 报告阶段吞吐
变化为 -16.31%，同一轮 harness 耗时变化为 -0.54%；YieldOnce 报告阶段吞吐变化为
+5.78%，harness 耗时变化为 +10.75%。这些矛盾数据不能证明稳定回退或稳定提升，
本轮也未继续执行纠偏后的 1M/internal/WakeBurst。它们只作为后续同源交错复验基线，
不应表述为性能已经通过或没有影响。

<a id="one-due-timer-runner"></a>
<a id="v058-retained-changes"></a>

# v0.5.8 基线保留项

单到期驱动候选已按用户决定撤回，不提供 `run_once_with_one_due_timer()`、
`SingleTaskRunReport` 或异常消费记账守卫。旧 `run_once()` / `run()` 保持
v0.5.8 的批量到期处理、优先入队、分支内出队、批末计数和异常传播行为。

生产侧仅保留默认及 serial Single 两个旧驱动方法的零增量优化：没有登记或消费
定时项时，跳过 `fetch_add(0, Relaxed)`；非零计数仍在原位置批量提交。
每个空定时阶段最多省去两次原子读改写，不增加任务/运行时字段、锁、Clone 或堆分配。
不改变公共 API 签名、任务调度/唤醒协议或 timeout 语义，也不承诺具体吞吐提升比例。

中文注释按恢复后的实际链路校准。`local_async_runtime::<O>()` 的 `O` 必须与绑定时
一致，当前类型擦除实现不会检查类型，不可通过不同 `O` 探测运行时或期待返回 `None`。

保留验证脚本的有界进程组回收、延迟处理 SIGTERM/SIGINT、绝对截止及成功后复核，
并保留历史证据分析器的严格截止检查和 Python 3.8 兼容测试。它们不进入生产库。
这些工具只保留在不跟踪的 `docs/rollback_validation/`，用于当前工作区本地验证，
不随crate发布。`scripts/` 已恢复v0.5.8基线，没有新增脚本、测试清单或缓存文件。
候选构建、采样、变体和检测器入口已撤回，不能据旧候选记录声明当前版本性能通过。

标准复验严格串行，排除人工观测、ignored RSS和千万任务旧例；默认/serial的
迁移后Debug与Release各135项通过。本次没有执行基准，不提供零增量优化的量化性能承诺。

# 基准测试

## 云服务平台
- 16核(vCPU) 2.5 GHz主频、3.2 GHz睿频的Intel ® Xeon ® Platinum 8269CY（Cascade Lake）
- 内存:64G
- CentOS 7.3 64位


|项目|pi_async|async_std|tokio|备注|
|---|---|---|---|---|
|bench_async_mutex|3,266 ns/iter (+/- 136)|149,332 ns/iter (+/- 7,212)|6,374,238 ns/iter (+/- 861,432)||
|contention|338,786 ns/iter (+/- 68,222)|901,779 ns/iter (+/- 28,380)|2,157,495 ns/iter (+/- 38,100)||
|create|257 ns/iter (+/- 2)|61 ns/iter (+/- 0)|63 ns/iter (+/- 0)||
|no_contention|215,515 ns/iter (+/- 1,121)|225,034 ns/iter (+/- 740)|550,285 ns/iter (+/- 2,313)||
|await_empty_many|605,232 ns/iter (+/- 125,354)|394,823 ns/iter (+/- 19,107)|393,625 ns/iter (+/- 4,459)||
|chained_spawn|504,570 ns/iter (+/- 24,166)|1,090,176 ns/iter (+/- 27,817)|251,943 ns/iter (+/- 1,412)||
|ping_pong|1,176,361 ns/iter (+/- 197,786)|3,859,845 ns/iter (+/- 73,410)|1,193,711 ns/iter (+/- 20,376)||
|spawn_empty_many|4,187,949 ns/iter (+/- 587,053)|18,887,015 ns/iter (+/- 347,589)|9,941,412 ns/iter (+/- 659,722)||
|spawn_many|3,436,761 ns/iter (+/- 279,137)|19,001,495 ns/iter (+/- 380,355)|7,615,952 ns/iter (+/- 210,639)||
|spawn_one_to_one|6,205,756 ns/iter (+/- 826,745)|36,189,628 ns/iter (+/- 357,690)|16,620,075 ns/iter (+/- 589,085)||
|yield_many|23,757,528 ns/iter (+/- 4,110,213)|52,304,694 ns/iter (+/- 519,928)|17,746,497 ns/iter (+/- 550,878)||
|block_on|83 ns/iter (+/- 0)|2,593 ns/iter (+/- 48)|178 ns/iter (+/- 1)||
|local_run|666,627 ns/iter (+/- 5,476)||||
|local_send_many|5,885,537 ns/iter (+/- 98,251)||||
|local_spawn_many|1,260,102 ns/iter (+/- 5,423)|20,201,034 ns/iter (+/- 692,642)|1,553,246 ns/iter (+/- 49,815)||

# 贡献指南

# License

This project is licensed under the [MIT license].

[MIT license]: LICENSE

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in pi_async by you, shall be licensed as MIT, without any additional
terms or conditions.
