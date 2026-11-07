# 任务基础描述值的声明

## 当前入口与实际检查

源码根为crates/task/src。lib.rs公开definitions模块（module）；三个类型（type）的真实定义都在definitions.rs。没有配对实现文件或函数体，也没有todo!。

当前三项声明已实际执行cargo build -p pi-async-task --offline，退出码0，dev配置，输出构建耗时0.60s；cargo doc -p pi-async-task --no-deps --offline退出码0；cargo test -p pi-async-task --doc --offline的3个公开用法示例全部通过。定向rustfmt --check --edition 2021检查通过，cargo metadata确认本包无第三方依赖。公开文档（rustdoc）检查共6个HTML页面，其中3个为枚举（enum）页面：内部说明和源码标记没有进入公开页面，也没有本库源码附页或其跳转链接。

实际工具链（toolchain）为rustc 1.99.0-nightly (eff8269f7 2026-07-18)、cargo 1.99.0-nightly (3efb1f477 2026-07-17)，主机（host）为x86_64-unknown-linux-gnu。上述结果只覆盖当前三项声明、此主机环境及公开示例，不证明实际任务（Task）机制、并发（concurrency）、清理或性能已实现，也不证明其它编译目标或最低支持版本通过。此前只有TaskPhase的检查保留其历史范围，本次结果独立取得。

## 目标与已确定声明

提供三种轻量且含义独立的公开描述值，不实现任务（Task）的执行、查询、取消（cancellation）、清理或排队。

| 实际类型（type） | 成员及含义 |
| --- | --- |
| TaskPhase | Preparing：成功建立后、首次合法轮询（poll）之前；Advancing：首次合法轮询（poll）开始后的推进阶段，含挂起（Pending）与交接；Exiting：当前无轮询（poll）且永久停止后续轮询（poll），尚有自身责任；Ended：原异步计算（Future）已合法析构（drop），自身责任完成或合法移交。 |
| CancelRequestStatus | NotRecorded：目标未记录取消请求（cancellation request）；Recorded：目标已记录。发送请求或正常完成都不能被替换成另一种含义。 |
| FutureDropStatus | Unconfirmed：合法析构（drop）的完成尚未确认，包括尚未开始、进行中、尚未发布或失败；Confirmed：合法析构（drop）已完成并发布确认，不表示最终存储已经回收。 |

三个类型（type）均为无载荷枚举（enum），公开且使用Clone、Copy、Debug、Eq、PartialEq派生宏（derive macro）。不强制布局（layout）、整数编码或稳定外部格式；不提供默认值、排序、哈希（hash）、序列化（serialization）或手写方法。没有声明特征（trait）、类型别名（type alias）、自有宏（macro）或额外依赖。

## 选择理由与边界

枚举（enum）使名字与有限含义直接对应，避免几个bool值互换或字符串/整数的解释分歧。普通构造、复制（copy）和相等比较为O(1)，不增加分配、锁（lock）、异步等待（await）或用户回调（callback）；调试（debug）的格式化（formatting）目标可以执行外部行为，不能对它承诺相同成本或无失败。没有进行吞吐或延迟测量。

Clone和Copy只复制描述值；PartialEq和Eq只表达同类型（type）值的相等关系；Debug用于调试（debug）展示且格式不稳定。没有资源字段（field），线程（thread）能力由编译器（compiler）自动推导，不编写不安全代码（unsafe）。不设置默认值，避免默认构造被误当作实际观察；不从变体（variant）顺序推导任务（Task）操作顺序。

三个值不持有任务（Task）、原异步计算（Future）、执行结果（output）、队列（queue）链接或操作权限，也不相互调用。实际任务（Task）的组合约束与清理确认含义直接写在definitions.rs的局部说明中；当前没有采集联合快照（snapshot）的接口（API），不能把多个历史值拼接当作同一时刻事实。

## 尚未提供的能力

当前没有任务（Task）存储、原子状态（atomic state）、实时查询、创建或执行入口、取消等待（cancellation wait）、结果领取、队列（queue）或执行器（executor）。具体同步（synchronization）、观察时点、合法线程清理与这些机制的功能/性能仍须另外明确。不得由三个描述值推定这些能力已经存在。
