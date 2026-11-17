# 任务基础描述值

## 当前概念

| 名称 | 本主题的含义与边界 |
| --- | --- |
| 任务（Task） | 管理同一原异步计算（Future）的实体；当前源码已声明其存储、权限句柄及操作入口，具体业务操作尚未实现。 |
| 阶段描述值（TaskPhase） | 表示准备、计算推进、退出处理或自身责任结束；复制值不会复制任务（Task）或授予执行权。 |
| 取消请求（cancellation request） | 希望停止指定任务（Task）后续轮询（poll）的要求；发送、目标记录、实际退出分别成立。 |
| CancelRequestStatus | NotRecorded表示目标未记录要求，Recorded表示目标确已记录；记录不保证最终因取消退出。 |
| FutureDropStatus | Unconfirmed表示尚未取得原异步计算（Future）合法析构（drop）的完成确认；Confirmed表示合法析构（drop）已完成且已发布确认。 |
| 描述值与真实状态（state） | 本主题的枚举（enum）可以直接构造，但构造本身不观察或改变实际目标，也不能当作实际目标已经到达该情况的证明。 |
| 执行结果（output） | 异步计算（Future）正常完成产生的值；清理原异步计算（Future）不表示外部已经领取或销毁此值。 |

## 必要区别

已记录取消请求（cancellation request）仍可能从当前轮询（poll）正常返回完成就绪（Ready）。正常完成也需要清理原异步计算（Future）。所以Recorded与Confirmed分别描述不同事实；Unconfirmed也不能授予重复析构（drop）的权限。

同一目标已经成立的记录或完成确认不被后续动作撤销；保存的描述值本身不会自动更新。跨线程转移（Send）与共享引用的跨线程安全性（Sync）只适用于这些没有资源载荷的值，不改变实际原异步计算（Future）的线程限制。


## 任务、队列与唤醒声明的新增概念

以下说明新增声明；此前三项基础描述值的含义保持不变。

| 中文词汇 | 英文或代码名 | 本次声明的确切含义 |
| --- | --- | --- |
| 可执行句柄 | Runnable | 一份不可克隆的执行安排责任；持有透明任务引用，不把原异步计算移出固定存储。 |
| 共享控制句柄 | ControlHandle | 可克隆、可跨线程查询、唤醒或提出取消；不领取输出，不借用原异步计算。 |
| 结果句柄 | JoinHandle | 唯一结果领取权；释放只放弃领取，不默认取消。LocalOutput保留原线程限制。 |
| 线程所属域 | TaskThread | 当前线程上的原值访问及清理登记；不选择任务、不包含执行器循环。 |
| 接纳登记 | TaskRegistration | 执行方保留的唯一管理登记；放弃请求退出，原值清理责任仍由所属域承接。 |
| 取消等待异步计算 | CancelWait | 惰性Future；首次实际推进时识别等待者，成功须原值合法析构确认，结果独立领取。 |
| 任务队列 | TaskQueue | 原所属线程上的侵入式双端队列；节点内嵌于原任务，成功出队后才复用链接。 |
| 外部任务队列 | ExternalTaskQueue | 跨线程交付接口；一次取得当前有限批次并保序转交，不等待未来提交。 |
| 固定唤醒目标 | WakeTarget | 共享同一所属域、固定线程及外部接入，本线程默认尾插，跨线程走原外部接口。 |
| 投递恢复接口 | DispatchRecovery | 接管未交付的原Runnable及重试或合法退出责任；不是错误返回给Waker调用者。 |

精确公开名称与变体以crates/task、crates/task-queue和crates/task-wake下的声明为准。
这些新操作尚未实现，不能以声明存在推断已经可运行或并发行为已经验证。


## 等唤醒状态与当前任务数据访问

- 等唤醒：原任务（Task）根计算已返回挂起（Pending）并完成交接，当前没有执行访问、未处理唤醒（wake-up）、已发出的执行持有、取消（cancellation）或退出。这是一次取样事实；false不能推出任务已完成或已入队。运行时（runtime）统计还须限定自己的已接纳任务。
- 接纳登记（TaskRegistration）：原线程（thread）中的既有强持有，可借用原任务（Task）的快照（snapshot）及正确类型（type）的附加数据（task metadata），不授予第二份执行持有。
- 当前任务访问错误（CurrentTaskAccessError）：本次同步数据借用的有限失败。NoCurrentTask表示无真实当前任务（Task），MetadataTypeMismatch表示请求M与创建关联的M不同；两者都不执行读取闭包（closure）。


## 定时与空闲来源的访问概念

| 中文词汇 | 英文词汇 | 此处含义 |
| --- | --- | --- |
| 定时任务队列 | timer task queue | 由本地拥有者推进、按已显式给定时间交付到期原值的来源；共享入口不推进计时。 |
| 空闲任务队列 | idle task queue | 单独保管原Runnable；是否空闲由调用者判断，取消清理不受空闲条件限制。 |
| 共享句柄 | shared handle | 指向同一原来源的受限访问对象；可克隆不等于可管理来源。 |
| 不透明键 | opaque key | 原定时登记身份；内部搬运不改变，正常释放不取消，不强制Copy／Clone。 |
| 清理进度 | cleanup progress | Complete或MoreWork，与本次交出的内容是否为空分别表达。 |
| 当前任务编号 | current task ID | 原Future实际poll范围内的TaskId，不是新建身份或操作权限。 |

上述同名概念与既有相关用法共用其对象，不产生另一份Task、Future或队列。


## MPSC节点（node）的保存与持有

- MPSC节点（MpscQueueEntry）：不透明的原生Links、唯一ticket和不变分配来源；可以是任务（Task）内嵌字段（field）或独立空节点（node）。
- MPSC节点持有句柄（MpscEntryHandle）：唯一保管节点（node）的透明句柄（transparent handle），移动（move）不增加任务（Task）实例（instance）或强引用（strong reference）；可Send，不承诺Sync，不支持Clone、Copy、Default。
- 双链接保存结构（LocalAndMpscLinks）：私有按值组合的QueueEntry与MpscQueueEntry，创建时一次准备，任何时刻至多一个ticket保存原Runnable。
- 节点分配来源（MpscEntryOrigin）：私有且初始化后不变的EmbeddedTask／StandaloneEmpty分类，分别决定归还业务执行责任或释放独立分配（allocation）；ticket为空不等于后者。
- 链接定位（StorageLinks）：按真实创建布局（layout）分别返回可选本地与MPSC节点（node），不改变原任务（Task）生命周期（lifecycle）或授予原异步计算（Future）访问权。
- 停止旧包装析构（ManuallyDrop）：成功交出节点（node）持有或取回原Runnable时，避免旧MpscEntryHandle再次执行Drop；不替代下一持有者的清理责任。
