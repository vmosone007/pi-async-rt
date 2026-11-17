# 任务基础描述值的声明

## 三项基础描述值的历史基线与实际检查

以下记录对应新增任务、队列与唤醒声明之前的三项基础描述值基线。该基线的源码根为crates/task/src，lib.rs公开definitions模块（module），三个类型（type）的真实定义位于definitions.rs；当时没有配对实现文件、函数体或todo!。当前三库的源码入口和未实现边界见下文“任务与双队列唤醒接入的声明扩展”。

该历史基线已实际执行cargo build -p pi-async-task --offline，退出码0，dev配置，输出构建耗时0.60s；cargo doc -p pi-async-task --no-deps --offline退出码0；cargo test -p pi-async-task --doc --offline的3个公开用法示例全部通过。定向rustfmt --check --edition 2021检查通过，当时的cargo metadata确认该包无第三方依赖。公开文档（rustdoc）检查共6个HTML页面，其中3个为枚举（enum）页面：内部说明和源码标记没有进入公开页面，也没有本库源码附页或其跳转链接。

实际工具链（toolchain）为rustc 1.99.0-nightly (eff8269f7 2026-07-18)、cargo 1.99.0-nightly (3efb1f477 2026-07-17)，主机（host）为x86_64-unknown-linux-gnu。上述结果只覆盖该历史基线的三项声明、此主机环境及公开示例，不证明实际任务（Task）机制、并发（concurrency）、清理或性能已实现，也不证明其它编译目标或最低支持版本通过。此前只有TaskPhase的检查保留其历史范围，本次结果独立取得。

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

三个值不持有任务（Task）、原异步计算（Future）、执行结果（output）、队列（queue）链接或操作权限，也不相互调用。实际任务（Task）的组合约束与清理确认含义直接写在definitions.rs的局部说明中；这三个描述值本身不负责采集联合快照（snapshot）；本次新增的TaskSnapshot及各句柄查询入口见实际源码声明，不能把多个历史值拼接当作同一时刻事实。

## 基础描述值本身不提供的能力

三个基础描述值本身不提供任务（Task）存储、原子状态（atomic state）、实时查询、创建或执行入口、取消等待（cancellation wait）、结果领取、队列（queue）或执行器（executor）。本次新增的相关类型（type）、字段（field）与入口已在下文及源码中声明，具体业务操作仍为todo!，其功能与性能尚未验证；不得由描述值或声明存在推定这些能力已经可运行。


## 任务与双队列唤醒接入的声明扩展

### 实际源码与当前能力

源码根为crates/。pi-async-task位于crates/task，其definitions、task、runnable、control、join分别承载基础描述、单任务机制、创建与执行持有、共享控制与取消等待、唯一结果领取。配对的*_inner.rs保存未实现操作；definitions为纯定义，没有配对实现文件。

pi-async-task-queue位于crates/task-queue，external声明统一外部接入与TaskBatch，local声明TaskQueue及受限链适配，pool提供已有pi_task_pool的可选默认适配。pi-async-task-wake位于crates/task-wake，recovery是纯接口声明，target承载固定目标及线程本地入口。queue依赖task，wake依赖前两者；没有执行器或I/O实现。

已有外部构建记录：三个库的默认特性构建已取得成功；最终特性组合及文档检查尚待当前产物完成后核实。所有新的业务入口仍有todo!，构建成功不代表任务执行、取消、队列并发或吞吐已经实现或验证。

### 已确定的协作

创建返回Runnable、ControlHandle与JoinHandle，创建不自动提交或轮询。Runnable不可克隆，控制句柄可共享，输出只有唯一领取权。原值与输出复用同一任务存储，Plain不携带链接，IntrusiveLocal内嵌默认本地队列节点。原值的合法析构不等待其它共享句柄全部释放。

单次执行至多轮询直接封装的Future一次。正在轮询或挂起交接期间的唤醒只记录需求；必要收尾完成后才交付唯一后续Runnable。重复唤醒可合并，新的需求不能丢失；Ready保留本次结果，取消只阻止后续轮询。独立入口与清理不另行捕获恐慌，只捕获直接Future::poll动态范围内可展开的恐慌。

同步取消无返回值且不等退出；CancelWait惰性发送，初始身份、自等待、成环或登记拒绝不发送本份新请求。同一目标至多一个持续等待，退出登记后可重新申请。等待关系只在实际尚无结果并准备挂起时存在，结果确定或放弃即退边；已发合法取消不撤回。开始挂起后不更换实际等待者。

TaskQueue是本线程无锁侵入式双端队列，默认尾插；显式本地提交可头插，影响下一次选择。外部TaskBatch保留来源选择顺序，只能追加内部尾部；pi_task_pool原生加权选择不能冒称全局先进先出。默认外部适配只使用该库的并行队列，不使用快速直接执行，也不改变其内部算法。粗略估计不能替代适配器的精确逻辑计数。

WakeTarget只路由，不推进原异步计算。固定ThreadId用于判断线程，TLS仅存本地弱引用并核对原目标身份；本地入口暂不可用时仍投递原外部接入。共享状态不存Rc或本地原值指针，原生Waker继续复用任务控制存储。没有错误返回通道的Schedule遇到正常拒绝时把原值交DispatchRecovery，不能静默丢失。

### 尚未取得的实现证据

计数与状态竞争、清理与引用计数、等待槽交接、取消等待图并发、线程退出、侵入式链接复用、外部适配的时限及功能性能仍须由后续具体实现证明。当前不宣称这些行为已通过运行测试，也不宣称完成整个异步运行时。


起点堆栈由宿主全局钩子调用capture_current_panic_backtrace采集，本库不安装全局钩子。
PanicRecord.backtraces按本次轮询内事件发生顺序保存；用户可能在内部捕获或重新抛出恐慌，
这些记录不保证与最终payload一一对应，也不能把catch之后的调用栈冒充起点。
未接纳初始Runnable被放弃后，结果错误为TaskError::Abandoned，取消等待为Failed(TargetAbandoned)；
有效控制句柄仍保活，所以不能误报TargetAbsent或永远继续挂起。


## 任务状态查询与当前数据访问

本主题源码位于crates/task，公开声明归definitions、control、task，并从lib导出。配对局部操作位于task_inner.rs；等唤醒判断和当前数据借用仍为todo!，尚不能作为已实现功能使用。

- ControlHandle::is_waiting_for_wake和TaskRegistration::is_waiting_for_wake共用TaskRef的窄判定；原根计算挂起（Pending）且交接完成、无执行访问、无待处理唤醒（wake-up）或发出的执行持有、无取消（cancellation）或退出时为true。只读一次原状态，不把Idle单项当作充分条件。
- TaskRegistration::snapshot复用原TaskSnapshot；metadata<M>复用原存储的类型（type）核对，相符返回Some(&M)，否则None。登记不增加成员或Clone／Send／Sync能力；借用不超过&self，原Future与M的清理时点分开。
- try_with_current_metadata<M,R>只在真实本库Future::poll内定位当前M；正确类型（type）同步调用read一次，返回RawResult<R, CurrentTaskAccessError>。无当前任务（Task）或M不符分别为NoCurrentTask、MetadataTypeMismatch，不执行read。M为Send + Sync + 'static；R没有额外线程或静态存续约束，但不能带走所给&M。
- 当前访问沿原CurrentGuard，不另建TLS；M内部安全更新由M自身提供。read的合法业务副作用及成本由使用者负责，恐慌（panic）沿原调用范围传播。现有生命周期（lifecycle）、Header、TaskSnapshot、状态编码及原方法体不改。

三个库（crate）的既有Result使用pi_result::RawResult等价类型，原错误、原值返还和特性（feature）保持。新依赖来自注册表，采用pi_result = "0.1"；不将任务（Task）、Future或Runnable的返还改成诊断报告。实际取得三库默认／全部特性（feature）及队列／唤醒无默认配置的八次cargo build结果，均退出0；工具为rustc 1.99.0-nightly (eff8269f7 2026-07-18)。这些结果只证明本批声明可编译，新判定及当前数据借用仍未实现。


## 定时、空闲与管理访问的公开抽象

本部分补充队列来源的声明及本地连接；原任务（Task）、原异步计算（Future）和原可执行句柄（Runnable）保持同一对象。具体默认定时／空闲存储尚不存在，不能将特征（trait）声明视为默认服务实现。

### 代码归属与访问

- `pi-async-task-queue::management`只定义`QueueCleanupProgress<B>`。B是本次已交出的原内容；两种进度均允许空B。
- `timer::TimerTaskQueue`是本地拥有者，`TimerQueueHandle`只提供共享提交、取消请求（cancellation request）和查询。`Item`和`Instant`不带全局Send／Sync约束；仅共享提交方法要求两者Send + 'static。键（key）要求Send + Sync + 'static，不强制Copy／Clone。
- `idle::IdleTaskQueue`提供共享普通访问和独立取消清理；`IdleQueueManager`承担唯一实际管理。`ExternalQueueManager`独立补充已有普通外部访问，不改原ExternalTaskQueue签名（signature）。
- 管理对象的handle返回同一原来源；类型（type）相等不证明实例相同。默认管理对象不提供Clone；不能声称特征（trait）自动阻止任意第三方另建管理别名。普通访问不能经裸存储或Deref取得管理权限。

### 时间、交付和退出

定时时刻均来自同一时钟实例、起点和单位。`advance(&now)`只做有限推进；相同时间可重复，逆向时间在产生推进效果前拒绝。取一个到期原值与取当前全部到期原值分别提供；后者固定本次时间和已接收范围，不能把前缀冒充全部，未能证明完整则保留内容并返回WouldBlock。后到内容不无限扩充本次范围。`next_check_at`只读取得最迟应再检查的时刻；未知不伪装None，已知立即工作不能给出较晚时刻。

定时取消请求（cancellation request）成功仅表示已记录／合并，不证明登记存在、取消落实或原异步计算（Future）已析构（drop）。待清理内容保留原Item及既定缘由，清理不受到期、空闲或时间追赶限制。发布后必要通知不能漏掉，通知失败不撤销已发生的接收。

空闲普通取出由调用者先判断空闲；非零本次数量上限首版为1。Ready向量（Vec）非空且不超过上限，具体顺序由实现决定，调用方保序。取消清理独立有限推进，不受正常上限1限制。同一Runnable在并发竞争中只交出一次，已绑定任务（Task）不借此迁移。

所有返回批次（batch）独立拥有内容，返回前退出队列（queue）保护，迭代不回访来源或追赶新提交。数量包含已接收、尚未交出／实际清理的全部内容；内部移动、到期或取消标记不减数，实际交付／清理只减一次。状态（state）和数量分别取样，不承诺全局同一瞬间。

`clear`不改变接收状态（state），未完调用继续本次固定范围。`close`先停收再复用清理，纳入旧未完范围和停收前已接收内容；失败不重开，不遗漏尚未完成的合法发布。Complete仅表明来源当前责任完成／合法移交，不确认所有任务（Task）或原异步计算（Future）退出。各调用有限工作、不等新任务（Task）或其它线程（thread）完成，不据此承诺固定墙钟耗时。具体并发范围算法、取消发现及推进额度尚待分别设计。

### 本地连接与未实现位置

`TaskQueue`私有closed默认false，三个压入入口关闭后优先返还原Runnable或完整TaskBatch及Closed。clear直接复用原drain并包装Complete；close先置closed再调用clear。原弹出、drain及Drop不因closed停止，原正常开放路径不改。新增一处bool与压入分支，不宣称尺寸或热路径（hot path）成本为零。

`pi-async-task::try_current_task_id()`直接复用已有当前身份查询；原CurrentGuard只覆盖实际F::poll范围，直接子异步计算（Future）相同，独立任务（Task）不同。范围以外返回None，不新建线程局部存储（TLS）、索引或保活持有。已有两项查询／数据借用占位不在此落实。

`TimerQueueError::into_report`直接返回Backend原错误报告（Report）；其它八类进入`timer_error_report_inner`，当前仍是todo!。该局部说明给出ErrorKind映射和原具体原因保留方式，不要求原载荷（payload）成为错误上下文（error context）。提交错误用RawResult返还原值，管理clear／close使用pi_result::Result；报告转换不放到成功热路径（hot path）。

具体定时适配、MPSC节点／链接、空闲取消发现及普通外部来源管理包装均未由这些声明决定；不得从接口（API）存在推得这些实现已经完成。


## MPSC节点（node）的任务（Task）侧支持

源码根为`crates/task/src/`；关键声明在`task.rs`、创建选择在`definitions.rs`，配对局部操作在`task_inner.rs`。这次扩充保留原公开创建签名（signature）及原线程清理语义。

`QueueSupport`现在声明Plain、IntrusiveLocal、IntrusiveMpsc及IntrusiveLocalAndMpsc四种创建布局（layout）。后两者分别使用MpscQueueEntry和私有LocalAndMpscLinks；原两种布局（layout）不保留未使用的MPSC内容。每个任务（Task）仍只有原来的一块存储（storage），两种链接（link）不得同时持有Runnable。

`MpscQueueEntry`与`MpscEntryHandle`均为无泛型参数（generic parameter）的公开不透明类型（opaque type）。节点（node）包含原生Links、唯一ticket及不变的私有MpscEntryOrigin；句柄（handle）只保存NonNull指针（pointer），可Send，不承诺Sync，不支持Clone、Copy或Default。业务来源与独立空节点（node）来源分别为EmbeddedTask与StandaloneEmpty，不能通过ticket是否为空猜测释放（drop）方式。

三个公开入口分别为try_from_runnable、try_into_runnable及try_new_empty。缺链接（link）原样返还Runnable；独立空节点（node）不能转成Runnable，原样返还句柄（handle）。工厂显式分配独立空节点（node），通过pi_result::Result报告检测到的失败；业务节点（node）随原任务（Task）初始化。原生stub与普通清理终点使用不同实例（instance）。

`cordyceps::Linked`由节点（node）定义方实现。into_ptr及成功取回Runnable使用ManuallyDrop停止旧句柄（handle）的释放（drop）；from_ptr仅在原生满足唯一恢复前提后使用。links及四种StorageLinks只投影真实字段（field）地址。业务句柄（handle）释放（drop）先取空ticket并结束节点（node）访问，再沿原Runnable处理；最后一步可能回收整块任务存储（task allocation）。独立空节点（node）只按自身原分配（allocation）释放一次。

TaskVTable增加mpsc_queue_entry；TaskRef通过窄出口调用它。Header及TaskSnapshot不增加字段（field），不改变原run、poll、取消（cancellation）和所属域析构（owner-domain drop）的约定。公开create／create_local和BuildError返还原输入的含义保持。

当前新增转换、独立工厂、原生适配及窄定位仍以todo!表示；创建入口仅补充已确定的两类L初始化。已有三个未实现操作保持。外部实际执行cargo build -p pi-async-task --all-features --offline --locked退出0，默认配置亦退出0；新增未实现路径产生八项dead_code警告，未禁用诊断。公开文档（rustdoc）生成退出0。没有运行新增行为测试（test），不能将这些声明当作可运行的新功能。自定义MPSC来源的计数、清理和关闭由其后续设计承担，本次不生成其实现。
