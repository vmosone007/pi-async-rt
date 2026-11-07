# 任务基础描述值

## 当前概念

| 名称 | 本主题的含义与边界 |
| --- | --- |
| 任务（Task） | 管理同一原异步计算（Future）的实体；当前源码尚未实现这个实体。 |
| 阶段描述值（TaskPhase） | 表示准备、计算推进、退出处理或自身责任结束；复制值不会复制任务（Task）或授予执行权。 |
| 取消请求（cancellation request） | 希望停止指定任务（Task）后续轮询（poll）的要求；发送、目标记录、实际退出分别成立。 |
| CancelRequestStatus | NotRecorded表示目标未记录要求，Recorded表示目标确已记录；记录不保证最终因取消退出。 |
| FutureDropStatus | Unconfirmed表示尚未取得原异步计算（Future）合法析构（drop）的完成确认；Confirmed表示合法析构（drop）已完成且已发布确认。 |
| 描述值与真实状态（state） | 本主题的枚举（enum）可以直接构造，但构造本身不观察或改变实际目标，也不能当作实际目标已经到达该情况的证明。 |
| 执行结果（output） | 异步计算（Future）正常完成产生的值；清理原异步计算（Future）不表示外部已经领取或销毁此值。 |

## 必要区别

已记录取消请求（cancellation request）仍可能从当前轮询（poll）正常返回完成就绪（Ready）。正常完成也需要清理原异步计算（Future）。所以Recorded与Confirmed分别描述不同事实；Unconfirmed也不能授予重复析构（drop）的权限。

同一目标已经成立的记录或完成确认不被后续动作撤销；保存的描述值本身不会自动更新。跨线程转移（Send）与共享引用的跨线程安全性（Sync）只适用于这些没有资源载荷的值，不改变实际原异步计算（Future）的线程限制。
