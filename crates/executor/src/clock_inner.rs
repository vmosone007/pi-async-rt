fn manual_clock_new_inner() -> pi_result::Result<ManualClock> {
    todo!("编号：T1，尚未实现， 目标：只用global::try_now检查原全局时间轴已安装，成功返回零大小ManualClock，失败原Report原样返回。进程全局安装由外部按pi_clock公开约定准备，本方法不调用try_init，不增加OnceLock、锁、等待发布或擅自重置起点；不把未知错误当已有成功。读取缓存这里只为验证已安装，不能用于新截止时间取时。构造不创建线程／任务、不刷新时间、不改pi_clock源码。")
}
#[allow(unused_variables)]
fn manual_clock_now_inner(this: &ManualClock) -> pi_result::Result<GlobalManualInstant> {
    todo!("编号：T2，尚未实现， 目标：仅global::try_refresh一次并取RefreshOutcome.published。相等或迟到样本保原只增结果，原Report原样返回。禁止用try_now缓存作为新鲜时间、用std::time::Instant另建起点、重复采样、扫描定时器或推进任务。执行器一次事件循环取得的同一结果显式借给后续定时推进和检查；新计时截止另按其开始时机真实取时一次。")
}
#[allow(unused_variables)]
fn manual_clock_add_inner(
    this: &ManualClock,
    now: &GlobalManualInstant,
    duration: Duration,
) -> pi_result::Result<GlobalManualInstant> {
    todo!(r#"编号：T3，尚未实现， 目标：对显式now调用原GlobalManualInstant::try_add(duration)，原Report原样返回。不得再次刷新时钟、回绕、静默截断或饱和；本层不限制sleep／timeout入口的业务时长范围，那些校验归运行时核心。"#)
}
#[allow(unused_variables)]
fn manual_clock_remaining_inner(
    this: &ManualClock,
    now: &GlobalManualInstant,
    deadline: &GlobalManualInstant,
) -> pi_result::Result<Duration> {
    todo!(r#"编号：T4，尚未实现， 目标：同源deadline<=now返回Duration::ZERO，其余通过deadline.try_duration_since(*now)受检计算。只使用显式值、不再取时，无Report成功分配，不把零剩余时间当来源已经清理或服务关闭。"#)
}
