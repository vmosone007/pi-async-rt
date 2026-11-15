fn runtime_options_default_inner() -> RuntimeOptions {
    todo!("P1；四项各调用原Default：普通和空闲是不同实例的MpscQueueOptions（64/3），timer保其三个已定义工作额度和原轮配置，executor默认All/64/2/10ms。不构造队列/线程/全局时钟，不将两个来源选项共用为同一实例。默认值不改变串行/并发接纳规则。")
}
