fn executor_options_default_inner() -> ExecutorOptions {
    todo!("P1；maintenance_batch=64，would_block_retries=2，timer_delivery=All，max_sleep=10ms；只构造值，不取时/分配/初始化全局时钟。")
}
#[allow(unused_variables)]
fn executor_options_validate_inner(this: &ExecutorOptions) -> pi_result::Result<()> {
    todo!("P1；仅校验额外重试<=2及max_sleep在闭区间1ms..10ms；NonZeroUsize已保证额度非零，不添加未经定义的上限。错误用pi_result InvalidInput保具体字段和值，不panic、不降级修正用户输入；不读取来源或时钟。")
}
