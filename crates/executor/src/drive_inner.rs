fn executor_options_default_inner() -> ExecutorOptions {
    ExecutorOptions {
        maintenance_batch: const { NonZeroUsize::new(64).unwrap() },
        would_block_retries: 2,
        timer_delivery: TimerDelivery::All,
        max_sleep: Duration::from_millis(10),
    }
}
#[allow(unused_variables)]
fn executor_options_validate_inner(this: &ExecutorOptions) -> pi_result::Result<()> {
    if this.would_block_retries > 2 {
        return Err(pi_result::Error::new(pi_result::ErrorKind::InvalidInput)
            .attach(format!(
                "额外立即重试次数超出0至2（would_block_retries out of range）: would_block_retries={}",
                this.would_block_retries,
            )));
    }
    if this.max_sleep < Duration::from_millis(1)
        || this.max_sleep > Duration::from_millis(10)
    {
        return Err(pi_result::Error::new(pi_result::ErrorKind::InvalidInput)
            .attach(format!(
                "被动重查上限超出1ms至10ms（max_sleep out of range）: max_sleep={:?}",
                this.max_sleep,
            )));
    }
    Ok(())
}
