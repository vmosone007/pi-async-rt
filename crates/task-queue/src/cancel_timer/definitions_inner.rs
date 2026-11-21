// 仅实现本模块（module）的局部叶操作（leaf operation）。
fn timer_options_default_inner() -> CancelTimerOptions {
    CancelTimerOptions {
        work_batch_size: const { NonZeroUsize::new(64).unwrap() },
        advance_ticks: const { NonZeroU32::new(64).unwrap() },
        atomic_attempts: const { NonZeroU8::new(3).unwrap() },
    }
}
