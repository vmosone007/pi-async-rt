fn runtime_options_default_inner() -> RuntimeOptions {
    RuntimeOptions {
        external_queue: MpscQueueOptions::default(),
        idle_queue: MpscQueueOptions::default(),
        timer: CancelTimerOptions::default(),
        executor: ExecutorOptions::default(),
    }
}
