fn entry_task_inner<K: Send + Sync + 'static, R: AsRef<TimerRegistration<K>>>(
    task: Runnable,
    registration: R,
) -> TimerEntry<K, R> {
    // 唯一移动原任务（Task）与同一登记；不克隆、不接纳、不取时、不轮询（poll）。
    // 登记用途应由调用者保证为任务（Task）；本类型不从登记内容推导用途，也不提供可写入口。
    TimerEntry {
        registration,
        value: EntryValue::Task(Some(task)),
        settled: false,
        key_type: PhantomData,
    }
}
fn entry_signal_inner<K: Send + Sync + 'static, R: AsRef<TimerRegistration<K>>>(
    registration: R,
) -> TimerEntry<K, R> {
    // 只保存同一登记，构造时不完成、不通知；交付时只结清等待结果，不构造额外任务（Task）。
    TimerEntry {
        registration,
        value: EntryValue::Signal,
        settled: false,
        key_type: PhantomData,
    }
}
fn entry_due_inner<K: Send + Sync + 'static, R: AsRef<TimerRegistration<K>>>(
    mut this: TimerEntry<K, R>,
) -> Option<Runnable> {
    // 先独占确定真实到期（Elapsed），使普通结束（drop）不再重报移除；再取出唯一原任务。
    this.settled = true;
    this.registration.as_ref().finish(RegistrationEnd::Elapsed);
    match &mut this.value {
        EntryValue::Task(task) => task.take(),
        EntryValue::Signal => None,
    }
}
fn entry_recover_inner<K: Send + Sync + 'static, R: AsRef<TimerRegistration<K>>>(
    mut this: TimerEntry<K, R>,
) -> Option<Runnable> {
    // 明确未接收的原值恢复：先撤销普通结束路径，原任务原样取回；结束原因由外层负责。
    this.settled = true;
    match &mut this.value {
        EntryValue::Task(task) => task.take(),
        EntryValue::Signal => None,
    }
}
fn entry_drop_inner<K: Send + Sync + 'static, R: AsRef<TimerRegistration<K>>>(
    this: &mut TimerEntry<K, R>,
) {
    if this.settled {
        return;
    }
    // 尚未交付即结束来源责任：先按真实移除定结果（取消事实由登记自身分类），撤销重复清理。
    this.settled = true;
    this.registration.as_ref().finish(RegistrationEnd::Removed);
    // 全部字段／登记借用结束后才释放原值；来源移除不代表原异步计算（Future）已析构确认。
    let value = std::mem::replace(&mut this.value, EntryValue::Signal);
    drop(value);
}

#[cfg(test)]
#[path = "entry_inner_test.rs"]
mod entry_inner_test;
