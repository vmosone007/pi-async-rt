// 当前文件落实配对声明中的局部操作。

#[allow(unused_variables)]
fn queue_new_inner(owner: &TaskThread) -> TaskQueue {
    TaskQueue { owner: owner.id(), entries: LinkedList::new(QueueAdapter { links: LinkOps, pointers: DefaultPointerOps::new() }), count: 0, closed: false, local: PhantomData }
}

#[allow(unused_variables)]
fn queue_owner_inner(this: &TaskQueue) -> TaskOwnerId {
    this.owner
}

#[allow(unused_variables)]
fn queue_len_inner(this: &TaskQueue) -> usize {
    this.count
}

#[allow(unused_variables)]
fn queue_is_empty_inner(this: &TaskQueue) -> bool {
    this.count == 0
}

#[allow(unused_variables)]
fn queue_push_front_inner(this: &mut TaskQueue, task: Runnable) -> Result<(), QueuePushError> {
    if task.snapshot().owner != Some(this.owner) { return Err(QueuePushError { task, cause: QueuePushErrorKind::WrongOwner }); }
    let Some(count) = this.count.checked_add(1) else { return Err(QueuePushError { task, cause: QueuePushErrorKind::CountOverflow }); };
    // SAFETY: 本地独占队列（queue）且原持有（Runnable）尚未入链，所属域（owner）匹配。
    let node = match unsafe { QueueEntry::store(task, this.owner) } {
        Ok(node) => node,
        Err(task) => return Err(QueuePushError { task, cause: QueuePushErrorKind::LinkMissing }),
    };
    // 回滚守卫（rollback guard）只覆盖已存ticket、尚未入链的原持有（Runnable）。
    struct UnlinkedTicket { node: NonNull<QueueEntry>, owner: TaskOwnerId, armed: bool }
    impl Drop for UnlinkedTicket {
        fn drop(&mut self) {
            if self.armed {
                // SAFETY: 守卫（guard）保住已存ticket的原址；未入链时仅取回本份唯一持有。
                let link = unsafe { QueueEntry::link_from_node(self.node) };
                if !unsafe { link.as_ref() }.is_linked() { drop(unsafe { QueueEntry::take(self.node, self.owner) }); }
            }
        }
    }
    let mut rollback = UnlinkedTicket { node, owner: this.owner, armed: true };
    // SAFETY: 节点（node）由其ticket保活，原生插入不调用用户代码且已验证链接（link）空闲。
    let value = unsafe { UnsafeRef::from_raw(node.as_ptr()) };
    this.entries.push_front(value);
    this.count = count;
    rollback.armed = false;
    Ok(())
}

#[allow(unused_variables)]
fn queue_pop_front_inner(this: &mut TaskQueue) -> Option<Runnable> {
    let node = this.entries.pop_front()?;
    this.count = this.count.checked_sub(1).unwrap_or_else(|| std::process::abort());
    // SAFETY: 原生摘链（unlink）完整结束，ticket仍覆盖节点（node）。
    Some(unsafe { QueueEntry::take(NonNull::from(&*node), this.owner) })
}

#[allow(unused_variables)]
fn queue_push_back_inner(this: &mut TaskQueue, task: Runnable) -> Result<(), QueuePushError> {
    if task.snapshot().owner != Some(this.owner) { return Err(QueuePushError { task, cause: QueuePushErrorKind::WrongOwner }); }
    let Some(count) = this.count.checked_add(1) else { return Err(QueuePushError { task, cause: QueuePushErrorKind::CountOverflow }); };
    // SAFETY: 本地独占队列（queue）且原持有（Runnable）尚未入链，所属域（owner）匹配。
    let node = match unsafe { QueueEntry::store(task, this.owner) } {
        Ok(node) => node,
        Err(task) => return Err(QueuePushError { task, cause: QueuePushErrorKind::LinkMissing }),
    };
    // 回滚守卫（rollback guard）只覆盖已存ticket、尚未入链的原持有（Runnable）。
    struct UnlinkedTicket { node: NonNull<QueueEntry>, owner: TaskOwnerId, armed: bool }
    impl Drop for UnlinkedTicket {
        fn drop(&mut self) {
            if self.armed {
                // SAFETY: 守卫（guard）保住已存ticket的原址；未入链时仅取回本份唯一持有。
                let link = unsafe { QueueEntry::link_from_node(self.node) };
                if !unsafe { link.as_ref() }.is_linked() { drop(unsafe { QueueEntry::take(self.node, self.owner) }); }
            }
        }
    }
    let mut rollback = UnlinkedTicket { node, owner: this.owner, armed: true };
    // SAFETY: 节点（node）由其ticket保活，原生插入不调用用户代码且已验证链接（link）空闲。
    let value = unsafe { UnsafeRef::from_raw(node.as_ptr()) };
    this.entries.push_back(value);
    this.count = count;
    rollback.armed = false;
    Ok(())
}

#[allow(unused_variables)]
fn queue_pop_back_inner(this: &mut TaskQueue) -> Option<Runnable> {
    let node = this.entries.pop_back()?;
    this.count = this.count.checked_sub(1).unwrap_or_else(|| std::process::abort());
    // SAFETY: 原生摘链（unlink）完整结束，ticket仍覆盖节点（node）。
    Some(unsafe { QueueEntry::take(NonNull::from(&*node), this.owner) })
}

#[allow(unused_variables)]
fn queue_append_inner(this: &mut TaskQueue, batch: TaskBatch) -> Result<usize, BatchPushError> {
    let mut batch = batch;
    let length = batch.tasks.len();
    let Some(count) = this.count.checked_add(length) else { return Err(BatchPushError { batch, cause: QueuePushErrorKind::CountOverflow }); };
    if batch.tasks.iter().any(|task| task.snapshot().owner != Some(this.owner)) {
        return Err(BatchPushError { batch, cause: QueuePushErrorKind::WrongOwner });
    }
    // 使用受限节点接口（QueueEntry）预检，不创建第二份持有（Runnable），不提前插入任何前缀。
    // 每次安全取回并旋转（rotate）原容器，失败把已检查部分旋回原顺序，无每任务分配（allocation）。
    for checked in 0..length {
        let task = batch.tasks.pop_front().unwrap_or_else(|| std::process::abort());
        // SAFETY: 全批所属域（owner）已核，原持有未入链，当前线程（thread）独占节点（node）。
        match unsafe { QueueEntry::store(task, this.owner) } {
            Ok(node) => batch.tasks.push_back(unsafe { QueueEntry::take(node, this.owner) }),
            Err(task) => {
                batch.tasks.push_front(task);
                batch.tasks.rotate_right(checked);
                return Err(BatchPushError { batch, cause: QueuePushErrorKind::LinkMissing });
            }
        }
    }
    for task in batch.tasks {
        // SAFETY: 预检与提交之间无外部调用（external call）或并发节点写入，所有链接仍空闲。
        let node = unsafe { QueueEntry::store(task, this.owner) }.unwrap_or_else(|_| std::process::abort());
        this.entries.push_back(unsafe { UnsafeRef::from_raw(node.as_ptr()) });
    }
    this.count = count;
    Ok(length)
}

#[allow(unused_variables)]
fn queue_drain_inner(this: &mut TaskQueue) -> TaskBatch {
    let mut tasks = std::collections::VecDeque::with_capacity(this.count);
    while let Some(node) = this.entries.pop_front() {
        this.count = this.count.checked_sub(1).unwrap_or_else(|| std::process::abort());
        // SAFETY: 每个节点（node）完全摘链（unlink）后才取回唯一持有（Runnable）。
        tasks.push_back(unsafe { QueueEntry::take(NonNull::from(&*node), this.owner) });
    }
    TaskBatch::from_tasks(tasks)
}

#[allow(unused_variables)]
fn queue_drop_inner(this: &mut TaskQueue) {
    // 余项守卫（remaining-items guard）保留摘链（unlink）责任；不增加与队列长度相关的分配（allocation）。
    struct Remaining<'queue> { queue: &'queue mut TaskQueue }
    impl Drop for Remaining<'_> {
        fn drop(&mut self) {
            while let Some(task) = self.queue.pop_front() { drop(task); }
        }
    }
    let remaining = Remaining { queue: this };
    while let Some(task) = remaining.queue.pop_front() { drop(task); }
}

#[allow(unused_variables)]
unsafe fn adapter_value_inner(
    this: &QueueAdapter,
    link: NonNull<LinkedListLink>,
) -> *const QueueEntry {
    // SAFETY: 此链接（link）由同适配器（Adapter）的正向转换取得。
    unsafe { QueueEntry::node_from_link(link) }.as_ptr()
}
#[allow(unused_variables)]
unsafe fn adapter_link_inner(
    this: &QueueAdapter,
    value: *const QueueEntry,
) -> NonNull<LinkedListLink> {
    // SAFETY: 原生适配器（Adapter）提供有效且保活的节点（node），本转换不改变持有。
    unsafe { QueueEntry::link_from_node(NonNull::new(value.cast_mut()).unwrap_or_else(|| std::process::abort())) }
}
#[allow(unused_variables)]
fn adapter_ops_inner(this: &QueueAdapter) -> &LinkOps {
    &this.links
}
#[allow(unused_variables)]
fn adapter_ops_mut_inner(this: &mut QueueAdapter) -> &mut LinkOps {
    &mut this.links
}
#[allow(unused_variables)]
fn adapter_pointer_ops_inner(this: &QueueAdapter) -> &DefaultPointerOps<UnsafeRef<QueueEntry>> {
    &this.pointers
}

#[allow(unused_variables)]
fn queue_clear_inner(this: &mut TaskQueue) -> pi_result::Result<QueueCleanupProgress> {
    // 独立批次（TaskBatch）先接管全部原持有；其字段（field）的展开（unwinding）清理承担余项。
    // drain已完整摘链（unlink），原Runnable析构（drop）不再访问本队列（queue）的节点（node）。
    drop(this.drain());
    Ok(QueueCleanupProgress::Complete)
}

#[allow(unused_variables)]
fn queue_close_inner(this: &mut TaskQueue) -> pi_result::Result<QueueCleanupProgress> {
    this.clear()
}
