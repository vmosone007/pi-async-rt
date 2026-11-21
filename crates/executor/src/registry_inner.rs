#[allow(unused_variables)]
fn registry_new_inner() -> TaskRegistry {
    use std::ptr::null_mut;

    TaskRegistry {
        shared: Arc::new(RegistryShared {
            head: AtomicPtr::new(null_mut()),
            incoming: AtomicPtr::new(null_mut()),
            participants: AtomicUsize::new(0),
            final_retired: UnsafeCell::new(null_mut()),
        }),
        index: HashMap::new(),
        pending_index: null_mut(),
        index_first: true,
        retired: null_mut(),
        cursor: null_mut(),
        previous: null_mut(),
        local: PhantomData,
    }
}
#[allow(unused_variables)]
fn registry_handle_inner(this: &TaskRegistry) -> TaskRegistryHandle {
    TaskRegistryHandle { shared: Arc::clone(&this.shared) }
}
#[allow(unused_variables)]
fn registry_clone_inner(this: &TaskRegistryHandle) -> TaskRegistryHandle {
    TaskRegistryHandle { shared: Arc::clone(&this.shared) }
}
#[allow(unused_variables)]
fn registry_enter_inner(this: &RegistryShared) -> Option<RegistryReadGuard<'_>> {
    use std::sync::atomic::Ordering::{AcqRel, Relaxed};

    let mut previous = this.participants.load(Relaxed);
    loop {
        if previous & REGISTRY_SEALED != 0 {
            return None;
        }
        if previous == REGISTRY_READERS {
            std::process::abort();
        }
        // 与封闭（seal）共用一次比较交换（CAS）；成功后才能读取原链，回收方会考虑本份参与资格。
        match this.participants.compare_exchange_weak(previous, previous + 1, AcqRel, Relaxed) {
            Ok(_) => return Some(RegistryReadGuard { shared: this }),
            Err(actual) => previous = actual,
        }
    }
}
#[allow(unused_variables)]
fn registry_register_inner(this: &TaskRegistryHandle, query: TaskQuery) {
    use std::{ptr::null_mut, sync::atomic::Ordering::{Relaxed, Release}};

    let Some(participant) = this.shared.enter() else {
        // 封闭（seal）之后本次原查询（TaskQuery）仍归调用者路径释放，不占登记表的参与资格。
        drop(query);
        return;
    };
    let node = Box::into_raw(Box::new(RegistryNode {
        query,
        next: AtomicPtr::new(null_mut()),
        incoming_next: UnsafeCell::new(null_mut()),
        retired_next: UnsafeCell::new(null_mut()),
        indexed: UnsafeCell::new(false),
        pin: PhantomPinned,
    }));

    let mut head = this.shared.head.load(Relaxed);
    loop {
        // SAFETY: 首次发布拥有链（head）前节点（node）完全私有。参与者守卫（guard）保证旧节点（node）地址不被回收复用。
        unsafe { (*node).next.store(head, Relaxed); }
        match this.shared.head.compare_exchange_weak(head, node, Release, Relaxed) {
            Ok(_) => break,
            Err(actual) => head = actual,
        }
    }
    let mut incoming = this.shared.incoming.load(Relaxed);
    loop {
        // SAFETY: 本节点（node）未进入非拥有索引链（incoming）；已有读者只读查询（TaskQuery）和拥有链（head）的后继（next）。
        unsafe { (*node).incoming_next.get().write(incoming); }
        match this.shared.incoming.compare_exchange_weak(incoming, node, Release, Relaxed) {
            Ok(_) => break,
            Err(actual) => incoming = actual,
        }
    }
    // 序列发布完毕才归还资格；若恰为最后参与者（participant），守卫（guard）可以释放刚交出的节点（node）。
    drop(participant);
}

unsafe fn registry_index_record_inner(
    index: &mut HashMap<TaskId, NonNull<RegistryNode>>,
    node: NonNull<RegistryNode>,
) -> *mut RegistryNode {
    let raw = node.as_ptr();
    // SAFETY: 记录（record）的发布已取得，原拥有链（head）保有其存储。
    let (id, next) = unsafe { ((*raw).query.id(), (*raw).incoming_next.get().read()) };
    index.insert(id, node);
    // SAFETY: 只有原管理者写 indexed。索引（index）插入完成后才允许记录退役（retire）。
    unsafe { (*raw).indexed.get().write(true) };
    next
}

#[allow(unused_variables)]
fn registry_index_inner(this: &mut TaskRegistry) {
    use std::{ptr::null_mut, sync::atomic::Ordering::AcqRel};

    while let Some(node) = NonNull::new(this.pending_index) {
        // SAFETY: 普通维护取得的余项由原管理者独占访问；成功后才移动位置。
        this.pending_index = unsafe { registry_index_record(&mut this.index, node) };
    }
    // 只取得当前索引链（incoming）；后来发布的记录留在共享入口，由下一次刷新读取。
    let remaining = this.shared.incoming.swap(null_mut(), AcqRel);
    if remaining.is_null() {
        return;
    }
    let mut guard = RegistryIndexGuard { shared: &this.shared, remaining };
    while let Some(node) = NonNull::new(guard.remaining) {
        // SAFETY: 原管理者独占已取得的新链，守卫（guard）保留尚未成功更新索引的记录。
        guard.remaining = unsafe { registry_index_record(&mut this.index, node) };
    }
}
#[allow(unused_variables)]
fn registry_query_inner(this: &mut TaskRegistry, id: TaskId) -> Option<&TaskQuery> {
    let raw = match this.index.get(&id) {
        Some(node) => node.as_ptr(),
        None => {
            this.refresh_index();
            this.index.get(&id)?.as_ptr()
        }
    };
    // SAFETY: 索引（index）只保留未摘下的记录；返回借用期间，原管理者不能退役（retire）或释放该记录。
    Some(unsafe { &(*raw).query })
}
#[allow(unused_variables)]
fn registry_maintain_inner(this: &mut TaskRegistry, max_nodes: usize) {
    use pi_async_task::definitions::{FutureDropStatus, TaskPhase};
    use std::{ptr::null_mut, sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release}};

    if max_nodes != 0 {
        if this.pending_index.is_null() && !this.shared.incoming.load(Relaxed).is_null() {
            this.pending_index = this.shared.incoming.swap(null_mut(), AcqRel);
        }
        if this.cursor.is_null() {
            this.cursor = this.shared.head.load(Acquire);
            this.previous = null_mut();
        }
    }
    for _ in 0..max_nodes {
        if this.index_first || this.cursor.is_null() {
            if let Some(node) = NonNull::new(this.pending_index) {
                // SAFETY: 本次取得的余项仍由 head 保有。只有本地管理者能处理余项并修改索引。
                this.pending_index = unsafe { registry_index_record(&mut this.index, node) };
                this.index_first = false;
                continue;
            }
        }
        let node = this.cursor;
        if node.is_null() {
            break;
        }
        this.index_first = true;
        // SAFETY: 游标（cursor）只指向仍在链上的记录；只有本地管理者可摘链及读写 indexed。
        let (next, finished_id) = unsafe {
            let next = (*node).next.load(Acquire);
            let finished_id = if (*node).indexed.get().read() {
                let snapshot = (*node).query.snapshot();
                (snapshot.phase == TaskPhase::Ended && snapshot.future_drop == FutureDropStatus::Confirmed)
                    .then_some(snapshot.id)
            } else {
                None
            };
            (next, finished_id)
        };
        this.cursor = next;
        if let Some(id) = finished_id {
            let removed = if this.previous.is_null() {
                // 生产者若先发布新头，本次不重试；当前记录仍在下一段链上。
                this.shared.head.compare_exchange(node, next, Release, Relaxed).is_ok()
            } else {
                // SAFETY: 上一个保留的记录仍在链上，生产者只加新头，只有本地管理者可更新这个后继（next）。
                unsafe { (*this.previous).next.store(next, Release) };
                true
            };
            if removed {
                this.index.remove(&id);
                // SAFETY: 任务查询句柄（TaskQuery）的借用已结束；既有读者仍可沿原后继（next）前进。
                unsafe { (*node).retired_next.get().write(this.retired) };
                this.retired = node;
                continue;
            }
        }
        this.previous = node;
    }
    if this.cursor.is_null() {
        this.previous = null_mut();
    }

    // 零增量读改写（RMW）确认没有旧参与者，并向随后进入的参与者发布此前摘链。
    if !this.retired.is_null() && this.shared.participants.fetch_add(0, AcqRel) == 0 {
        let mut reclaim = RegistryReclaimGuard { remaining: std::mem::replace(&mut this.retired, null_mut()) };
        while !reclaim.remaining.is_null() {
            let node = reclaim.remaining;
            // SAFETY: 记录已脱离索引（index）和读者；先交代余项，再释放这份独占的 Box。
            reclaim.remaining = unsafe { (*node).retired_next.get().read() };
            // 若归还最后一份任务存储（task allocation）的持有，就可能触发任务附加数据（task metadata）的析构（drop）。
            // 调用方须先结束外层 RefCell 和线程局部存储（TLS）的借用（borrowing），以免析构（drop）重入（reentrancy）时冲突。
            drop(unsafe { Box::from_raw(node) });
        }
    }
}
#[allow(unused_variables)]
fn registry_waiting_inner(this: &TaskRegistryHandle) -> usize {
    use std::sync::atomic::Ordering::Acquire;

    // 所属域（owner thread）已经真实退出后不登记读者，直接为零样本。
    let Some(guard) = this.shared.enter() else {
        return 0;
    };
    let mut count = 0;
    let mut node = guard.shared.head.load(Acquire);
    while !node.is_null() {
        // 安全（safety）：读者已登记，节点不会在此期间被回收；只读查询不改任务状态。
        let (snapshot, next) = unsafe { ((*node).query.snapshot(), (*node).next.load(Acquire)) };
        // 先按本服务已接纳筛选，再调用原任务查询；原接口不要求接纳，直接运行也可为真。
        if snapshot.admitted && unsafe { (*node).query.is_waiting_for_wake() } {
            count += 1;
        }
        node = next;
    }
    count
}

fn registry_cancelling_inner(this: &TaskRegistryHandle) -> usize {
    use pi_async_task::definitions::{CancelRequestStatus, ExecutionStatus, FutureDropStatus};
    use std::sync::atomic::Ordering::Acquire;

    let Some(guard) = this.shared.enter() else {
        return 0;
    };
    let mut count = 0;
    let mut node = guard.shared.head.load(Acquire);
    while !node.is_null() {
        // 安全（safety）：同一读者保护与只读节点访问；每项只取一次快照。
        let (snapshot, next) = unsafe { ((*node).query.snapshot(), (*node).next.load(Acquire)) };
        // 只计已接纳、已记录取消、原值尚未确认析构且不处于清理未确认的任务。
        if snapshot.admitted
            && snapshot.cancellation == CancelRequestStatus::Recorded
            && snapshot.future_drop == FutureDropStatus::Unconfirmed
            && snapshot.execution != ExecutionStatus::CleanupUnconfirmed
        {
            count += 1;
        }
        node = next;
    }
    count
}

fn registry_unconfirmed_inner(
    this: &TaskRegistry,
    owner: pi_async_task::definitions::TaskOwnerId,
) -> (usize, usize) {
    use pi_async_task::definitions::{ExecutionStatus, FutureDropStatus};
    use std::sync::atomic::Ordering::Acquire;

    let mut total = 0;
    let mut admitted = 0;
    let mut node = this.shared.head.load(Acquire);
    while !node.is_null() {
        // SAFETY: 所属域（TaskThread）已经实际退出，封闭（seal）前只有本地管理者能摘链；并发发布者只增加新头。
        let (snapshot, next) = unsafe { ((*node).query.snapshot(), (*node).next.load(Acquire)) };
        if snapshot.owner == Some(owner)
            && snapshot.future_drop == FutureDropStatus::Unconfirmed
            && snapshot.execution == ExecutionStatus::CleanupUnconfirmed
        {
            total += 1;
            admitted += usize::from(snapshot.admitted);
        }
        node = next;
    }
    (total, admitted)
}
#[allow(unused_variables)]
fn registry_owner_drop_inner(this: &mut TaskRegistry) {
    use std::{ptr::null_mut, sync::atomic::Ordering::AcqRel};

    // 原所属域（TaskThread）须已实际析构（drop）；各来源责任须已完成或合法移交。封闭（seal）后不再访问本地索引。
    this.index.clear();
    this.pending_index = null_mut();
    this.cursor = null_mut();
    this.previous = null_mut();
    let retired = std::mem::replace(&mut this.retired, null_mut());
    // SAFETY: 封闭（seal）前仅本地管理者能写该槽，写完才发布给唯一收集者。
    unsafe { this.shared.final_retired.get().write(retired); }
    if this.shared.participants.fetch_or(REGISTRY_SEALED, AcqRel) & REGISTRY_READERS == 0 {
        this.shared.collect_final();
    }
}
#[allow(unused_variables)]
fn registry_read_drop_inner(this: &mut RegistryReadGuard<'_>) {
    use std::sync::atomic::{fence, Ordering::{Acquire, Release}};

    if this.shared.participants.fetch_sub(1, Release) == (REGISTRY_SEALED | 1) {
        // 获取屏障（Acquire fence）取得管理者封闭前的移交及已经退出的参与者发布；只有最后一份守卫（guard）进入回收。
        fence(Acquire);
        this.shared.collect_final();
    }
}
#[allow(unused_variables)]
fn registry_final_inner(this: &RegistryShared) {
    use std::{ptr::null_mut, sync::atomic::Ordering::Relaxed};

    // 已封闭（sealed）且旧参与者全部退出，唯一收集者取得两条不相交的拥有链。
    let head = this.head.swap(null_mut(), Relaxed);
    // SAFETY: 管理者在封闭（seal）前移交私链，此后只有唯一最终收集者访问该槽。
    let retired = unsafe { this.final_retired.get().replace(null_mut()) };
    this.incoming.store(null_mut(), Relaxed);

    let mut reclaim = RegistryReclaimGuard { remaining: head };
    if head.is_null() {
        reclaim.remaining = retired;
    } else {
        let mut node = head;
        loop {
            // SAFETY: 无参与者或索引再读这些存活节点（node）。先整理全部后继（retired_next），再执行可能展开的析构（drop）。
            let next = unsafe { (*node).next.load(Relaxed) };
            if next.is_null() {
                unsafe { (*node).retired_next.get().write(retired); }
                break;
            }
            unsafe { (*node).retired_next.get().write(next); }
            node = next;
        }
    }

    while !reclaim.remaining.is_null() {
        let node = reclaim.remaining;
        // SAFETY: 每个节点（node）来自一次 Box::into_raw。先交代余项，再释放本份只读查询（TaskQuery），不触及原异步计算（Future）。
        reclaim.remaining = unsafe { (*node).retired_next.get().read() };
        drop(unsafe { Box::from_raw(node) });
    }
}
#[allow(unused_variables)]
fn registry_reclaim_drop_inner(this: &mut RegistryReclaimGuard) {
    while !this.remaining.is_null() {
        let node = this.remaining;
        // SAFETY: 守卫（guard）独占已脱离读者及索引的回收链；先移走当前节点（node），原值析构（drop）即使展开也不会重试它。
        this.remaining = unsafe { (*node).retired_next.get().read() };
        drop(unsafe { Box::from_raw(node) });
    }
}

#[allow(unused_variables)]
fn registry_index_drop_inner(this: &mut RegistryIndexGuard<'_>) {
    use std::{
        ptr::null_mut,
        sync::atomic::Ordering::{Relaxed, Release},
    };

    let first = this.remaining;
    if first.is_null() {
        return;
    }
    let mut tail = first;
    loop {
        // SAFETY: 原管理者独占已经取下的索引链（incoming）；未完成的记录尚未退役（retire）。
        let next = unsafe { (*tail).incoming_next.get().read() };
        if next.is_null() {
            break;
        }
        tail = next;
    }

    let mut incoming = this.shared.incoming.load(Relaxed);
    loop {
        // SAFETY: 发布前只有本守卫（RegistryIndexGuard）访问这段索引链（incoming）的末尾。
        unsafe { (*tail).incoming_next.get().write(incoming) };
        match this.shared.incoming.compare_exchange_weak(incoming, first, Release, Relaxed) {
            Ok(_) => {
                this.remaining = null_mut();
                return;
            }
            Err(actual) => incoming = actual,
        }
    }
}

#[cfg(test)]
mod registry_tests {
    use super::TaskRegistry;
    use crate::ThreadParker;
    use pi_async_runtime_core::task::{TaskConfig, TaskFactory};
    use pi_async_task::definitions::{FutureDropStatus, QueueSupport};
    use pi_async_task::{Runnable, Schedule, TaskThread};
    use std::cell::Cell;
    use std::future::{pending, Future};
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc};
    use std::task::{Context, Poll};

    /// 只创建原任务（Task）用于保存只读查询（TaskQuery）；本用例不提交业务。
    #[derive(Clone, Copy)]
    struct UnsubmittedSchedule;

    impl Schedule for UnsubmittedSchedule {
        fn schedule(&self, _: Runnable) {
            panic!("未提交的任务（Task）不得安排调度（scheduling）");
        }
    }

    /// 分开记录原附加数据（metadata）的释放（drop）；失败用例只令最先释放的原值产生一次恐慌（panic）。
    struct MetadataProbe {
        released: Arc<AtomicUsize>,
        fail_first: Option<Arc<AtomicBool>>,
    }

    impl Drop for MetadataProbe {
        fn drop(&mut self) {
            self.released.fetch_add(1, Ordering::Relaxed);
            if let Some(fail) = &self.fail_first {
                if fail.swap(false, Ordering::Relaxed) {
                    std::panic::panic_any("registry-metadata-drop");
                }
            }
        }
    }

    /// 本地异步计算（Future）的析构（drop）由原所属域（TaskThread）负责；只读查询不能延后它。
    struct LocalFuture(Rc<Cell<usize>>);

    impl Future for LocalFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            panic!("登记和退出不应轮询（poll）原异步计算（Future）");
        }
    }

    impl Drop for LocalFuture {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    /// 经 TaskRegistry::handle 和共享句柄（handle）的克隆（Clone）登记两个真实本地任务（Task）。
    /// 实际退出所属域后，登记表（TaskRegistry）释放两份原查询；已封闭登记表不得保留新的查询。
    #[test]
    fn test_registry_releases_queries_at_owner_exit_and_rejects_late_registration() {
        let owner = TaskThread::new();
        let registry = TaskRegistry::new();
        let registered = registry.handle();
        let remaining = registered.clone();
        let factory = TaskFactory::try_new(
            UnsubmittedSchedule,
            ThreadParker::new().notifier(),
            QueueSupport::IntrusiveLocalAndMpsc,
        ).expect("准备原任务工厂（TaskFactory）");
        let dropped = Rc::new(Cell::new(0));
        let make_local = |metadata: &Arc<AtomicUsize>| {
            factory.create_local(
                &owner,
                LocalFuture(Rc::clone(&dropped)),
                TaskConfig::new(MetadataProbe { released: Arc::clone(metadata), fail_first: None }),
            ).unwrap()
        };
        let first_meta = Arc::new(AtomicUsize::new(0));
        let second_meta = Arc::new(AtomicUsize::new(0));
        let (first, first_result) = make_local(&first_meta);
        let (second, second_result) = make_local(&second_meta);
        registered.register(first_result.query_handle());
        remaining.register(second_result.query_handle());
        assert_eq!(owner.live_tasks(), 2);
        assert_eq!(dropped.get(), 0);

        drop(owner);
        assert_eq!(dropped.get(), 2);
        assert_eq!(first_result.snapshot().future_drop, FutureDropStatus::Confirmed);
        assert_eq!(second_result.snapshot().future_drop, FutureDropStatus::Confirmed);
        drop((first, first_result, second, second_result));
        assert_eq!(first_meta.load(Ordering::Relaxed), 0);
        assert_eq!(second_meta.load(Ordering::Relaxed), 0);
        drop(registry);
        assert_eq!(first_meta.load(Ordering::Relaxed), 1);
        assert_eq!(second_meta.load(Ordering::Relaxed), 1);

        let late_meta = Arc::new(AtomicUsize::new(0));
        let (late, late_result) = factory.create(
            pending::<()>(),
            TaskConfig::new(MetadataProbe { released: Arc::clone(&late_meta), fail_first: None }),
        ).unwrap();
        remaining.register(late_result.query_handle());
        assert!(!late_result.snapshot().admitted);
        drop((late, late_result));
        assert_eq!(late_meta.load(Ordering::Relaxed), 1);
        drop((registered, remaining));
        assert_eq!(first_meta.load(Ordering::Relaxed), 1);
        assert_eq!(second_meta.load(Ordering::Relaxed), 1);
        assert_eq!(dropped.get(), 2);
    }

    /// 经 TaskRegistry::refresh_index 和 TaskRegistry::query 按原任务编号（TaskId）取得任务查询句柄（TaskQuery）。
    /// 两份登记应同时可查；尚未登记的原任务（Task）应查不到，登记后应可查且旧记录仍在。
    #[test]
    fn test_registry_queries_original_tasks_by_id_after_registration() {
        let mut registry = TaskRegistry::new();
        let owner = TaskThread::new();
        let handle = registry.handle();
        let factory = TaskFactory::try_new(
            UnsubmittedSchedule,
            ThreadParker::new().notifier(),
            QueueSupport::IntrusiveLocalAndMpsc,
        ).expect("准备原任务工厂（TaskFactory）");
        let (first, first_result) = factory.create(pending::<()>(), TaskConfig::new(())).unwrap();
        let (second, second_result) = factory.create(pending::<()>(), TaskConfig::new(())).unwrap();
        let (later, later_result) = factory.create(pending::<()>(), TaskConfig::new(())).unwrap();
        handle.register(first_result.query_handle());
        handle.register(second_result.query_handle());
        registry.refresh_index();

        assert_eq!(registry.query(first.id()).map(|query| query.id()), Some(first.id()));
        assert_eq!(registry.query(second.id()).map(|query| query.id()), Some(second.id()));
        assert!(registry.query(later.id()).is_none());

        handle.register(later_result.query_handle());
        assert_eq!(registry.query(later.id()).map(|query| query.id()), Some(later.id()));
        assert_eq!(registry.query(first.id()).map(|query| query.id()), Some(first.id()));
        assert_eq!(registry.query(second.id()).map(|query| query.id()), Some(second.id()));
        drop(owner);
    }

    /// 额度（budget）为零时不取得索引链；额度为一时只更新一项索引。
    /// 按编号（TaskId）查找仍能处理该链余项和后来发布的同一条固定索引链。
    #[test]
    fn test_registry_lookup_keeps_pending_and_newly_published_records() {
        let mut registry = TaskRegistry::new();
        let handle = registry.handle();
        let factory = TaskFactory::try_new(
            UnsubmittedSchedule,
            ThreadParker::new().notifier(),
            QueueSupport::IntrusiveLocalAndMpsc,
        ).expect("准备原任务工厂（TaskFactory）");
        let (first, first_result) = factory.create(pending::<()>(), TaskConfig::new(())).unwrap();
        let (second, second_result) = factory.create(pending::<()>(), TaskConfig::new(())).unwrap();
        handle.register(first_result.query_handle());
        handle.register(second_result.query_handle());

        registry.maintain(0);
        assert!(registry.index.is_empty());
        assert!(!handle.shared.incoming.load(Ordering::Acquire).is_null());
        registry.maintain(1);
        assert_eq!(registry.index.len(), 1);
        assert!(registry.index.contains_key(&second.id()));

        let (later, later_result) = factory.create(pending::<()>(), TaskConfig::new(())).unwrap();
        handle.register(later_result.query_handle());
        registry.maintain(1);
        assert_eq!(registry.index.len(), 1);
        assert!(!handle.shared.incoming.load(Ordering::Acquire).is_null());
        assert_eq!(registry.query(later.id()).map(|query| query.id()), Some(later.id()));
        assert_eq!(registry.index.len(), 3);
        assert_eq!(registry.query(first.id()).map(|query| query.id()), Some(first.id()));
        assert_eq!(registry.query(second.id()).map(|query| query.id()), Some(second.id()));
    }

    /// 经 TaskRegistry::maintain 留下未更新索引的记录，再经 TaskRegistry::drop 封闭（seal）。
    /// 最后一名参与者仍存续时由原拥有链（head）保管，参与者退出后原附加数据各释放一次。
    #[test]
    fn test_registry_drops_with_unindexed_remainder() {
        let mut registry = TaskRegistry::new();
        let handle = registry.handle();
        let factory = TaskFactory::try_new(
            UnsubmittedSchedule,
            ThreadParker::new().notifier(),
            QueueSupport::IntrusiveLocalAndMpsc,
        ).expect("准备原任务工厂（TaskFactory）");
        let drops = std::array::from_fn::<_, 2, _>(|_| Arc::new(AtomicUsize::new(0)));
        let tasks = drops.each_ref().map(|released| {
            let (task, result) = factory.create(
                pending::<()>(),
                TaskConfig::new(MetadataProbe { released: Arc::clone(released), fail_first: None }),
            ).unwrap();
            handle.register(result.query_handle());
            (task, result)
        });
        registry.maintain(1);
        assert!(!registry.pending_index.is_null());
        assert_eq!(registry.index.len(), 1);
        let reader = handle.shared.enter().expect("开放时允许真实参与者入场");
        drop(tasks);
        drop(registry);
        assert!(handle.shared.enter().is_none());
        assert!(drops.iter().all(|count| count.load(Ordering::Relaxed) == 0));
        drop(reader);
        assert!(drops.iter().all(|count| count.load(Ordering::Relaxed) == 1));
    }

    /// 经 TaskRegistry::maintain 核对共用额度（budget）及继续扫描的位置。
    /// 原任务（Task）确认析构（drop）后，仍需先建立索引（index）才可回收它的任务查询句柄（TaskQuery）。
    #[test]
    fn test_registry_maintenance_resumes_and_releases_only_indexed_finished_tasks() {
        let mut registry = TaskRegistry::new();
        let owner = TaskThread::new();
        let handle = registry.handle();
        let factory = TaskFactory::try_new(
            UnsubmittedSchedule,
            ThreadParker::new().notifier(),
            QueueSupport::IntrusiveLocalAndMpsc,
        ).expect("准备原任务工厂（TaskFactory）");
        let first_released = Arc::new(AtomicUsize::new(0));
        let (first, first_result) = factory.create(
            pending::<()>(),
            TaskConfig::new(MetadataProbe { released: Arc::clone(&first_released), fail_first: None }),
        ).unwrap();
        let (live, live_result) = factory.create(pending::<()>(), TaskConfig::new(())).unwrap();
        let first_id = first.id();
        handle.register(first_result.query_handle());
        handle.register(live_result.query_handle());
        registry.refresh_index();
        drop(first);
        assert_eq!(first_result.snapshot().future_drop, FutureDropStatus::Confirmed);
        drop(first_result);

        registry.maintain(0);
        assert_eq!(first_released.load(Ordering::Relaxed), 0);
        registry.maintain(1);
        assert_eq!(first_released.load(Ordering::Relaxed), 0);

        let later_released = Arc::new(AtomicUsize::new(0));
        let (later, later_result) = factory.create(
            pending::<()>(),
            TaskConfig::new(MetadataProbe { released: Arc::clone(&later_released), fail_first: None }),
        ).unwrap();
        let later_id = later.id();
        handle.register(later_result.query_handle());
        drop(later);
        assert_eq!(later_result.snapshot().future_drop, FutureDropStatus::Confirmed);
        drop(later_result);

        registry.maintain(1);
        assert_eq!(first_released.load(Ordering::Relaxed), 0);
        assert_eq!(later_released.load(Ordering::Relaxed), 0);
        assert!(registry.index.contains_key(&later_id));
        registry.maintain(1);
        assert_eq!(first_released.load(Ordering::Relaxed), 1);
        assert_eq!(later_released.load(Ordering::Relaxed), 0);
        assert!(registry.query(first_id).is_none());
        assert_eq!(registry.query(later_id).map(|query| query.id()), Some(later_id));
        registry.maintain(1);
        assert_eq!(later_released.load(Ordering::Relaxed), 1);
        assert!(registry.query(later_id).is_none());
        assert_eq!(registry.query(live.id()).map(|query| query.id()), Some(live.id()));
        assert_eq!(live_result.snapshot().future_drop, FutureDropStatus::Unconfirmed);
        drop(owner);
    }

    /// 经 TaskRegistry::maintain 验证：额度为零时可回收（reclaim）已退役的只读任务记录（task query record），但不检查下一项。
    /// 经 RegistryShared::enter 保持真实参与资格，以任务附加数据（task metadata）的析构（drop）次数观察释放。
    #[test]
    fn test_registry_zero_budget_reclaims_retired_records_without_scanning() {
        let mut registry = TaskRegistry::new();
        let owner = TaskThread::new();
        let handle = registry.handle();
        let factory = TaskFactory::try_new(
            UnsubmittedSchedule,
            ThreadParker::new().notifier(),
            QueueSupport::IntrusiveLocalAndMpsc,
        ).expect("准备原任务工厂（TaskFactory）");
        let untouched_released = Arc::new(AtomicUsize::new(0));
        let retired_released = Arc::new(AtomicUsize::new(0));
        let create = |released: &Arc<AtomicUsize>| {
            factory.create(
                pending::<()>(),
                TaskConfig::new(MetadataProbe { released: Arc::clone(released), fail_first: None }),
            ).unwrap()
        };
        let (untouched, untouched_result) = create(&untouched_released);
        let (retired, retired_result) = create(&retired_released);
        let untouched_id = untouched.id();
        let retired_id = retired.id();
        handle.register(untouched_result.query_handle());
        handle.register(retired_result.query_handle());
        registry.refresh_index();

        let participant = handle.shared.enter().expect("开放时允许真实参与者入场");
        drop((untouched, retired));
        assert_eq!(untouched_result.snapshot().future_drop, FutureDropStatus::Confirmed);
        assert_eq!(retired_result.snapshot().future_drop, FutureDropStatus::Confirmed);
        drop((untouched_result, retired_result));
        registry.maintain(1);
        assert!(registry.query(retired_id).is_none());
        assert_eq!(retired_released.load(Ordering::Relaxed), 0);
        assert_eq!(untouched_released.load(Ordering::Relaxed), 0);

        drop(participant);
        assert_eq!(retired_released.load(Ordering::Relaxed), 0);
        registry.maintain(0);
        assert_eq!(retired_released.load(Ordering::Relaxed), 1);
        assert_eq!(untouched_released.load(Ordering::Relaxed), 0);
        assert_eq!(registry.query(untouched_id).map(|query| query.id()), Some(untouched_id));

        registry.maintain(1);
        assert!(registry.query(untouched_id).is_none());
        assert_eq!(untouched_released.load(Ordering::Relaxed), 1);
        drop(owner);
        drop(registry);
        assert_eq!(retired_released.load(Ordering::Relaxed), 1);
        assert_eq!(untouched_released.load(Ordering::Relaxed), 1);
    }

    /// 在所属域（TaskThread）退出时由原异步计算（Future）的析构（drop）报告失败。
    struct FailingFuture(Rc<Cell<usize>>);

    impl Future for FailingFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            panic!("退出不应轮询（poll）原异步计算（Future）");
        }
    }

    impl Drop for FailingFuture {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
            std::panic::panic_any("registry-owner-cleanup-failure");
        }
    }

    /// 经 TaskRegistry::unconfirmed_counts 核对所属域（TaskThread）真实退出后的两个计数。
    /// 三份任务（Task）仅登记、未提交：本地清理失败计入总数，成功清理及未绑定者不计入。
    #[test]
    fn test_registry_counts_unadmitted_owner_failure_after_task_thread_drop() {
        let registry = TaskRegistry::new();
        let owner = TaskThread::new();
        let owner_id = owner.id();
        let handle = registry.handle();
        let factory = TaskFactory::try_new(
            UnsubmittedSchedule,
            ThreadParker::new().notifier(),
            QueueSupport::IntrusiveLocalAndMpsc,
        ).expect("准备原任务工厂（TaskFactory）");
        let attempts = Rc::new(Cell::new(0));
        let (failed, failed_result) = factory.create_local(
            &owner, FailingFuture(Rc::clone(&attempts)), TaskConfig::new(()),
        ).unwrap();
        let (finished, finished_result) = factory.create_local(
            &owner, pending::<()>(), TaskConfig::new(()),
        ).unwrap();
        let (unbound, unbound_result) = factory.create(pending::<()>(), TaskConfig::new(())).unwrap();
        handle.register(failed_result.query_handle());
        handle.register(finished_result.query_handle());
        handle.register(unbound_result.query_handle());
        assert_eq!(owner.live_tasks(), 2);
        assert_eq!(attempts.get(), 0);

        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner)))
            .expect_err("实际退出应传播原恐慌（panic）");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"registry-owner-cleanup-failure"));
        let failure = failed_result.snapshot();
        assert_eq!(failure.owner, Some(owner_id));
        assert_eq!(failure.execution, pi_async_task::definitions::ExecutionStatus::CleanupUnconfirmed);
        assert_eq!(failure.future_drop, FutureDropStatus::Unconfirmed);
        assert!(!failure.admitted);
        assert_eq!(finished_result.snapshot().future_drop, FutureDropStatus::Confirmed);
        let outside = unbound_result.snapshot();
        assert_eq!(outside.owner, None);
        assert_eq!(outside.future_drop, FutureDropStatus::Unconfirmed);

        assert_eq!(registry.unconfirmed_counts(owner_id), (1, 0));
        drop((failed, failed_result, finished, finished_result, unbound, unbound_result));
        drop(registry);
        assert_eq!(attempts.get(), 1);
    }

    /// 经 RegistryShared::enter 取得真实参与者守卫（RegistryReadGuard），保持它跨越登记表封闭（seal）。
    /// 同线程固定两种责任的先后；不改参与者计数，验证登记表不能提前释放原查询（TaskQuery）。
    #[test]
    fn test_registry_defers_metadata_release_until_the_last_participant_leaves() {
        let owner = TaskThread::new();
        let registry = TaskRegistry::new();
        let handle = registry.handle();
        let factory = TaskFactory::try_new(
            UnsubmittedSchedule,
            ThreadParker::new().notifier(),
            QueueSupport::IntrusiveLocalAndMpsc,
        ).expect("准备原任务工厂（TaskFactory）");
        let released = Arc::new(AtomicUsize::new(0));
        let (task, result) = factory.create_local(
            &owner,
            pending::<()>(),
            TaskConfig::new(MetadataProbe { released: Arc::clone(&released), fail_first: None }),
        ).unwrap();
        handle.register(result.query_handle());
        let participant = handle.shared.enter().expect("开放时允许真实参与者入场");

        drop(owner);
        assert_eq!(result.snapshot().future_drop, FutureDropStatus::Confirmed);
        drop((task, result));
        drop(registry);
        assert!(handle.shared.enter().is_none());
        assert_eq!(released.load(Ordering::Relaxed), 0);

        drop(participant);
        assert_eq!(released.load(Ordering::Relaxed), 1);
        drop(handle);
        assert_eq!(released.load(Ordering::Relaxed), 1);
    }

    /// 经 TaskRegistry::maintain 摘下已完成的只读任务记录（task query record），但原参与者仍在时不释放它。
    /// 随后经 TaskRegistry::drop 封闭（seal），两条链的任务附加数据（task metadata）都由最后退出的线程（thread）释放。
    /// 原子操作（atomic operation）只安排测试（test）的先后；等待上限用于报告测试（test）异常，不判断生产行为。
    #[test]
    fn test_registry_last_remote_participant_releases_live_and_retired_records() {
        use std::{thread, time::{Duration, Instant}};

        let mut registry = TaskRegistry::new();
        let owner = TaskThread::new();
        let handle = registry.handle();
        let factory = TaskFactory::try_new(
            UnsubmittedSchedule,
            ThreadParker::new().notifier(),
            QueueSupport::IntrusiveLocalAndMpsc,
        ).expect("准备原任务工厂（TaskFactory）");
        let live_released = Arc::new(AtomicUsize::new(0));
        let retired_released = Arc::new(AtomicUsize::new(0));
        let (live, live_result) = factory.create_local(
            &owner,
            pending::<()>(),
            TaskConfig::new(MetadataProbe { released: Arc::clone(&live_released), fail_first: None }),
        ).unwrap();
        let (finished, finished_result) = factory.create(
            pending::<()>(),
            TaskConfig::new(MetadataProbe { released: Arc::clone(&retired_released), fail_first: None }),
        ).unwrap();
        let retired_id = finished.id();
        handle.register(live_result.query_handle());
        handle.register(finished_result.query_handle());
        registry.refresh_index();
        drop(finished);
        assert_eq!(finished_result.snapshot().future_drop, FutureDropStatus::Confirmed);
        drop(finished_result);

        let remote = handle.clone();
        let entered = AtomicBool::new(false);
        let leave = AtomicBool::new(false);
        let wait_for = |signal: &AtomicBool, message: &str| {
            let deadline = Instant::now() + Duration::from_secs(5);
            while !signal.load(Ordering::Acquire) {
                assert!(Instant::now() < deadline, "{message}");
                thread::yield_now();
            }
        };
        thread::scope(|scope| {
            let entered = &entered;
            let leave = &leave;
            let remote_thread = scope.spawn(move || {
                let participant = remote.shared.enter().expect("开放时允许参与者入场");
                entered.store(true, Ordering::Release);
                wait_for(leave, "等待本地管理者封闭（seal）超时");
                drop(participant);
            });
            wait_for(entered, "等待另一线程（thread）入场超时");
            registry.maintain(1);
            assert!(registry.query(retired_id).is_none());
            assert_eq!(retired_released.load(Ordering::Relaxed), 0);
            assert_eq!(live_released.load(Ordering::Relaxed), 0);

            drop(owner);
            assert_eq!(live_result.snapshot().future_drop, FutureDropStatus::Confirmed);
            drop((live, live_result));
            let local = handle.shared.enter().expect("封闭（seal）前取得第二份参与资格");
            drop(registry);
            assert!(handle.shared.enter().is_none());
            assert_eq!(retired_released.load(Ordering::Relaxed), 0);
            assert_eq!(live_released.load(Ordering::Relaxed), 0);
            drop(local);
            assert_eq!(retired_released.load(Ordering::Relaxed), 0);
            assert_eq!(live_released.load(Ordering::Relaxed), 0);

            leave.store(true, Ordering::Release);
            remote_thread.join().expect("最后参与者应正常退出");
            assert_eq!(retired_released.load(Ordering::Relaxed), 1);
            assert_eq!(live_released.load(Ordering::Relaxed), 1);
        });
    }

    /// 经实际 TaskThread::drop 结束原所属域，再验证终末回收（final reclamation）传播原恐慌（panic）。
    /// 最先释放的附加数据（metadata）失败后，剩余各份仍恰好释放一次；不约束原任务（Task）的释放顺序。
    #[test]
    fn test_registry_releases_remaining_metadata_after_the_first_drop_panics() {
        let owner = TaskThread::new();
        let registry = TaskRegistry::new();
        let handle = registry.handle();
        let factory = TaskFactory::try_new(
            UnsubmittedSchedule,
            ThreadParker::new().notifier(),
            QueueSupport::IntrusiveLocalAndMpsc,
        ).expect("准备原任务工厂（TaskFactory）");
        let fail_first = Arc::new(AtomicBool::new(true));
        let probes = std::array::from_fn::<_, 3, _>(|_| Arc::new(AtomicUsize::new(0)));
        let tasks = probes.each_ref().map(|released| {
            let (task, result) = factory.create_local(
                &owner,
                pending::<()>(),
                TaskConfig::new(MetadataProbe {
                    released: Arc::clone(released),
                    fail_first: Some(Arc::clone(&fail_first)),
                }),
            ).unwrap();
            handle.register(result.query_handle());
            (task, result)
        });

        drop(owner);
        drop(tasks);
        assert!(probes.iter().all(|count| count.load(Ordering::Relaxed) == 0));
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(registry)))
            .expect_err("应传播原附加数据（metadata）的首个恐慌（panic）");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"registry-metadata-drop"));
        assert!(probes.iter().all(|count| count.load(Ordering::Relaxed) == 1));
        drop(handle);
        assert!(probes.iter().all(|count| count.load(Ordering::Relaxed) == 1));
    }
}
