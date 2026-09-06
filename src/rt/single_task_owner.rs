//! 单线程内置池的真实线程所有权，不把运行时展示编号当作消费权限。
//!
//! 契约入口：`docs/SINGLE_TASK_POOL_OWNER_DESIGN.md#owner-design`；
//! 红线入口：`tests/single_task_pool_owner.rs`。本模块不负责调度、唤醒、计数或迁移。
//! 首次消费永久绑定一个标准库线程标识；生产者只能查询，不能抢先取得消费权限。

use std::cell::Cell;
use std::convert::TryFrom;
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicUsize, Ordering};

use super::PI_ASYNC_THREAD_LOCAL_ID;

thread_local! {
    // 仅消费入口初始化；无析构器，不保留运行时、任务或用户对象。
    static CONSUMER_THREAD: Cell<usize> = const { Cell::new(0) };
}

/// 一个池占一个原子字，不增加每任务存储，不建立全局注册表。
///
/// 标准库 ThreadId 在线程退出后也不复用。0 专门表示未绑定，转换失败时拒绝消费，
/// 不截断、不回绕。这个值不是操作系统线程号，也不是公开 runtime ID。
/// 原子只保护授权，容器本身仍由唯一 owner 串行访问；不能授权其它线程 Drop !Send 值。
pub(super) struct SingleTaskOwner {
    thread: AtomicUsize,
}

impl SingleTaskOwner {
    /// 构造未绑定身份；纯初始化，O(1) 时间/空间，无分配、锁、回调或运行时依赖。
    pub(super) const fn new() -> Self {
        Self { thread: AtomicUsize::new(0) }
    }

    /// 查询调用线程是否拥有本池；未初始化或 TLS 不可用时返回 false。
    ///
    /// 非纯函数（读取 TLS/原子快照），无写入副作用。O(1)，无锁/自旋/分配/Clone，
    /// 不阻塞、不调用用户代码、不持借用跨 await；可从外部 Waker 安全调用。
    #[inline]
    pub(super) fn is_current(&self) -> bool {
        CONSUMER_THREAD.try_with(|thread| {
            let token = thread.get();
            token != 0 && self.thread.load(Ordering::Acquire) == token
        }).unwrap_or(false)
    }

    /// 在触及本地队列、选择器或 timer 之前验证或首次绑定真实消费者。
    ///
    /// 成功为 ()，错误为 PermissionDenied（其它 owner）或 Other（TLS/标识不可用）。
    /// 同 owner 重复调用幂等；其它线程不能接管已退出 owner 的池。首次调用写 TLS、
    /// 最多一次强 CAS；热路径只有缓存读取和原子 load，O(1)，无阻塞锁或自旋等待。
    /// 首次标准库线程查询属于冷路径；不保存 Thread/Arc，不触发用户回调。
    #[inline]
    pub(super) fn bind_current(&self, runtime_id: usize) -> Result<()> {
        let token = CONSUMER_THREAD.try_with(|thread| {
            let cached = thread.get();
            if cached != 0 { return Ok(cached); }
            let value = std::thread::current().id().as_u64().get();
            let token = usize::try_from(value)
                .map_err(|_| Error::new(ErrorKind::Other, "线程标识不能表示为 usize"))?;
            if token == 0 {
                return Err(Error::new(ErrorKind::Other, "线程标识不能为零"));
            }
            thread.set(token);
            Ok(token)
        }).map_err(|_| Error::new(ErrorKind::Other, "消费者线程局部存储不可用"))??;

        let owner = self.thread.load(Ordering::Acquire);
        if owner == token {
            return Ok(());
        }
        if owner != 0 {
            return Err(Error::new(ErrorKind::PermissionDenied, "单线程任务池只能由首次消费者线程驱动"));
        }
        self.claim(token)?;
        // 安全：旧编号存储是当前线程独有 TLS；不跨回调持有引用，也不覆盖已有身份。
        // 权限已独立验证，packed ID 只服务既有查询协议，绝不能反过来授予本地访问权。
        PI_ASYNC_THREAD_LOCAL_ID.try_with(|id| unsafe {
            if *id.get() == usize::MAX { *id.get() = runtime_id << 32; }
        }).map_err(|_| Error::new(ErrorKind::Other, "运行时线程局部存储不可用"))
    }

    /// 局部授权状态推进；token 必须非零。单次 CAS 的失败是拒绝，不是重试信号。
    /// 仅消费入口调用；不触及被保护对象。测试人工 token 只验证本局部不变量。
    #[inline]
    fn claim(&self, token: usize) -> Result<()> {
        if token == 0 {
            return Err(Error::new(ErrorKind::InvalidInput, "消费者标识不能为零"));
        }
        match self.thread.compare_exchange(0, token, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => Ok(()),
            Err(owner) if owner == token => Ok(()),
            Err(_) => Err(Error::new(ErrorKind::PermissionDenied, "单线程任务池只能由首次消费者线程驱动")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn test_owner_local_state_boundaries() {
        let owner = SingleTaskOwner::new();
        assert_eq!(owner.claim(0).unwrap_err().kind(), ErrorKind::InvalidInput);
        assert_eq!(owner.thread.load(Ordering::Acquire), 0);
        owner.claim(usize::MAX).unwrap();
        owner.claim(usize::MAX).unwrap();
        assert_eq!(owner.claim(1).unwrap_err().kind(), ErrorKind::PermissionDenied);
        assert_eq!(owner.thread.load(Ordering::Acquire), usize::MAX);
        assert_eq!(std::mem::size_of::<SingleTaskOwner>(), std::mem::size_of::<usize>());
    }

    #[test]
    fn test_owner_first_consumer_race_has_one_winner() {
        let owner = Arc::new(SingleTaskOwner::new());
        let barrier = Arc::new(Barrier::new(4));
        let threads: Vec<_> = (0..4).map(|_| {
            let owner = owner.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                assert!(!owner.is_current());
                barrier.wait();
                let result = owner.bind_current(1);
                assert_eq!(result.is_ok(), owner.is_current());
                if let Err(error) = &result { assert_eq!(error.kind(), ErrorKind::PermissionDenied); }
                result.is_ok()
            })
        }).collect();
        let winners = threads.into_iter().map(|thread| usize::from(thread.join().unwrap())).sum::<usize>();
        assert_eq!(winners, 1);
        assert!(!owner.is_current());
        assert_eq!(owner.bind_current(1).unwrap_err().kind(), ErrorKind::PermissionDenied);
    }

    #[test]
    fn test_owner_query_does_not_initialize_and_binding_preserves_legacy_id() {
        std::thread::spawn(|| {
            let first = SingleTaskOwner::new();
            let second = SingleTaskOwner::new();
            assert_eq!(CONSUMER_THREAD.with(Cell::get), 0);
            assert!(!first.is_current());
            assert_eq!(CONSUMER_THREAD.with(Cell::get), 0);
            first.bind_current(7).unwrap();
            first.bind_current(8).unwrap();
            second.bind_current(9).unwrap();
            assert!(first.is_current());
            assert!(second.is_current());
            // 安全：只在此测试线程读取其私有 TLS，不能用写入伪造生产授权。
            assert_eq!(PI_ASYNC_THREAD_LOCAL_ID.with(|id| unsafe { *id.get() }), 7 << 32);
        }).join().unwrap();
    }
}
