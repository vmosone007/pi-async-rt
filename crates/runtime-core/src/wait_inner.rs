#[cfg(test)]
#[path = "wait_inner_test.rs"]
mod wait_inner_test;

fn wait_one_timeout_poll_inner<A: TimerAccess, T>(
    this: Pin<&mut WaitOneTimeout<A, T>>,
    cx: &mut Context<'_>,
) -> Poll<pi_result::RawResult<T, WaitTimeoutError>> {
    // 安全（safety）：本包装只固定一个内部组合字段；投影保持原地址，不移动、不替换、不额外实现Unpin。
    let inner = unsafe { this.map_unchecked_mut(|value| &mut value.inner) };
    match inner.poll(cx) {
        Poll::Pending => Poll::Pending,
        Poll::Ready(Ok(Ok(value))) => Poll::Ready(Ok(value)),
        Poll::Ready(Ok(Err(error))) => Poll::Ready(Err(WaitTimeoutError::Wait(error))),
        Poll::Ready(Err(TimeoutError::Elapsed)) => Poll::Ready(Err(WaitTimeoutError::Elapsed)),
        Poll::Ready(Err(TimeoutError::Timer(error))) => Poll::Ready(Err(WaitTimeoutError::Timer(error))),
    }
}

fn wait_any_timeout_poll_inner<A: TimerAccess, T>(
    this: Pin<&mut WaitAnyTimeout<A, T>>,
    cx: &mut Context<'_>,
) -> Poll<pi_result::RawResult<T, WaitTimeoutError>> {
    // 安全（safety）：同one；胜者语义及关闭规则由原WaitAny与TimeoutOn负责，不在此重做协议。
    let inner = unsafe { this.map_unchecked_mut(|value| &mut value.inner) };
    match inner.poll(cx) {
        Poll::Pending => Poll::Pending,
        Poll::Ready(Ok(Ok(value))) => Poll::Ready(Ok(value)),
        Poll::Ready(Ok(Err(error))) => Poll::Ready(Err(WaitTimeoutError::Wait(error))),
        Poll::Ready(Err(TimeoutError::Elapsed)) => Poll::Ready(Err(WaitTimeoutError::Elapsed)),
        Poll::Ready(Err(TimeoutError::Timer(error))) => Poll::Ready(Err(WaitTimeoutError::Timer(error))),
    }
}

fn wait_all_timeout_poll_inner<A: TimerAccess, T>(
    this: Pin<&mut WaitAllTimeout<A, T>>,
    cx: &mut Context<'_>,
) -> Poll<pi_result::RawResult<Vec<pi_result::RawResult<T, WaitError>>, TimeoutError>> {
    // 安全（safety）：同one；完整有序向量与逐项错误由原WaitAll提供，超时不交部分向量。
    let inner = unsafe { this.map_unchecked_mut(|value| &mut value.inner) };
    inner.poll(cx)
}
