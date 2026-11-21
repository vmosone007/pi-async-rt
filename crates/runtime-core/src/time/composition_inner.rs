#[allow(unused_variables)]
fn sleep_new_inner<A: TimerAccess>(access: A, duration: Duration) -> Sleep<A> {
    let min_positive = Duration::from_millis(1);
    let max = Duration::from_secs(1);
    let (timeout, error) = if duration.is_zero() {
        (None, None)
    } else if duration < min_positive || duration > max {
        (None, Some(TimerError::InvalidDuration { value: duration, min_positive, max: Some(max) }))
    } else {
        (Some(Timeout::new(access, duration)), None)
    };
    Sleep { timeout, error, done: false }
}
#[allow(unused_variables)]
fn sleep_poll_inner<A: TimerAccess>(
    this: Pin<&mut Sleep<A>>,
    cx: &mut Context<'_>,
) -> Poll<pi_result::RawResult<(), TimerError>> {
    let this = this.get_mut();
    assert!(!this.done, "已完成的睡眠不能再次轮询（sleep polled after completion）");
    let result = match this.error.take() {
        Some(error) => Poll::Ready(Err(error)),
        None => match this.timeout.as_mut() {
            Some(timeout) => Pin::new(timeout).poll(cx),
            None => Poll::Ready(Ok(())),
        },
    };
    if result.is_ready() {
        this.done = true;
    }
    result
}
#[allow(unused_variables)]
fn timeout_on_new_inner<A: TimerAccess, F: Future>(
    access: A,
    future: F,
    duration: Duration,
) -> TimeoutOn<A, F> {
    TimeoutOn { future: Some(future), access: Some(access), duration, timeout: None, done: false }
}
#[allow(unused_variables)]
fn timeout_on_poll_inner<A: TimerAccess, F: Future>(
    this: Pin<&mut TimeoutOn<A, F>>,
    cx: &mut Context<'_>,
) -> Poll<pi_result::RawResult<F::Output, TimeoutError>> {
    // SAFETY: 只有 future 字段结构性固定（structurally pinned）；它只经 Pin 借用或原地替换，绝不移出 F。
    let this = unsafe { this.get_unchecked_mut() };
    assert!(!this.done, "已完成的超时组合不能再次轮询（timeout composition polled after completion）");
    // SAFETY: future 与整个组合共用其固定地址（pinning）；字段释放也在原位置。
    let mut future = unsafe { Pin::new_unchecked(&mut this.future) };
    let business = future.as_mut().as_pin_mut().expect("异步计算（Future）必须仍在原位置").poll(cx);
    let result = if let Poll::Ready(value) = business {
        Ok(value)
    } else {
        if this.timeout.is_none() {
            let access = this.access.take().expect("首次计时必须有原计时访问（TimerAccess）");
            this.timeout = Some(Timeout::new(access, this.duration));
        }
        let timeout = this.timeout.as_mut().expect("已开始的计时器（timer）必须仍在");
        match Pin::new(timeout).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Ok(())) => Err(TimeoutError::Elapsed),
            Poll::Ready(Err(error)) => Err(TimeoutError::Timer(error)),
        }
    };
    this.done = true;
    // 计时值移至局部；即使业务析构（drop）展开，已建立的计时兴趣仍会结束。
    let timeout = this.timeout.take();
    future.set(None);
    drop(timeout);
    Poll::Ready(result)
}
