fn manual_clock_new_inner() -> pi_result::Result<ManualClock> {
    pi_clock_manual::global::try_now()?;
    Ok(ManualClock)
}
#[allow(unused_variables)]
fn manual_clock_now_inner(this: &ManualClock) -> pi_result::Result<GlobalManualInstant> {
    Ok(pi_clock_manual::global::try_refresh()?.published)
}
#[allow(unused_variables)]
fn manual_clock_add_inner(
    this: &ManualClock,
    now: &GlobalManualInstant,
    duration: Duration,
) -> pi_result::Result<GlobalManualInstant> {
    now.try_add(duration)
}
#[allow(unused_variables)]
fn manual_clock_remaining_inner(
    this: &ManualClock,
    now: &GlobalManualInstant,
    deadline: &GlobalManualInstant,
) -> pi_result::Result<Duration> {
    if deadline <= now {
        Ok(Duration::ZERO)
    } else {
        deadline.try_duration_since(*now)
    }
}
