#[allow(unused_variables)]
fn serial_admit_inner(state: &AdmissionState) -> bool {
    state.status == pi_async_runtime_core::RuntimeStatus::Open
        && state.in_flight_tasks == 0
        && state.internal_tasks == 0
}
#[allow(unused_variables)]
fn concurrent_admit_inner(state: &AdmissionState) -> bool {
    state.status == pi_async_runtime_core::RuntimeStatus::Open
}
