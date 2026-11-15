#[allow(unused_variables)]
fn serial_admit_inner(state: &AdmissionState) -> bool {
    todo!("P0；仅Open且in_flight_tasks==0且internal_tasks==0时允许取下一普通外部当前有限批次。internal包含未admit本地子任务及已有唤醒返回，防准备中任务漏过。一个已取批次仍按既定规则保序入本地，不将单线程串行误改成每次只取一个（后一限制属于以后池成员）。不限制并发提交、原返回、定时、idle或本地派发。CleanupUnconfirmed保原未确认数，不为继续新批次清零；调用者可显式close。无时钟/分配/锁/用户回调。")
}
#[allow(unused_variables)]
fn concurrent_admit_inner(state: &AdmissionState) -> bool {
    todo!("P0；仅status==Open即允许本轮一次普通获取，不因其它已接纳任务Pending/in_flight>0暂停。仍服从源非等待/有限当前范围及关闭检查，不创建线程/迁移已绑定任务。静态布尔规则，无分配/其它查询。")
}
