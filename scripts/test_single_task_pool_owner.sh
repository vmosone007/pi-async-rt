#!/usr/bin/env bash
# 单线程 owner 修复的标准回归入口：默认/serial 顺序、单构建作业、单测试线程。
# 范围详见 docs/SINGLE_TASK_POOL_OWNER_ACCEPTANCE.md；不执行 MultiTaskRuntime、
# ignored、人工观测、千万任务旧测试或任何基准/检测器。参数只允许 debug/release。
# 当前仓库是单包 pi-async-rt；后续新增测试须先核验调用链并更新归档，不能只按名称猜测。
set -euo pipefail
cd "$(dirname "$0")/.."

profile=()
if (( $# > 1 )); then printf '%s\n' '只允许一个 profile 参数' >&2; exit 2; fi
case "${1:-debug}" in
    debug) ;;
    release) profile=(--release) ;;
    *) printf '%s\n' '只允许 debug 或 release' >&2; exit 2 ;;
esac
export RUST_TEST_THREADS=1

# 每个调用单独进程退出，避免旧标准测试未 join 的 worker 影响下一目标。
run() {
    local expected=$1 output status=0
    shift
    printf '执行:'
    printf ' %q' "$@"
    printf '\n'
    output=$(timeout 300s "$@" 2>&1) || status=$?
    printf '%s\n' "$output"
    if (( status != 0 )); then return "$status"; fi
    # 精确筛选改名、feature 漂移或意外少跑，不能以 Cargo 的零退出码冒充通过。
    if [[ "$output" != *"test result: ok. $expected passed; 0 failed;"* ]]; then
        printf '测试数量与冻结清单不符，预期 %s 项通过\n' "$expected" >&2
        return 1
    fi
}

for mode in default serial; do
    feature=()
    if [[ "$mode" == serial ]]; then feature=(--features serial); fi
    cargo_test=(cargo test -p pi-async-rt --locked --offline -j 1 "${profile[@]}" "${feature[@]}")
    run 10 "${cargo_test[@]}" --lib -- --test-threads=1 \
        --skip rt::serial_local_thread::test_local_runtime_block_on \
        --skip rt::serial_local_compatible_wasm_runtime::test_local_compatible_wasm_runtime_block_on
    for target in single_task_pool_owner single_task_pool_owner_contract single_task_pool_owner_concurrency single_task_pool_owner_alloc block_on; do
        case "$target" in
            single_task_pool_owner) expected=5 ;;
            single_task_pool_owner_contract) expected=13; if [[ "$mode" == serial ]]; then expected=15; fi ;;
            single_task_pool_owner_concurrency) expected=2 ;;
            single_task_pool_owner_alloc) expected=1 ;;
            block_on) expected=3 ;;
        esac
        run "$expected" "${cargo_test[@]}" --test "$target" -- --test-threads=1
    done
    expected=9
    if [[ "$mode" == serial ]]; then expected=3; fi
    run "$expected" "${cargo_test[@]}" --test async_value -- --test-threads=1 --skip multi_thread
    run 1 "${cargo_test[@]}" --test timeout_waiter -- --test-threads=1 --skip multi_thread
    if [[ "$mode" == default ]]; then
        run 1 "${cargo_test[@]}" --test worker_wakeup -- --test-threads=1 --exact \
            default_runtime::test_worker_thread_external_spawn_wakes_sleeping_worker
        run 1 "${cargo_test[@]}" --test async_task_scheduling_runtime_matrix -- --test-threads=1 --exact \
            default_runtime::test_single_runner_managed_and_legacy_scheduling_contracts
        run 1 "${cargo_test[@]}" --test async_task_scheduling_runtime_matrix -- --test-threads=1 --exact \
            default_runtime::test_worker_runtime_self_wake_pending_completes_exactly_once
    fi
    # Rustdoc 的 --test-args 会再次拆分含空格的参数，不能直接传完整带空格测试名。
    # 先核验当前清单的唯一行号后缀，来源或行号漂移时停止，避免漏测或误跑多线程示例。
    doc_list=$(timeout 300s "${cargo_test[@]}" --doc -- --list)
    printf '%s\n' "$doc_list"
    [[ $(printf '%s\n' "$doc_list" | rg -c ': test$') == 11 ]]
    [[ $(printf '%s\n' "$doc_list" | rg -c '^src/lib.rs - \(line 19\): test$') == 1 ]]
    [[ $(printf '%s\n' "$doc_list" | rg -c '^src/lib.rs - \(line 26\): test$') == 1 ]]
    [[ $(printf '%s\n' "$doc_list" | rg -c '26\): test$') == 1 ]]
    run 7 "${cargo_test[@]}" --doc -- --test-threads=1 --skip rt/multi_thread.rs --skip '26)'
done
