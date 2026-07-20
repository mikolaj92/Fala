from std.os import remove
from std.collections import List
from fala.domain import RuntimePool, RuntimeBudget, DelegationPolicy
from fala.domain_store import NativeDomainStore
from fala.runtime_policy import parse_runtime_refs_json, select_runtime, merge_runtime_budgets, parse_runtime_budget_json, create_delegation_envelope, extract_delegation_envelope, resolve_delegation_policy


def _check(condition: Bool, message: String) raises:
    if not condition: raise Error("runtime policy smoke: " + message)


def _expect_error(text: String, needle: String) raises:
    var failed = False
    try:
        var _ = parse_runtime_refs_json(text)
    except err:
        failed = String(err).find(needle) >= 0
    _check(failed, "typed malformed JSON error")




def main() raises:
    var refs = parse_runtime_refs_json("[{\"id\":\"a\",\"metadata\":{\"load\":9}},{\"id\":\"b\",\"metadata\":{\"load\":1}}]")
    _check(len(refs) == 2 and refs[1].id == "b", "strict runtime refs")
    var pool = RuntimePool("pool", "[{\"id\":\"a\",\"metadata\":{\"load\":9}},{\"id\":\"b\",\"metadata\":{\"load\":1}}]", "[\"case\"]", "{\"policy\":\"least_busy\",\"round_robin_index\":1}")
    var least = select_runtime(pool, "case")
    _check(least.runtime.id == "b" and least.policy == "least_busy", "least busy selection")
    var pending_pool = RuntimePool("pending-pool", "[{\"id\":\"a\",\"metadata\":{\"pending_processes\":8}},{\"id\":\"b\",\"metadata\":{\"pending_processes\":2}}]", "[\"case\"]", "{\"policy\":\"least_busy\"}")
    var pending_least = select_runtime(pending_pool, "case")
    _check(pending_least.runtime.id == "b", "least busy falls back to pending processes")
    var pending_tie_pool = RuntimePool("pending-tie-pool", "[{\"id\":\"a\",\"metadata\":{\"pending_processes\":2}},{\"id\":\"b\",\"metadata\":{\"pending_processes\":2}}]", "[\"case\"]", "{\"policy\":\"least_busy\"}")
    var pending_tie = select_runtime(pending_tie_pool, "case")
    _check(pending_tie.runtime.id == "a" and pending_tie.index == 0, "least busy pending tie is deterministic")
    var precedence_pool = RuntimePool("precedence-pool", "[{\"id\":\"a\",\"metadata\":{\"load\":5,\"pending_processes\":0}},{\"id\":\"b\",\"metadata\":{\"pending_processes\":2}}]", "[\"case\"]", "{\"policy\":\"least_busy\"}")
    var precedence = select_runtime(precedence_pool, "case")
    _check(precedence.runtime.id == "b", "explicit load takes precedence over pending processes")
    var rr = select_runtime(pool, "case", "round_robin", round_robin_index=3)
    _check(rr.runtime.id == "b" and rr.next_index == 0, "round robin determinism")
    var negative_rr = select_runtime(pool, "case", "round_robin", round_robin_index=-2)
    _check(negative_rr.runtime.id == "a" and negative_rr.index == 0, "negative round robin index uses modulo")
    var string_pool = RuntimePool("string-values", "[{\"id\":\"a\",\"metadata\":{\"load\":\"9\"}},{\"id\":\"b\",\"metadata\":{\"load\":\"1\"}}]", "[\"case\"]", "{\"policy\":\"least_busy\",\"round_robin_index\":\"1\"}")
    var string_least = select_runtime(string_pool, "case")
    _check(string_least.runtime.id == "b", "numeric strings coerce for load metadata")
    var string_rr = select_runtime(string_pool, "case", "round_robin")
    _check(string_rr.runtime.id == "b", "numeric strings coerce for round robin metadata")
    var manual = select_runtime(pool, "case", "manual", manual_runtime_id="a")
    _check(manual.runtime.id == "a", "manual selection")
    var policy = DelegationPolicy("policy", "pool", "[\"case\"]", RuntimeBudget(runtime_hops=1, impulse_count=2), "{\"opaque\":true}")
    var resolved = resolve_delegation_policy(pool, policy, "case", RuntimeBudget(impulse_count=5), round_robin_index=0)
    _check(resolved.runtime.id == "b" and resolved.policy == "least_busy", "policy resolution honors pool metadata")
    var rejected = False
    try:
        _ = resolve_delegation_policy(pool, policy, "other", manual_runtime_id="a")
    except err:
        rejected = String(err).find("impulse_type_not_accepted") >= 0
    _check(rejected, "policy impulse allow-list")
    var merged = merge_runtime_budgets(RuntimeBudget(runtime_hops=2, impulse_count=2), RuntimeBudget(runtime_hops=1, impulse_count=5))
    _check(merged.runtime_hops == 1 and merged.impulse_count == 2 and not merged.allows(impulse_count=3), "budget exhaustion")
    var unlimited = parse_runtime_budget_json("{\"runtime_hops\":null,\"attempts\":2}")
    var zero = parse_runtime_budget_json("{\"runtime_hops\":0}")
    _check(not unlimited.runtime_hops_limited and unlimited.attempts_limited and zero.runtime_hops_limited and not zero.allows(runtime_hops=1), "budget null versus zero")
    var envelope = create_delegation_envelope("delivery", "run-target", "pool", merged, "{\"tenant\":\"local\"}")
    var extracted = extract_delegation_envelope(envelope)
    _check(extracted.delivery_id == "delivery" and extracted.target_run_id == "run-target" and extracted.pool_id == "pool" and extracted.metadata.find("tenant") >= 0, "delegation envelope roundtrip")
    # Persistence must retain explicit zero budgets as exhausted, not unlimited.
    var store = NativeDomainStore(":memory:\0")
    store.initialize()
    store.put_runtime_pool(pool)
    var persisted_policy = DelegationPolicy("persisted", "pool", "[\"case\"]", RuntimeBudget(runtime_hops=0, runtime_hops_limited=True), "{}")
    store.put_delegation_policy(persisted_policy)
    var loaded_policy = store.get_delegation_policy("persisted")
    _check(loaded_policy.budget.runtime_hops_limited and not loaded_policy.budget.allows(runtime_hops=1), "persisted zero budget remains exhausted")
    var non_rr_selected = store.select_runtime_and_advance("pool", "case")
    _check(non_rr_selected.policy == "least_busy", "non-round-robin selection policy")
    var non_rr_pool = store.get_runtime_pool("pool")
    _check(non_rr_pool.metadata.find("\"round_robin_index\":1") >= 0, "non-round-robin selection preserves cursor")
    var first_policy_selected = store.select_runtime_and_advance("pool", "case", policy="first")
    _check(first_policy_selected.policy == "first" and first_policy_selected.runtime.id == "a", "first policy selection")
    var after_first_policy = store.get_runtime_pool("pool")
    _check(after_first_policy.metadata.find("\"round_robin_index\":1") >= 0, "first policy preserves round-robin cursor")
    # File-backed pool/policy selection must survive a close/reopen, including
    # the persisted round-robin cursor used by the reference driver.
    var runtime_path = "/tmp/fala-runtime-policy-reopen.sqlite"
    try:
        remove(runtime_path)
    except err:
        pass
    var file_store = NativeDomainStore(runtime_path)
    file_store.initialize()
    var rr_pool = RuntimePool("rr-pool", "[{\"id\":\"target-a\"},{\"id\":\"target-b\"}]", "[\"case\"]", "{\"policy\":\"round_robin\",\"round_robin_index\":0}")
    file_store.put_runtime_pool(rr_pool)
    var reloaded_pool = file_store.get_runtime_pool("rr-pool")
    _check(reloaded_pool.id == "rr-pool" and reloaded_pool.runtimes.find("target-a") >= 0 and reloaded_pool.impulse_types == "[\"case\"]", "file-backed pool fields")
    var first_rr = file_store.select_runtime_and_advance("rr-pool", "case")
    _check(first_rr.runtime.id == "target-a" and first_rr.next_index == 1, "atomic round robin first target")
    var second_rr = file_store.select_runtime_and_advance("rr-pool", "case")
    _check(second_rr.runtime.id == "target-b" and second_rr.next_index == 0, "atomic round robin second target")
    file_store.close()
    var reopened_store = NativeDomainStore.open(runtime_path)
    reopened_store.initialize()
    var persisted_pool = reopened_store.get_runtime_pool("rr-pool")
    _check(persisted_pool.metadata.find("\"round_robin_index\":0") >= 0 and len(reopened_store.list_runtime_pools()) == 1, "round robin cursor survives reopen")
    var after_reopen = select_runtime(persisted_pool, "case")
    _check(after_reopen.runtime.id == "target-a", "selection from reopened pool")
    var unknown_runtime = False
    try:
        _ = select_runtime(persisted_pool, "case", "manual", manual_runtime_id="missing")
    except err:
        unknown_runtime = String(err).find("unknown_runtime") >= 0
    _check(unknown_runtime, "unknown manual runtime error")
    var empty_pool_error = False
    try:
        _ = select_runtime(RuntimePool("empty", "[]", "[\"case\"]", "{}"), "case")
    except err:
        empty_pool_error = String(err).find("no_runtime_targets") >= 0
    _check(empty_pool_error, "empty pool error")
    var unknown_pool_error = False
    try:
        _ = reopened_store.get_runtime_pool("missing-pool")
    except err:
        unknown_pool_error = String(err).find("runtime pool not found") >= 0
    _check(unknown_pool_error, "unknown pool error")
    reopened_store.close()
    remove(runtime_path)
    var malformed_pool_runtimes = RuntimePool("bad-runtimes", "not-json", "[\"case\"]", "{}")
    var pool_write_failed = False
    try:
        store.put_runtime_pool(malformed_pool_runtimes)
    except err:
        pool_write_failed = String(err).find("invalid runtime pool") >= 0
    _check(pool_write_failed, "malformed runtime JSON rejected at persistence boundary")
    var malformed_pool_types = RuntimePool("bad-types", "[]", "{\"case\":true}", "{}")
    pool_write_failed = False
    try:
        store.put_runtime_pool(malformed_pool_types)
    except err:
        pool_write_failed = String(err).find("invalid runtime pool") >= 0
    _check(pool_write_failed, "malformed pool impulse allow-list rejected")
    var malformed_pool_metadata = RuntimePool("bad-metadata", "[]", "[]", "[]")
    pool_write_failed = False
    try:
        store.put_runtime_pool(malformed_pool_metadata)
    except err:
        pool_write_failed = String(err).find("invalid runtime pool") >= 0
    _check(pool_write_failed, "malformed pool metadata rejected")
    var malformed_pool_metadata_json = RuntimePool("bad-metadata-json", "[]", "[]", "not-json")
    pool_write_failed = False
    try:
        store.put_runtime_pool(malformed_pool_metadata_json)
    except err:
        pool_write_failed = String(err).find("invalid runtime pool") >= 0
    _check(pool_write_failed, "malformed pool metadata JSON rejected")
    _check(len(store.list_runtime_pools()) == 1, "malformed pools are not durable")
    var malformed_policy_types = DelegationPolicy("bad-policy-types", "pool", "{\"case\":true}", RuntimeBudget(), "{}")
    var policy_write_failed = False
    try:
        store.put_delegation_policy(malformed_policy_types)
    except err:
        policy_write_failed = String(err).find("invalid delegation policy") >= 0
    _check(policy_write_failed, "malformed policy impulse allow-list rejected")
    var malformed_policy_json = DelegationPolicy("bad-policy-json", "pool", "not-json", RuntimeBudget(), "{}")
    policy_write_failed = False
    try:
        store.put_delegation_policy(malformed_policy_json)
    except err:
        policy_write_failed = String(err).find("invalid delegation policy") >= 0
    _check(policy_write_failed, "malformed policy JSON rejected")
    var malformed_policy_metadata = DelegationPolicy("bad-policy-metadata", "pool", "[]", RuntimeBudget(), "[]")
    policy_write_failed = False
    try:
        store.put_delegation_policy(malformed_policy_metadata)
    except err:
        policy_write_failed = String(err).find("invalid delegation policy") >= 0
    _check(policy_write_failed, "malformed policy metadata rejected")
    _check(len(store.list_delegation_policies()) == 1, "malformed policies are not durable")
    _expect_error("[{\"uri\":\"missing-id\"}]", "missing_field")
    _expect_error("[{\"id\":\"a\",\"unknown\":true}]", "unknown_field")
    print("runtime policy smoke ok")
