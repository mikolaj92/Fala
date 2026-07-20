"""Optional multi-Fala bridge outbox/inbox (not Essential Fala).

Default composition remains separate journals + subprocess handoff.
"""

from std.collections import List
from fala.sqlite import SQLiteError
from fala.domain import BridgeDelivery
from fala.domain_store import NativeDomainStore, BridgeEnqueueResult


def put_bridge_delivery(mut store: NativeDomainStore, row: BridgeDelivery) raises SQLiteError -> BridgeDelivery:
    return store.put_bridge_delivery(row)


def enqueue_bridge_delivery(
    mut store: NativeDomainStore,
    row: BridgeDelivery,
    idempotency_key: String = "",
    actor: String = "",
    correlation_id: String = "",
    causation_id: String = "",
) raises SQLiteError -> BridgeEnqueueResult:
    return store.enqueue_bridge_delivery(row, idempotency_key, actor, correlation_id, causation_id)


def list_bridge_deliveries(mut store: NativeDomainStore, run_id: String) raises SQLiteError -> List[String]:
    return store.list_bridge_deliveries(run_id)


def put_inbox_delivery(mut store: NativeDomainStore, row: BridgeDelivery) raises SQLiteError -> BridgeDelivery:
    return store.put_inbox_delivery(row)


def import_bridge_delivery(
    mut store: NativeDomainStore, row: BridgeDelivery, idempotency_key: String = ""
) raises SQLiteError -> BridgeDelivery:
    return store.import_bridge_delivery(row, idempotency_key)


def import_inbox_delivery(mut store: NativeDomainStore, row: BridgeDelivery) raises SQLiteError -> BridgeDelivery:
    return store.import_inbox_delivery(row)


def list_bridge_inbox(
    mut store: NativeDomainStore, run_id: String, status: String = "", limit: Int = -1
) raises SQLiteError -> List[String]:
    return store.list_bridge_inbox(run_id, status, limit)


def get_outbox_delivery(
    mut store: NativeDomainStore, run_id: String, delivery_id: String
) raises SQLiteError -> BridgeDelivery:
    return store.get_outbox_delivery(run_id, delivery_id)


def get_inbox_delivery(
    mut store: NativeDomainStore, run_id: String, delivery_id: String
) raises SQLiteError -> BridgeDelivery:
    return store.get_inbox_delivery(run_id, delivery_id)


def list_bridge_records(
    mut store: NativeDomainStore, table: String, run_id: String, status: String = "", limit: Int = -1
) raises SQLiteError -> List[BridgeDelivery]:
    return store.list_bridge_records(table, run_id, status, limit)


def list_outbox_records(
    mut store: NativeDomainStore, run_id: String, status: String = "", limit: Int = -1
) raises SQLiteError -> List[BridgeDelivery]:
    return store.list_outbox_records(run_id, status, limit)


def list_inbox_records(
    mut store: NativeDomainStore, run_id: String, status: String = "", limit: Int = -1
) raises SQLiteError -> List[BridgeDelivery]:
    return store.list_inbox_records(run_id, status, limit)


def transition_bridge_delivery(
    mut store: NativeDomainStore,
    table: String,
    run_id: String,
    delivery_id: String,
    to_status: String,
    updated_at: String,
    idempotency_key: String = "",
) raises SQLiteError -> BridgeDelivery:
    return store.transition_bridge_delivery(table, run_id, delivery_id, to_status, updated_at, idempotency_key)


def claim_bridge_delivery(
    mut store: NativeDomainStore,
    table: String,
    run_id: String,
    delivery_id: String,
    updated_at: String,
    idempotency_key: String = "",
) raises SQLiteError -> BridgeDelivery:
    return store.claim_bridge_delivery(table, run_id, delivery_id, updated_at, idempotency_key)


def deliver_bridge_delivery(
    mut store: NativeDomainStore,
    table: String,
    run_id: String,
    delivery_id: String,
    updated_at: String,
    idempotency_key: String = "",
) raises SQLiteError -> BridgeDelivery:
    return store.deliver_bridge_delivery(table, run_id, delivery_id, updated_at, idempotency_key)


def retry_bridge_delivery(
    mut store: NativeDomainStore,
    table: String,
    run_id: String,
    delivery_id: String,
    updated_at: String,
    idempotency_key: String = "",
) raises SQLiteError -> BridgeDelivery:
    return store.retry_bridge_delivery(table, run_id, delivery_id, updated_at, idempotency_key)
