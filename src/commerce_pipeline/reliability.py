from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Mapping


TABLE_PRIMARY_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "shipments": "shipment_id",
}

TABLE_FIELDS = {
    "customers": frozenset(
        {
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "customer_status",
            "city",
            "state_code",
            "created_at",
            "updated_at",
        }
    ),
    "products": frozenset(
        {
            "product_id",
            "sku",
            "product_name",
            "category",
            "unit_price",
            "active",
            "created_at",
            "updated_at",
        }
    ),
    "orders": frozenset(
        {
            "order_id",
            "customer_id",
            "order_status",
            "order_total",
            "currency",
            "ordered_at",
            "updated_at",
        }
    ),
    "order_items": frozenset(
        {
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "line_total",
        }
    ),
    "payments": frozenset(
        {
            "payment_id",
            "order_id",
            "payment_status",
            "payment_method",
            "amount",
            "paid_at",
            "created_at",
            "updated_at",
        }
    ),
    "shipments": frozenset(
        {
            "shipment_id",
            "order_id",
            "shipment_status",
            "tracking_code",
            "carrier",
            "shipped_at",
            "delivered_at",
            "created_at",
            "updated_at",
        }
    ),
}


@dataclass(frozen=True)
class SchemaAssessment:
    compatible: bool
    change_type: str
    reason: str | None
    source_table: str
    missing_fields: tuple[str, ...]
    extra_fields: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EventOrderAssessment:
    status: str
    event_delay_seconds: float
    is_out_of_order: bool
    is_late: bool
    requires_backfill: bool

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReconciliationAssessment:
    source_count: int
    target_count: int
    difference: int
    missing_records: int
    unexpected_records: int
    reconciled: bool

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def assess_record_schema(
    source_table: str,
    record: Mapping[str, Any] | object,
) -> SchemaAssessment:
    expected_fields = TABLE_FIELDS.get(source_table)
    primary_key = TABLE_PRIMARY_KEYS.get(source_table)

    if expected_fields is None or primary_key is None:
        return SchemaAssessment(
            compatible=False,
            change_type="incompatible",
            reason="unexpected_source_table",
            source_table=source_table,
            missing_fields=(),
            extra_fields=(),
        )

    if not isinstance(record, Mapping):
        return SchemaAssessment(
            compatible=False,
            change_type="incompatible",
            reason="record_not_object",
            source_table=source_table,
            missing_fields=tuple(sorted(expected_fields)),
            extra_fields=(),
        )

    actual_fields = frozenset(str(field) for field in record)
    missing_fields = tuple(sorted(expected_fields - actual_fields))
    extra_fields = tuple(sorted(actual_fields - expected_fields))

    if primary_key not in actual_fields:
        return SchemaAssessment(
            compatible=False,
            change_type="incompatible",
            reason="missing_primary_key",
            source_table=source_table,
            missing_fields=missing_fields,
            extra_fields=extra_fields,
        )

    primary_key_value = record[primary_key]

    if type(primary_key_value) is not int:
        return SchemaAssessment(
            compatible=False,
            change_type="incompatible",
            reason="incompatible_primary_key_type",
            source_table=source_table,
            missing_fields=missing_fields,
            extra_fields=extra_fields,
        )

    if missing_fields:
        return SchemaAssessment(
            compatible=False,
            change_type="incompatible",
            reason="missing_expected_fields",
            source_table=source_table,
            missing_fields=missing_fields,
            extra_fields=extra_fields,
        )

    if extra_fields:
        return SchemaAssessment(
            compatible=True,
            change_type="additive",
            reason=None,
            source_table=source_table,
            missing_fields=(),
            extra_fields=extra_fields,
        )

    return SchemaAssessment(
        compatible=True,
        change_type="unchanged",
        reason=None,
        source_table=source_table,
        missing_fields=(),
        extra_fields=(),
    )


def _utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime values must include timezone information")

    return value.astimezone(timezone.utc)


def assess_event_order(
    event_timestamp: datetime,
    high_watermark: datetime,
    *,
    allowed_lateness_seconds: float = 300.0,
) -> EventOrderAssessment:
    if allowed_lateness_seconds < 0:
        raise ValueError("allowed lateness cannot be negative")

    event_utc = _utc_datetime(event_timestamp)
    watermark_utc = _utc_datetime(high_watermark)

    delay_seconds = max(
        (watermark_utc - event_utc).total_seconds(),
        0.0,
    )
    is_out_of_order = event_utc < watermark_utc
    is_late = delay_seconds > allowed_lateness_seconds

    if is_late:
        status = "late"
    elif is_out_of_order:
        status = "out_of_order"
    else:
        status = "on_time"

    return EventOrderAssessment(
        status=status,
        event_delay_seconds=round(delay_seconds, 3),
        is_out_of_order=is_out_of_order,
        is_late=is_late,
        requires_backfill=is_late,
    )


def assess_count_reconciliation(
    source_count: int,
    target_count: int,
) -> ReconciliationAssessment:
    if source_count < 0 or target_count < 0:
        raise ValueError("record counts cannot be negative")

    difference = target_count - source_count

    return ReconciliationAssessment(
        source_count=source_count,
        target_count=target_count,
        difference=difference,
        missing_records=max(-difference, 0),
        unexpected_records=max(difference, 0),
        reconciled=difference == 0,
    )


def freshness_seconds(
    observed_at: datetime,
    *,
    checked_at: datetime | None = None,
) -> float:
    observed_utc = _utc_datetime(observed_at)
    checked_utc = _utc_datetime(
        checked_at or datetime.now(timezone.utc)
    )

    return round(
        max((checked_utc - observed_utc).total_seconds(), 0.0),
        3,
    )


def recovery_seconds(
    failed_at: datetime,
    recovered_at: datetime,
) -> float:
    failed_utc = _utc_datetime(failed_at)
    recovered_utc = _utc_datetime(recovered_at)

    if recovered_utc < failed_utc:
        raise ValueError("recovery cannot occur before failure")

    return round(
        (recovered_utc - failed_utc).total_seconds(),
        3,
    )
