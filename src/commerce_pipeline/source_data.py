from __future__ import annotations

import hashlib
import json
import random
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any


BASE_TIME = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)

FIRST_NAMES = (
    "Ava",
    "Liam",
    "Emma",
    "Noah",
    "Mia",
    "Ethan",
    "Sophia",
    "Lucas",
    "Olivia",
    "Mason",
)

LAST_NAMES = (
    "Anderson",
    "Brown",
    "Clark",
    "Davis",
    "Garcia",
    "Harris",
    "Jackson",
    "Martin",
    "Patel",
    "Wilson",
)

LOCATIONS = (
    ("New York", "NY"),
    ("Jersey City", "NJ"),
    ("Philadelphia", "PA"),
    ("Boston", "MA"),
    ("Chicago", "IL"),
    ("Austin", "TX"),
    ("Seattle", "WA"),
    ("Atlanta", "GA"),
)

CATEGORIES = (
    "electronics",
    "home",
    "office",
    "fitness",
    "outdoors",
    "personal_care",
)

PRODUCT_WORDS = (
    "Adapter",
    "Bottle",
    "Cable",
    "Desk",
    "Headphones",
    "Keyboard",
    "Lamp",
    "Monitor",
    "Notebook",
    "Speaker",
)

ORDER_STATUSES = (
    "created",
    "confirmed",
    "processing",
    "shipped",
    "delivered",
    "cancelled",
)

PAYMENT_METHODS = (
    "credit_card",
    "debit_card",
    "digital_wallet",
    "bank_transfer",
)

CARRIERS = (
    "UPS",
    "FedEx",
    "USPS",
)


@dataclass(frozen=True)
class SourceDataConfig:
    customer_count: int = 1_000
    product_count: int = 250
    order_count: int = 5_000
    seed: int = 20260902

    def __post_init__(self) -> None:
        if self.customer_count <= 0:
            raise ValueError("customer_count must be positive")
        if self.product_count < 4:
            raise ValueError("product_count must be at least 4")
        if self.order_count <= 0:
            raise ValueError("order_count must be positive")


@dataclass(frozen=True)
class CommerceDataset:
    config: SourceDataConfig
    customers: list[dict[str, Any]]
    products: list[dict[str, Any]]
    orders: list[dict[str, Any]]
    order_items: list[dict[str, Any]]
    payments: list[dict[str, Any]]
    shipments: list[dict[str, Any]]


def money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def _customer_status(customer_id: int) -> str:
    if customer_id % 50 == 0:
        return "suspended"
    if customer_id % 20 == 0:
        return "inactive"
    return "active"


def _payment_status(order_status: str) -> str:
    mapping = {
        "created": "pending",
        "confirmed": "authorized",
        "processing": "captured",
        "shipped": "captured",
        "delivered": "captured",
        "cancelled": "refunded",
    }
    return mapping[order_status]


def generate_dataset(
    config: SourceDataConfig | None = None,
) -> CommerceDataset:
    selected_config = config or SourceDataConfig()
    randomizer = random.Random(selected_config.seed)

    customers: list[dict[str, Any]] = []
    for customer_id in range(1, selected_config.customer_count + 1):
        first_name = FIRST_NAMES[(customer_id - 1) % len(FIRST_NAMES)]
        last_name = LAST_NAMES[
            ((customer_id - 1) // len(FIRST_NAMES)) % len(LAST_NAMES)
        ]
        city, state_code = randomizer.choice(LOCATIONS)
        created_at = BASE_TIME - timedelta(days=customer_id % 365)

        customers.append(
            {
                "customer_id": customer_id,
                "email": (
                    f"{first_name.lower()}.{last_name.lower()}."
                    f"{customer_id}@example.test"
                ),
                "first_name": first_name,
                "last_name": last_name,
                "customer_status": _customer_status(customer_id),
                "city": city,
                "state_code": state_code,
                "created_at": created_at,
                "updated_at": created_at,
            }
        )

    products: list[dict[str, Any]] = []
    product_prices: dict[int, Decimal] = {}

    for product_id in range(1, selected_config.product_count + 1):
        category = CATEGORIES[(product_id - 1) % len(CATEGORIES)]
        product_word = PRODUCT_WORDS[
            (product_id - 1) % len(PRODUCT_WORDS)
        ]
        unit_price = money(
            Decimal(randomizer.randrange(500, 30_000)) / Decimal("100")
        )
        created_at = BASE_TIME - timedelta(days=product_id % 180)

        product_prices[product_id] = unit_price
        products.append(
            {
                "product_id": product_id,
                "sku": f"SKU-{product_id:06d}",
                "product_name": f"{category.title()} {product_word} {product_id}",
                "category": category,
                "unit_price": unit_price,
                "active": product_id % 25 != 0,
                "created_at": created_at,
                "updated_at": created_at,
            }
        )

    orders: list[dict[str, Any]] = []
    order_items: list[dict[str, Any]] = []
    payments: list[dict[str, Any]] = []
    shipments: list[dict[str, Any]] = []

    order_item_id = 1
    shipment_id = 1

    for order_id in range(1, selected_config.order_count + 1):
        customer_id = randomizer.randint(
            1,
            selected_config.customer_count,
        )
        order_status = ORDER_STATUSES[
            (order_id - 1) % len(ORDER_STATUSES)
        ]
        ordered_at = BASE_TIME + timedelta(minutes=order_id * 5)

        item_count = 1 + ((order_id - 1) % 4)
        selected_products = randomizer.sample(
            range(1, selected_config.product_count + 1),
            item_count,
        )

        current_items: list[dict[str, Any]] = []
        order_total = Decimal("0.00")

        for product_id in selected_products:
            quantity = randomizer.randint(1, 4)
            unit_price = product_prices[product_id]
            order_total += unit_price * quantity

            current_items.append(
                {
                    "order_item_id": order_item_id,
                    "order_id": order_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                }
            )
            order_item_id += 1

        order_total = money(order_total)
        order_items.extend(current_items)

        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_status": order_status,
                "currency": "USD",
                "order_total": order_total,
                "ordered_at": ordered_at,
                "updated_at": ordered_at,
            }
        )

        payment_status = _payment_status(order_status)
        if payment_status in {"captured", "refunded"}:
            paid_at = ordered_at + timedelta(minutes=2)
        else:
            paid_at = None

        payments.append(
            {
                "payment_id": order_id,
                "order_id": order_id,
                "payment_status": payment_status,
                "payment_method": randomizer.choice(PAYMENT_METHODS),
                "amount": order_total,
                "paid_at": paid_at,
                "created_at": ordered_at,
                "updated_at": ordered_at,
            }
        )

        if order_status in {"processing", "shipped", "delivered"}:
            if order_status == "processing":
                shipment_status = "packed"
                shipped_at = None
                delivered_at = None
            elif order_status == "shipped":
                shipment_status = "shipped"
                shipped_at = ordered_at + timedelta(days=1)
                delivered_at = None
            else:
                shipment_status = "delivered"
                shipped_at = ordered_at + timedelta(days=1)
                delivered_at = ordered_at + timedelta(days=4)

            shipments.append(
                {
                    "shipment_id": shipment_id,
                    "order_id": order_id,
                    "shipment_status": shipment_status,
                    "carrier": randomizer.choice(CARRIERS),
                    "tracking_code": f"TRK-{shipment_id:012d}",
                    "shipped_at": shipped_at,
                    "delivered_at": delivered_at,
                    "created_at": ordered_at + timedelta(hours=4),
                    "updated_at": delivered_at or shipped_at or ordered_at,
                }
            )
            shipment_id += 1

    return CommerceDataset(
        config=selected_config,
        customers=customers,
        products=products,
        orders=orders,
        order_items=order_items,
        payments=payments,
        shipments=shipments,
    )


def validate_dataset(dataset: CommerceDataset) -> list[str]:
    errors: list[str] = []

    customer_ids = {
        int(row["customer_id"])
        for row in dataset.customers
    }
    product_ids = {
        int(row["product_id"])
        for row in dataset.products
    }
    order_ids = {
        int(row["order_id"])
        for row in dataset.orders
    }

    if len(customer_ids) != len(dataset.customers):
        errors.append("duplicate customer IDs")

    if len(product_ids) != len(dataset.products):
        errors.append("duplicate product IDs")

    if len(order_ids) != len(dataset.orders):
        errors.append("duplicate order IDs")

    for order in dataset.orders:
        if int(order["customer_id"]) not in customer_ids:
            errors.append(
                f"order {order['order_id']} references an unknown customer"
            )

    item_totals: dict[int, Decimal] = defaultdict(
        lambda: Decimal("0.00")
    )
    order_product_pairs: set[tuple[int, int]] = set()

    for item in dataset.order_items:
        order_id = int(item["order_id"])
        product_id = int(item["product_id"])
        pair = (order_id, product_id)

        if order_id not in order_ids:
            errors.append(
                f"order item {item['order_item_id']} references an unknown order"
            )

        if product_id not in product_ids:
            errors.append(
                f"order item {item['order_item_id']} references an unknown product"
            )

        if pair in order_product_pairs:
            errors.append(
                f"order {order_id} contains duplicate product {product_id}"
            )
        order_product_pairs.add(pair)

        item_totals[order_id] += (
            Decimal(str(item["unit_price"]))
            * int(item["quantity"])
        )

    orders_by_id = {
        int(row["order_id"]): row
        for row in dataset.orders
    }

    for order_id, order in orders_by_id.items():
        expected_total = money(item_totals[order_id])
        actual_total = money(Decimal(str(order["order_total"])))

        if expected_total != actual_total:
            errors.append(
                f"order {order_id} total does not match its items"
            )

    payment_order_ids: set[int] = set()

    for payment in dataset.payments:
        order_id = int(payment["order_id"])

        if order_id not in order_ids:
            errors.append(
                f"payment {payment['payment_id']} references an unknown order"
            )

        if order_id in payment_order_ids:
            errors.append(f"order {order_id} has duplicate payments")
        payment_order_ids.add(order_id)

        if money(Decimal(str(payment["amount"]))) != money(
            Decimal(str(orders_by_id[order_id]["order_total"]))
        ):
            errors.append(
                f"payment {payment['payment_id']} does not match its order total"
            )

    for shipment in dataset.shipments:
        if int(shipment["order_id"]) not in order_ids:
            errors.append(
                f"shipment {shipment['shipment_id']} references an unknown order"
            )

    return errors


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    raise TypeError(f"unsupported fingerprint value: {type(value).__name__}")


def dataset_fingerprint(dataset: CommerceDataset) -> str:
    serialized = json.dumps(
        asdict(dataset),
        default=_json_default,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def summarize_dataset(dataset: CommerceDataset) -> dict[str, object]:
    order_statuses = Counter(
        str(row["order_status"])
        for row in dataset.orders
    )
    payment_statuses = Counter(
        str(row["payment_status"])
        for row in dataset.payments
    )
    shipment_statuses = Counter(
        str(row["shipment_status"])
        for row in dataset.shipments
    )

    total_order_value = sum(
        (
            Decimal(str(row["order_total"]))
            for row in dataset.orders
        ),
        Decimal("0.00"),
    )

    return {
        "seed": dataset.config.seed,
        "customers": len(dataset.customers),
        "products": len(dataset.products),
        "orders": len(dataset.orders),
        "order_items": len(dataset.order_items),
        "payments": len(dataset.payments),
        "shipments": len(dataset.shipments),
        "total_order_value": format(money(total_order_value), "f"),
        "order_statuses": dict(sorted(order_statuses.items())),
        "payment_statuses": dict(sorted(payment_statuses.items())),
        "shipment_statuses": dict(sorted(shipment_statuses.items())),
        "fingerprint": dataset_fingerprint(dataset),
    }