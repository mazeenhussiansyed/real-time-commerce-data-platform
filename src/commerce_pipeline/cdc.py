from __future__ import annotations

import os
import time
from collections.abc import Mapping

from confluent_kafka import Consumer, TopicPartition


TABLE_TOPICS = {
    "customers": "commerce.commerce.customers",
    "products": "commerce.commerce.products",
    "orders": "commerce.commerce.orders",
    "order_items": "commerce.commerce.order_items",
    "payments": "commerce.commerce.payments",
    "shipments": "commerce.commerce.shipments",
}


def kafka_bootstrap_servers() -> str:
    return os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "127.0.0.1:29092",
    )


def create_consumer(group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": kafka_bootstrap_servers(),
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )


def topic_partitions(
    consumer: Consumer,
    topic: str,
) -> list[int]:
    metadata = consumer.list_topics(
        topic=topic,
        timeout=10,
    )
    topic_metadata = metadata.topics.get(topic)

    if topic_metadata is None:
        raise RuntimeError(f"Kafka topic does not exist: {topic}")

    if topic_metadata.error is not None:
        raise RuntimeError(
            f"Kafka topic metadata failed for {topic}: "
            f"{topic_metadata.error}"
        )

    return sorted(topic_metadata.partitions)


def topic_event_count(
    consumer: Consumer,
    topic: str,
) -> int:
    total = 0

    for partition in topic_partitions(consumer, topic):
        low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, partition),
            timeout=10,
            cached=False,
        )
        total += high - low

    return total


def topic_event_counts(
    consumer: Consumer,
) -> dict[str, int]:
    return {
        table: topic_event_count(consumer, topic)
        for table, topic in TABLE_TOPICS.items()
    }


def topic_tail_positions(
    consumer: Consumer,
    topic: str,
) -> list[TopicPartition]:
    positions: list[TopicPartition] = []

    for partition in topic_partitions(consumer, topic):
        _, high = consumer.get_watermark_offsets(
            TopicPartition(topic, partition),
            timeout=10,
            cached=False,
        )
        positions.append(
            TopicPartition(topic, partition, high)
        )

    return positions


def wait_for_minimum_counts(
    consumer: Consumer,
    expected_counts: Mapping[str, int],
    *,
    timeout: float = 120.0,
) -> dict[str, int]:
    deadline = time.monotonic() + timeout
    last_counts: dict[str, int] = {}

    while time.monotonic() < deadline:
        last_counts = topic_event_counts(consumer)

        complete = all(
            last_counts.get(table, 0) >= expected
            for table, expected in expected_counts.items()
        )
        if complete:
            return last_counts

        time.sleep(2)

    raise RuntimeError(
        "Kafka topics did not reach the expected minimum counts: "
        f"expected={dict(expected_counts)}, actual={last_counts}"
    )