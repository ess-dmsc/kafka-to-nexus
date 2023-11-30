import uuid
from datetime import datetime
from typing import Optional, Union, Any

from confluent_kafka import Consumer, Producer
from streaming_data_types import serialise_ev44

import streaming_data_types.logdata_f142
from streaming_data_types.logdata_f144 import serialise_f144
from streaming_data_types.epics_connection_ep01 import (
    serialise_ep01,
    ConnectionInfo,
)
from streaming_data_types.alarm_al00 import (
    serialise_al00,
    Severity,
)

try:
    from fast_f142_serialiser import f142_serialiser

    def serialise_f142(
        value: Any,
        source_name: str,
        timestamp: datetime,
        alarm_status: Union[int, None] = None,
        alarm_severity: Union[int, None] = None,
    ):
        if alarm_status is None and alarm_severity is None:
            return serialise_f142.serialiser.serialise_message(
                source_name, value, timestamp
            )
        return streaming_data_types.logdata_f142.serialise_f142(
            value, source_name, datetime_to_ns(timestamp), alarm_status, alarm_severity
        )

    serialise_f142.serialiser = f142_serialiser()
except ImportError:

    def serialise_f142(
        value: Any,
        source_name: str,
        timestamp: datetime,
        alarm_status: Union[int, None] = None,
        alarm_severity: Union[int, None] = None,
    ):
        return streaming_data_types.logdata_f142.serialise_f142(
            value, source_name, datetime_to_ns(timestamp), alarm_status, alarm_severity
        )


import numpy as np


def create_producer(kafka_address) -> Producer:
    conf = {
        "bootstrap.servers": kafka_address,
        "queue.buffering.max.messages": 1000000,
        "linger.ms": 500,
    }
    return Producer(conf)


def create_consumer(kafka_address) -> Consumer:
    conf = {
        "bootstrap.servers": kafka_address,
        "group_id": uuid.uuid4(),
        "auto.offset.reset": "latest",
    }
    return Consumer(conf)


def datetime_to_ms(time: datetime) -> int:
    return int(time.timestamp() * 1000)


def datetime_to_ns(time: datetime):
    return int(time.timestamp() * 1e9)


def publish_message(
    producer: Producer, data: bytes, topic: str, timestamp: datetime, flush: bool = True
):
    delivered = False

    def callback(err, msg):
        if err is not None:
            raise RuntimeError(f"Could not deliver message: {err}")
        nonlocal delivered
        delivered = True

    producer.produce(
        topic=topic, value=data, timestamp=datetime_to_ms(timestamp), callback=callback
    )
    while not delivered:
        producer.poll(0.1)


def publish_f142_message(
    producer: Producer,
    topic: str,
    timestamp: datetime,
    value: Union[float, np.ndarray] = 42,
    source_name: str = "fw-test-helpers",
    alarm_status: Optional[int] = None,
    alarm_severity: Optional[int] = None,
    flush: bool = True,
):
    f142_message = serialise_f142(
        value,
        source_name,
        timestamp,
        alarm_status,
        alarm_severity,
    )
    publish_message(producer, f142_message, topic, timestamp, flush)


def publish_f144_message(
    producer: Producer,
    topic: str,
    timestamp: datetime,
    value: Union[float, np.ndarray] = 42,
    source_name: str = "fw-test-helpers",
):
    message = serialise_f144(
        value=value,
        source_name=source_name,
        timestamp_unix_ns=int(timestamp.timestamp() * 1e9),
    )
    publish_message(producer, message, topic, timestamp)


def publish_ev44_message(
    producer: Producer,
    topic: str,
    reference_time,
    reference_time_index,
    time_of_flight,
    pixel_id,
    timestamp: datetime,
    message_id: int = 1,
    source_name: str = "fw-test-helpers",
):
    message = serialise_ev44(
        source_name=source_name,
        message_id=message_id,
        reference_time=reference_time,
        reference_time_index=reference_time_index,
        time_of_flight=time_of_flight,
        pixel_id=pixel_id,
    )
    publish_message(producer, message, topic, timestamp)


def publish_ep01_message(
    producer: Producer,
    topic: str,
    timestamp: datetime,
    status: ConnectionInfo,
    source_name: str = "fw-test-helpers",
):
    message = serialise_ep01(
        status=status,
        source_name=source_name,
        timestamp_ns=int(timestamp.timestamp() * 1e9),
    )
    publish_message(producer, message, topic, timestamp)


def publish_al00_message(
    producer: Producer,
    topic: str,
    timestamp: datetime,
    severity: Severity,
    alarm_msg: str,
    source_name: str = "fw-test-helpers",
):
    message = serialise_al00(
        severity=severity,
        source=source_name,
        timestamp_ns=int(timestamp.timestamp() * 1e9),
        message=alarm_msg,
    )
    publish_message(producer, message, topic, timestamp)
