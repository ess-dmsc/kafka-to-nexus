import uuid
from datetime import datetime
from typing import Optional, Union, Any

from confluent_kafka import Consumer, Producer

from streaming_data_types.epics_connection_info_ep00 import serialise_ep00
import streaming_data_types.logdata_f142
from streaming_data_types.epics_pv_scalar_data_scal import serialise_scal
from streaming_data_types.epics_pv_conn_status_pvCn import (
    serialise_pvCn,
    ConnectionInfo,
)
from streaming_data_types.epics_pv_alarm_status_pvAl import (
    serialise_pvAl,
    AlarmState,
    AlarmSeverity,
    CAAlarmState,
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


def publish_scal_message(
    producer: Producer,
    topic: str,
    timestamp: datetime,
    value: Union[float, np.ndarray] = 42,
    source_name: str = "fw-test-helpers",
):
    message = serialise_scal(
        value=value,
        source_name=source_name,
        timestamp=timestamp,
    )
    publish_message(producer, message, topic, timestamp)


def publish_pvCn_message(
    producer: Producer,
    topic: str,
    timestamp: datetime,
    status: ConnectionInfo,
    source_name: str = "fw-test-helpers",
):
    message = serialise_pvCn(
        status=status,
        source_name=source_name,
        timestamp=timestamp,
    )
    publish_message(producer, message, topic, timestamp)


def publish_pvAl_message(
    producer: Producer,
    topic: str,
    timestamp: datetime,
    state: AlarmState,
    severity: AlarmSeverity,
    source_name: str = "fw-test-helpers",
):
    message = serialise_pvAl(
        ca_state=CAAlarmState.NO_ALARM,
        state=state,
        severity=severity,
        source_name=source_name,
        timestamp=timestamp,
    )
    publish_message(producer, message, topic, timestamp)


def publish_ep00_message(
    producer, topic, status, timestamp: datetime, source_name: str = "SIMPLE:DOUBLE"
):
    ep00_message = serialise_ep00(
        datetime_to_ns(timestamp), status, source_name=source_name
    )
    publish_message(producer, ep00_message, topic, timestamp)
