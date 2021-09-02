import uuid
from datetime import datetime
from typing import Optional, Union

from confluent_kafka import Consumer, Producer

from streaming_data_types.epics_connection_info_ep00 import serialise_ep00
from streaming_data_types.logdata_f142 import serialise_f142
import numpy as np


def create_producer() -> Producer:
    conf = {
        "bootstrap.servers": "localhost:9093",
    }
    return Producer(conf)


def create_consumer() -> Consumer:
    conf = {
        "bootstrap.servers": "localhost:9093",
        "group_id": uuid.uuid4(),
        "auto.offset.reset": "latest",
    }
    return Consumer(conf)


def datetime_to_ms(time: datetime) -> int:
    return int(time.timestamp() * 1000)


def datetime_to_ns(time: datetime):
    return int(time.timestamp() * 1e9)


def publish_f142_message(
    producer: Producer,
    topic: str,
    timestamp: datetime,
    value: Union[float, np.ndarray] = 42,
    source_name: Optional[str] = None,
    alarm_status: Optional[int] = None,
    alarm_severity: Optional[int] = None,
):
    """
    Publish an f142 message to a given topic.
    Optionally set the timestamp in the kafka header to allow, for example, fake "historical" data.
    :param producer: Producer to publish the message with
    :param topic: Name of topic to publish to
    :param timestamp: Timestamp of message
    :param source_name: Name of the source in the f142 message
    :param alarm_status: EPICS alarm status, use enum-like class from streaming_data_types.fbschemas.logdata_f142.AlarmStatus
    :param alarm_severity: EPICS alarm severity, use enum-like class from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity
    """
    if source_name is None:
        source_name = "fw-test-helpers"
    f142_message = serialise_f142(
        value,
        source_name,
        datetime_to_ns(timestamp),
        alarm_status,
        alarm_severity,
    )
    producer.produce(
        topic=topic, value=f142_message, timestamp=datetime_to_ms(timestamp)
    )
    producer.flush()


def publish_ep00_message(
    producer, topic, status, timestamp: datetime, source_name: Optional[str] = None
):
    if source_name is None:
        source_name = "SIMPLE:DOUBLE"
    ep00_message = serialise_ep00(datetime_to_ns(timestamp), status, source_name)
    producer.produce(
        topic=topic, value=ep00_message, timestamp=datetime_to_ms(timestamp)
    )
    producer.flush()
