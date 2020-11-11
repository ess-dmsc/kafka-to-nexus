from confluent_kafka import Producer, Consumer, TopicPartition
from typing import Optional
import uuid
from streaming_data_types.logdata_f142 import serialise_f142
from streaming_data_types.epics_connection_info_ep00 import serialise_ep00
from datetime import datetime


def create_producer() -> Producer:
    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "message.max.bytes": "20000000",
    }
    producer = Producer(**producer_config)
    return producer


def create_consumer():
    return Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": uuid.uuid4(),
            "default.topic.config": {"auto.offset.reset": "latest"},
        }
    )


def consume_everything(topic):
    consumer = Consumer(
        {"bootstrap.servers": "localhost:9092", "group.id": uuid.uuid4()}
    )
    topicpart = TopicPartition(topic, 0, 0)
    consumer.assign([topicpart])
    low, high = consumer.get_watermark_offsets(topicpart)

    return consumer.consume(high - 1)


def datetime_to_ms(time: datetime) -> int:
    return int(time.timestamp() * 1000)


def datetime_to_ns(time: datetime):
    return int(time.timestamp() * 1e9)


def publish_f142_message(
    producer: Producer,
    topic: str,
    timestamp: datetime,
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
    value = 42
    f142_message = serialise_f142(
        value,
        source_name,
        datetime_to_ns(timestamp),
        alarm_status,
        alarm_severity,
    )
    producer.produce(topic, f142_message, timestamp=datetime_to_ms(timestamp))
    producer.poll(0)
    producer.flush()


def publish_ep00_message(
    producer, topic, status, timestamp: datetime, source_name: Optional[str] = None
):
    if source_name is None:
        source_name = "SIMPLE:DOUBLE"
    ep00_message = serialise_ep00(datetime_to_ns(timestamp), status, source_name)
    producer.produce(topic, ep00_message, timestamp=datetime_to_ms(timestamp))
    producer.poll(0)
    producer.flush()
