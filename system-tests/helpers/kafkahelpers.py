from confluent_kafka import Producer, Consumer, TopicPartition
from typing import Optional
import uuid
from streaming_data_types.run_start_pl72 import serialise_pl72
from streaming_data_types.run_stop_6s4t import serialise_6s4t
from streaming_data_types.logdata_f142 import serialise_f142


def create_producer():
    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "message.max.bytes": "20000000",
    }
    producer = Producer(**producer_config)
    return producer


def publish_run_start_message(
    producer: Producer,
    nexus_structure_filepath: str,
    nexus_filename: str,
    topic: str = "TEST_writerCommand",
    start_time: Optional[int] = None,
    stop_time: Optional[int] = None,
    job_id: Optional[str] = str(uuid.uuid4()),
    service_id: Optional[str] = None,
) -> str:
    with open(nexus_structure_filepath, "r") as nexus_structure_file:
        nexus_structure = nexus_structure_file.read().replace("\n", "")

    runstart_message = serialise_pl72(
        job_id,
        nexus_filename,
        start_time,
        stop_time,
        nexus_structure=nexus_structure,
        service_id=service_id,
    )
    producer.produce(topic, bytes(runstart_message))
    producer.flush()
    return job_id


def publish_run_stop_message(
    producer: Producer,
    job_id: str,
    topic: str = "TEST_writerCommand",
    stop_time: Optional[int] = None,
    service_id: Optional[str] = None,
) -> str:
    runstop_message = serialise_6s4t(job_id, service_id=service_id, stop_time=stop_time)

    producer.produce(topic, bytes(runstop_message))
    producer.flush()
    return job_id


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


def _millseconds_to_nanoseconds(time_ms: int) -> int:
    return int(time_ms * 1000000)


def publish_f142_message(producer: Producer, topic: str, kafka_timestamp: Optional[int] = None,
                         source_name: Optional[str] = None, alarm_status: Optional[int] = None,
                         alarm_severity: Optional[int] = None):
    """
    Publish an f142 message to a given topic.
    Optionally set the timestamp in the kafka header to allow, for example, fake "historical" data.
    :param producer: Producer to publish the message with
    :param topic: Name of topic to publish to
    :param kafka_timestamp: Timestamp to set in the Kafka header (milliseconds after unix epoch)
    :param source_name: Name of the source in the f142 message
    :param alarm_status: EPICS alarm status, use enum-like class from streaming_data_types.fbschemas.logdata_f142.AlarmStatus
    :param alarm_severity: EPICS alarm severity, use enum-like class from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity
    """
    if source_name is None:
        source_name = "fw-test-helpers"
    value = 42
    f142_message = serialise_f142(value, source_name, _millseconds_to_nanoseconds(kafka_timestamp),
                                  alarm_status, alarm_severity)
    producer.produce(topic, f142_message, timestamp=kafka_timestamp)
    producer.poll(0)
