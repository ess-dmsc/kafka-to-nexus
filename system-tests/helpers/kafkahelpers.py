from confluent_kafka import Producer, Consumer, TopicPartition
from .flatbufferhelpers import create_f142_message
from typing import Optional
import uuid
from streaming_data_types.run_start_pl72 import serialise_pl72
from streaming_data_types.run_stop_6s4t import serialise_6s4t


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

    runstart_message = serialise_pl72(job_id, nexus_filename, start_time, stop_time,
                                      nexus_structure=nexus_structure, service_id=service_id)
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


def publish_f142_message(producer, topic, kafka_timestamp=None, source_name=None):
    """
    Publish an f142 message to a given topic.
    Optionally set the timestamp in the kafka header to allow, for example, fake "historical" data.
    :param topic: Name of topic to publish to
    :param kafka_timestamp: Timestamp to set in the Kafka header (milliseconds after unix epoch)
    :param source_name: Name of the source in the f142 message
    """
    if source_name is not None:
        f142_message = create_f142_message(kafka_timestamp, source_name)
    else:
        f142_message = create_f142_message(kafka_timestamp)
    producer.produce(topic, f142_message, timestamp=kafka_timestamp)
    producer.poll(0)
