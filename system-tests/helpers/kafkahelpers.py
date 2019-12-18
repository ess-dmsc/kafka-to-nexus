from confluent_kafka import Producer, Consumer, TopicPartition
from .flatbufferhelpers import create_f142_message
import uuid
import json


def create_producer():
    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "message.max.bytes": "20000000",
    }
    producer = Producer(**producer_config)
    return producer


def send_writer_command(
    filepath, producer, topic="TEST_writerCommand", start_time=None, stop_time=None, job_id=None, service_id=None, filename=None
):
    with open(filepath, "r") as cmd_file:
        data_str = cmd_file.read().replace("\n", "")
        data = json.loads(data_str)
        if start_time is not None:
            data["start_time"] = start_time
        if stop_time is not None:
            data["stop_time"] = stop_time
        if job_id is not None:
            data["job_id"] = job_id
        if service_id is not None:
            data["service_id"] = service_id
        if filename is not None:
            data["file_attributes"]["file_name"] = filename

    producer.produce(topic, json.dumps(data))
    producer.flush()
    return data["job_id"]


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


def publish_f142_message(producer, topic, kafka_timestamp=None):
    """
    Publish an f142 message to a given topic.
    Optionally set the timestamp in the kafka header to allow, for example, fake "historical" data.
    :param topic: Name of topic to publish to
    :param kafka_timestamp: Timestamp to set in the Kafka header (milliseconds after unix epoch)
    """
    f142_message = create_f142_message(kafka_timestamp)
    producer.produce(topic, f142_message, timestamp=kafka_timestamp)
    producer.poll(0)
