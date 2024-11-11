import json
import time
import uuid

from confluent_kafka import OFFSET_END, Consumer, TopicPartition
from streaming_data_types import deserialise_x5f2

from conftest import BROKERS


def create_consumer(topic):
    consumer_conf = {
        "bootstrap.servers": ",".join(BROKERS),
        "group.id": uuid.uuid4(),
        "auto.offset.reset": "latest",
    }
    consumer = Consumer(consumer_conf)
    topic_partitions = []

    metadata = consumer.list_topics(topic)
    partition_numbers = [p.id for p in metadata.topics[topic].partitions.values()]

    for pn in partition_numbers:
        partition = TopicPartition(topic, pn)
        # Make sure consumer is at end of the partition(s)
        partition.offset = OFFSET_END
        topic_partitions.append(partition)

    consumer.assign(topic_partitions)
    return consumer, topic_partitions


class TestFileWriter:
    def test_idle_status_messages_sent(self, file_writer):
        consumer, topic_partitions = create_consumer("local_filewriter_status")

        messages = []
        start_time = time.time()
        while time.time() < start_time + 12:
            msg = consumer.poll(0.005)
            if msg:
                messages.append(msg)

        buffer = messages[-1].value()
        value = deserialise_x5f2(buffer)
        state = json.loads(value.status_json)["state"]

        assert len(messages) >= 2
        for msg in messages:
            assert msg.value()[4:8] == b"x5f2"
        assert state == "idle"
