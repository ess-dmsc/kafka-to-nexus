import json
import os.path
import time
import uuid
from collections import Counter

from confluent_kafka import OFFSET_END, Consumer, Producer, TopicPartition
from streaming_data_types import deserialise_x5f2
from streaming_data_types.finished_writing_wrdn import deserialise_wrdn
from streaming_data_types.run_start_pl72 import serialise_pl72
from streaming_data_types.run_stop_6s4t import serialise_6s4t
from streaming_data_types.eventdata_ev44 import serialise_ev44

from conftest import BROKERS, INST_CONTROL_TOPIC, POOL_STATUS_TOPIC, POOL_TOPIC

NEXUS_STRUCTURE = {
    "children": [
        {
            "type": "group",
            "name": "entry",
            "children": [
                {
                    "module": "dataset",
                    "config": {
                        "name": "title",
                        "values": "This in my title",
                        "type": "string",
                    },
                },
                {
                    "module": "ev44",
                    "config": {"topic": "local_detector", "source": "grace"},
                },
            ],
        }
    ]
}


def generate_ev44(time_start, tofs, det_ids):
    buffer = serialise_ev44("grace", time_start, [time_start], [0], tofs, det_ids)


def extract_state_from_status_message(buffer):
    return json.loads(deserialise_x5f2(buffer).status_json)["state"]


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
        """
        Checks that when the file-writer is not running status messages are sent
        to the default topic with the correct frequency.
        """
        consumer, topic_partitions = create_consumer(POOL_STATUS_TOPIC)

        messages = []
        start_time = time.time()
        while time.time() < start_time + 12:
            msg = consumer.poll(0.005)
            if msg:
                messages.append(msg)

        state = extract_state_from_status_message(messages[-1].value())

        # Might be more than two depending on timings
        assert len(messages) >= 2
        for msg in messages:
            assert msg.value()[4:8] == b"x5f2"
        assert state == "idle"

    def test_writing_a_blank_file_sends_correct_messages(self, file_writer):
        """
        Tests that status and command messages are sent correctly when writing a file.
        It isn't important that the file is blank but we want to keep the "message" tests
        and the "data" tests separate, so a blank file is fine.
        """
        consumer, topic_partitions = create_consumer(INST_CONTROL_TOPIC)
        time_stamp = int(time.time())
        file_name = f"{time_stamp}.nxs"
        job_id = str(uuid.uuid4())

        buffer = serialise_pl72(
            job_id=job_id,
            filename=file_name,
            start_time=time_stamp * 1000,
            run_name="test_run",
            nexus_structure=json.dumps(NEXUS_STRUCTURE),
            service_id="",
            instrument_name="",
            broker="localhost:9092",
            control_topic=INST_CONTROL_TOPIC,
            metadata="{hello}",
        )

        producer = Producer({"bootstrap.servers": ",".join(BROKERS)})
        producer.produce(POOL_TOPIC, buffer)
        producer.flush()

        # Collect messages while writing
        start_time = time.time()
        messages = []
        while time.time() < start_time + 30:
            msg = consumer.poll(0.005)
            if msg:
                messages.append(msg)

        buffer = serialise_6s4t(
            job_id=job_id,
            run_name="test_run",
            service_id="",
            stop_time=None,
            command_id=str(uuid.uuid4()),
        )

        producer.produce(INST_CONTROL_TOPIC, buffer)
        producer.flush()

        # Collect messages after stopping
        start_time = time.time()
        while time.time() < start_time + 10:
            msg = consumer.poll(0.005)
            if msg:
                messages.append(msg)

        # Go through the messages and see what we have
        message_types = []
        for msg in messages:
            message_types.append(msg.value()[4:8])
        message_counts = Counter(message_types)

        # Should echo the start command first and only once
        # Then a response from the filewriter
        assert message_types[0:2] == [b"pl72", b"answ"]
        assert message_counts[b"pl72"] == 1
        # Some number of status messages
        assert message_counts[b"x5f2"] >= 6
        # Then finish with a stop message, a response and then a writing done message
        assert message_types[-3:] == [b"6s4t", b"answ", b"wrdn"]
        assert message_counts[b"6s4t"] == 1
        assert message_counts[b"wrdn"] == 1
        assert message_counts[b"answ"] == 2
        # Check the filewriter is writing in the status messages
        assert (
            extract_state_from_status_message(
                messages[message_types.index(b"x5f2")].value()
            )
            == "writing"
        )
        # Check the filename in the wrdn matches
        assert deserialise_wrdn(messages[~0].value()).file_name.endswith(file_name)
        # Check the metadata is forwarded to the wrdn
        # Finally check file exists
        assert os.path.exists(file_name)

    def test_data_written_to_file_is_correct(self, file_writer):
        """
        Test that the data in the file is what is expected
        """
        consumer, topic_partitions = create_consumer(INST_CONTROL_TOPIC)
        time_stamp = int(time.time())
        file_name = f"{time_stamp}.nxs"
        job_id = str(uuid.uuid4())

        buffer = serialise_pl72(
            job_id=job_id,
            filename=file_name,
            start_time=time_stamp * 1000,
            run_name="test_run",
            nexus_structure=json.dumps(NEXUS_STRUCTURE),
            service_id="",
            instrument_name="",
            broker="localhost:9092",
            control_topic=INST_CONTROL_TOPIC,
            metadata="{hello}",
        )

        producer = Producer({"bootstrap.servers": ",".join(BROKERS)})
        producer.produce(POOL_TOPIC, buffer)
        producer.flush()

        # Send some data to Kafka
        start_time = time.time()
        while time.time() < start_time + 30:
            msg = consumer.poll(0.005)
            if msg:
                messages.append(msg)

        buffer = serialise_6s4t(
            job_id=job_id,
            run_name="test_run",
            service_id="",
            stop_time=None,
            command_id=str(uuid.uuid4()),
        )

        producer.produce(INST_CONTROL_TOPIC, buffer)
        producer.flush()

        # Collect messages after stopping
        start_time = time.time()
        while time.time() < start_time + 10:
            msg = consumer.poll(0.005)
            if msg:
                messages.append(msg)

        # Go through the messages and see what we have
        message_types = []
        for msg in messages:
            message_types.append(msg.value()[4:8])
        message_counts = Counter(message_types)

        # Should echo the start command first and only once
        # Then a response from the filewriter
        assert message_types[0:2] == [b"pl72", b"answ"]
        assert message_counts[b"pl72"] == 1
        # Some number of status messages
        assert message_counts[b"x5f2"] >= 6
        # Then finish with a stop message, a response and then a writing done message
        assert message_types[-3:] == [b"6s4t", b"answ", b"wrdn"]
        assert message_counts[b"6s4t"] == 1
        assert message_counts[b"wrdn"] == 1
        assert message_counts[b"answ"] == 2
        # Check the filewriter is writing in the status messages
        assert (
                extract_state_from_status_message(
                    messages[message_types.index(b"x5f2")].value()
                )
                == "writing"
        )
        # Check the filename in the wrdn matches
        assert deserialise_wrdn(messages[~0].value()).file_name.endswith(file_name)
        # Check the metadata is forwarded to the wrdn
        # Finally check file exists
        assert os.path.exists(file_name)
