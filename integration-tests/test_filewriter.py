import json
import os.path
import time
import uuid
from collections import Counter
from datetime import datetime

import h5py
import numpy as np
from confluent_kafka import OFFSET_END, Consumer, Producer, TopicPartition
from streaming_data_types import deserialise_x5f2
from streaming_data_types.eventdata_ev44 import serialise_ev44
from streaming_data_types.finished_writing_wrdn import deserialise_wrdn
from streaming_data_types.logdata_f144 import serialise_f144
from streaming_data_types.run_start_pl72 import serialise_pl72
from streaming_data_types.run_stop_6s4t import serialise_6s4t

from conftest import (
    DETECTOR_TOPIC,
    INST_CONTROL_TOPIC,
    MOTION_TOPIC,
    POOL_STATUS_TOPIC,
    POOL_TOPIC,
    get_brokers,
)

NEXUS_STRUCTURE = {
    "children": [
        {
            "type": "group",
            "name": "entry",
            "children": [
                {"module": "mdat", "config": {"items": ["start_time", "end_time"]}},
                {
                    "module": "dataset",
                    "config": {
                        "name": "title",
                        "values": "This is my title",
                        "type": "string",
                    },
                },
                {
                    "name": "detector",
                    "type": "group",
                    "attributes": [
                        {"name": "NX_class", "dtype": "string", "values": "NXdata"}
                    ],
                    "children": [
                        {
                            "module": "ev44",
                            "config": {"topic": DETECTOR_TOPIC, "source": "grace"},
                        }
                    ],
                },
                {
                    "name": "motion",
                    "type": "group",
                    "attributes": [
                        {"name": "NX_class", "dtype": "string", "values": "NXlog"}
                    ],
                    "children": [
                        {
                            "module": "f144",
                            "config": {
                                "topic": MOTION_TOPIC,
                                "source": "axis",
                                "dtype": "double",
                            },
                        }
                    ],
                },
            ],
        }
    ]
}


def generate_ev44(time_start, tofs, det_ids):
    return serialise_ev44("grace", time_start, [time_start], [0], tofs, det_ids)


def generate_f144(value, time_stamp):
    return serialise_f144("axis", value, time_stamp)


def extract_state_from_status_message(buffer):
    return json.loads(deserialise_x5f2(buffer).status_json)["state"]


def extract_time_from_status_message(buffer):
    return json.loads(deserialise_x5f2(buffer).status_json)["state"]


def start_filewriter(
    producer, file_name, job_id, start_time_s, stop_time_s=None, metadata=""
):
    print(f"\nStarting writing file: {file_name}")
    buffer = serialise_pl72(
        job_id=job_id,
        filename=file_name,
        start_time=start_time_s * 1000,
        stop_time=stop_time_s * 1000 if stop_time_s else None,
        run_name="test_run",
        nexus_structure=json.dumps(NEXUS_STRUCTURE),
        service_id="",
        instrument_name="",
        broker="localhost:9092",
        control_topic=INST_CONTROL_TOPIC,
        metadata=metadata,
    )
    producer.produce(POOL_TOPIC, buffer)
    producer.flush()


def stop_filewriter(producer, job_id):
    buffer = serialise_6s4t(
        job_id=job_id,
        run_name="test_run",
        service_id="",
        stop_time=None,
        command_id=str(uuid.uuid4()),
    )
    producer.produce(INST_CONTROL_TOPIC, buffer)
    producer.flush()


def create_consumer(topic):
    consumer_conf = {
        "bootstrap.servers": ",".join(get_brokers()),
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
        producer = Producer({"bootstrap.servers": ",".join(get_brokers())})
        time_stamp = int(time.time())
        file_name = f"{time_stamp}.nxs"
        job_id = str(uuid.uuid4())
        metadata = json.dumps({"hello": 123})

        start_filewriter(producer, file_name, job_id, time_stamp, metadata=metadata)

        # Collect messages while writing
        start_time = time.time()
        messages = []
        while time.time() < start_time + 30:
            msg = consumer.poll(0.005)
            if msg:
                messages.append(msg)

        stop_filewriter(producer, job_id)

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
        assert deserialise_wrdn(messages[~0].value()).metadata == metadata
        # Finally check file exists
        assert os.path.exists(file_name)

    def test_data_written_to_file_is_correct(self, file_writer):
        """
        Test that the data in the file is what is expected
        """
        producer = Producer({"bootstrap.servers": ",".join(get_brokers())})
        time_stamp = int(time.time())
        file_name = f"{time_stamp}.nxs"
        job_id = str(uuid.uuid4())

        start_filewriter(producer, file_name, job_id, time_stamp)

        # Send some data to Kafka
        messages_sent = 0
        times = []
        while messages_sent < 30:
            times.append(time.time_ns())
            buffer = generate_ev44(
                times[~0],
                [i * 10 + messages_sent * 50 for i in range(5)],
                [i + messages_sent * 5 for i in range(5)],
            )
            producer.produce(DETECTOR_TOPIC, buffer)
            buffer = generate_f144(messages_sent, times[~0])
            producer.produce(MOTION_TOPIC, buffer)
            producer.flush()
            messages_sent += 1
            time.sleep(1)

        stop_filewriter(producer, job_id)

        # Give the file time to stop writing
        time.sleep(10)

        with h5py.File(file_name, "r") as f:
            # Check data is as expected
            assert len(f["/entry/detector/event_time_zero"]) == messages_sent
            assert len(f["/entry/detector/event_index"]) == messages_sent
            assert np.array_equal(f["/entry/detector/event_time_zero"][:], times)
            assert np.array_equal(
                f["/entry/detector/event_id"][:], [i for i in range(messages_sent * 5)]
            )
            assert np.array_equal(
                f["/entry/detector/event_time_offset"][:],
                [i * 10 for i in range(messages_sent * 5)],
            )
            assert f["/entry/title"][()].decode() == "This is my title"
            assert np.array_equal(f["/entry/motion/time"][:], times)
            assert np.array_equal(
                f["/entry/motion/value"][:], [i for i in range(messages_sent)]
            )

    def test_rejoins_pool_after_writing_done(self, file_writer):
        consumer, topic_partitions = create_consumer(POOL_STATUS_TOPIC)
        producer = Producer({"bootstrap.servers": ",".join(get_brokers())})
        approximate_write_time = 15

        messages = []
        start_time = time.time()
        while time.time() < start_time + 12:
            msg = consumer.poll(0.005)
            if msg:
                messages.append(msg)

        time_stamp = int(time.time())
        file_name = f"{time_stamp}.nxs"
        job_id = str(uuid.uuid4())

        start_filewriter(producer, file_name, job_id, time_stamp)

        time.sleep(approximate_write_time)
        messages_before = len(messages)

        stop_filewriter(producer, job_id)

        start_time = time.time()
        while time.time() < start_time + 12:
            msg = consumer.poll(0.005)
            if msg:
                messages.append(msg)

        assert len(messages) >= messages_before + 2
        # Check that the gap between the last message before and the first message after is greater than the write time
        assert (
            messages[messages_before].timestamp()[1]
            - messages[messages_before - 1].timestamp()[1]
            >= approximate_write_time
        )

    def test_start_and_stop_in_same_message(self, file_writer):
        """
        Tests that we get a file when the start and stop times are in the start message
        """
        producer = Producer({"bootstrap.servers": ",".join(get_brokers())})
        file_time_s = 10
        start_time = int(time.time())
        end_time = start_time + file_time_s
        file_name = f"{start_time}.nxs"
        job_id = str(uuid.uuid4())

        start_filewriter(producer, file_name, job_id, start_time, stop_time_s=end_time)

        # Give file time to write and finish
        time.sleep(30)

        with h5py.File(file_name, "r") as f:
            # Check start and stop time are what we expect
            assert datetime.strptime(
                f["/entry/start_time"][()].decode(), "%Y-%m-%dT%H:%M:%S.%fZ"
            ) == datetime.utcfromtimestamp(start_time)
            assert datetime.strptime(
                f["/entry/end_time"][()].decode(), "%Y-%m-%dT%H:%M:%S.%fZ"
            ) == datetime.utcfromtimestamp(end_time)