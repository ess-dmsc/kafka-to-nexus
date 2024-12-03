import json
import os.path
import time
import uuid
from collections import Counter
from datetime import datetime

import h5py
import numpy as np
from confluent_kafka import Producer
from streaming_data_types import deserialise_answ
from streaming_data_types.action_response_answ import ActionOutcome
from streaming_data_types.finished_writing_wrdn import deserialise_wrdn

from common import (
    NEXUS_STRUCTURE,
    create_consumer,
    extract_state_from_status_message,
    generate_ev44,
    generate_f144,
    start_filewriter,
    stop_filewriter,
)
from conftest import (
    DETECTOR_TOPIC,
    INST_CONTROL_TOPIC_1,
    INST_CONTROL_TOPIC_2,
    MOTION_TOPIC,
    OUTPUT_DIR,
    POOL_STATUS_TOPIC,
    get_brokers,
)


def wait_for_file_to_finish_writing(filename, timeout_s=60):
    """Wait for the file to appear, if it doesn't appear within the time limit then raise"""
    filename = os.path.join(OUTPUT_DIR, filename)
    start_time = time.time()
    # First wait for file to appear
    while time.time() < start_time + timeout_s:
        if os.path.exists(filename):
            break
    # Then check if writing has finished
    while time.time() < start_time + timeout_s:
        try:
            with h5py.File(filename, "r") as f:
                # Opening raises if the file is still being written
                print(f"File {filename} is ready!")
            # Add a short delay just to be sure it closes
            time.sleep(5)
            return
        except OSError:
            print(f"Could not open file {filename} as it still being written...")
            time.sleep(2)
    raise RuntimeError(
        f"File {filename} not ready after {timeout_s} seconds - something must have gone wrong!"
    )


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

        # Should be at least two, depending on exact timings may get extra
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
        consumer, topic_partitions = create_consumer(INST_CONTROL_TOPIC_1)
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

        wait_for_file_to_finish_writing(file_name)

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
            # Use keys to keep data in order
            producer.produce(DETECTOR_TOPIC, buffer, key="a")
            buffer = generate_f144(messages_sent, times[~0])
            producer.produce(MOTION_TOPIC, buffer, key="b")
            producer.flush()
            messages_sent += 1
            time.sleep(1)

        stop_filewriter(producer, job_id)

        wait_for_file_to_finish_writing(file_name)

        with h5py.File(os.path.join(OUTPUT_DIR, file_name), "r") as f:
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

        wait_for_file_to_finish_writing(file_name)

        with h5py.File(os.path.join(OUTPUT_DIR, file_name), "r") as f:
            # Check start and stop time are what we expect
            assert datetime.strptime(
                f["/entry/start_time"][()].decode(), "%Y-%m-%dT%H:%M:%S.%fZ"
            ) == datetime.utcfromtimestamp(start_time)
            assert datetime.strptime(
                f["/entry/end_time"][()].decode(), "%Y-%m-%dT%H:%M:%S.%fZ"
            ) == datetime.utcfromtimestamp(end_time)

    def test_two_writers_write_three_files(self, file_writer, second_filewriter):
        """
        Tests that the pool system works as designed.
        Requests three jobs but can only write two as only two file-writers, so the third one gets queued.
        When one of the file-writers finish the queued job should get picked up.
        """
        consumer_1, _ = create_consumer(INST_CONTROL_TOPIC_1)
        consumer_2, _ = create_consumer(INST_CONTROL_TOPIC_2)
        producer = Producer({"bootstrap.servers": ",".join(get_brokers())})

        # Start job 1
        time_stamp_1 = int(time.time())
        file_name_1 = f"{time_stamp_1}.nxs"
        job_id_1 = str(uuid.uuid4())
        start_filewriter(
            producer,
            file_name_1,
            job_id_1,
            time_stamp_1,
            inst_control_topic=INST_CONTROL_TOPIC_1,
        )

        # Start job 2
        time_stamp_2 = time_stamp_1 + 1
        file_name_2 = f"{time_stamp_2}.nxs"
        job_id_2 = str(uuid.uuid4())
        start_filewriter(
            producer,
            file_name_2,
            job_id_2,
            time_stamp_2,
            inst_control_topic=INST_CONTROL_TOPIC_2,
        )

        # Start job 3 - won't get picked up immediately as all file-writers are busy
        time_stamp_3 = time_stamp_1 + 2
        file_name_3 = f"{time_stamp_3}.nxs"
        job_id_3 = str(uuid.uuid4())
        start_filewriter(
            producer,
            file_name_3,
            job_id_3,
            time_stamp_3,
            inst_control_topic=INST_CONTROL_TOPIC_1,
        )

        # Let them write for a bit
        time.sleep(15)

        # Stop all jobs
        stop_filewriter(producer, job_id_1, INST_CONTROL_TOPIC_1)
        stop_filewriter(producer, job_id_2, INST_CONTROL_TOPIC_2)
        # "stopping" job 3 even though it hasn't started yet
        stop_filewriter(producer, job_id_3, INST_CONTROL_TOPIC_1)

        start_time = time.time()
        messages = []
        while time.time() < start_time + 15:
            msg = consumer_1.poll(0.005)
            if msg:
                messages.append(msg)
            msg = consumer_2.poll(0.005)
            if msg:
                messages.append(msg)

        wait_for_file_to_finish_writing(file_name_3)

        # Go through the messages and find the "wrdn" ones
        wrdn_files = []
        for msg in messages:
            if msg.value()[4:8] == b"wrdn":
                wrdn_files.append(deserialise_wrdn(msg.value()).file_name.split("/")[1])

        assert len(wrdn_files) == 3
        # Check the filename in the wrdn matches
        assert file_name_1 in wrdn_files
        assert file_name_2 in wrdn_files
        assert file_name_3 in wrdn_files
        # Finally check files exist
        assert os.path.exists(os.path.join(OUTPUT_DIR, file_name_1))
        assert os.path.exists(os.path.join(OUTPUT_DIR, file_name_2))
        assert os.path.exists(os.path.join(OUTPUT_DIR, file_name_3))

    def test_data_before_and_after(self, file_writer):
        """
        Test that when writing in the past:
         - the last value before the start time is written
         - the first value after the stop time is written
         - all values inbetween are written
        """
        producer = Producer({"bootstrap.servers": ",".join(get_brokers())})

        # Send some data to Kafka before starting writing
        messages_sent = 0
        times_ns = []
        while messages_sent < 15:
            times_ns.append(time.time_ns())
            buffer = generate_ev44(
                times_ns[~0],
                [i * 10 + messages_sent * 50 for i in range(5)],
                [i + messages_sent * 5 for i in range(5)],
            )
            # Use keys to keep data in order
            producer.produce(DETECTOR_TOPIC, buffer, key="a")
            buffer = generate_f144(messages_sent, times_ns[~0])
            producer.produce(MOTION_TOPIC, buffer, key="b")
            producer.flush()
            messages_sent += 1
            time.sleep(1)

        start_time_s = times_ns[4] // 10**9
        end_time_s = times_ns[10] // 10**9
        file_name = f"{start_time_s}.nxs"
        job_id = str(uuid.uuid4())

        start_filewriter(
            producer, file_name, job_id, start_time_s, stop_time_s=end_time_s
        )

        # Give the file time to stop writing
        wait_for_file_to_finish_writing(file_name)

        with h5py.File(os.path.join(OUTPUT_DIR, file_name), "r") as f:
            # Check data is as expected
            assert len(f["/entry/detector/event_time_zero"]) == 7
            assert f["/entry/detector/event_id"][0] == 15
            assert f["/entry/detector/event_id"][~0] == 54
            assert len(f["/entry/motion/time"]) == 8
            assert f["/entry/motion/value"][0] == 3
            assert f["/entry/motion/value"][~0] == 10

    def test_start_and_stop_in_the_future(self, file_writer):
        """
        Tests that we get a file when the start and stop times are in the future
        """
        producer = Producer({"bootstrap.servers": ",".join(get_brokers())})
        file_time_s = 10
        start_time = int(time.time()) + 30
        end_time = start_time + file_time_s
        file_name = f"{start_time}.nxs"
        job_id = str(uuid.uuid4())

        start_filewriter(producer, file_name, job_id, start_time, stop_time_s=end_time)

        # Wait for the future to catch up
        time.sleep(40)

        wait_for_file_to_finish_writing(file_name)

        with h5py.File(os.path.join(OUTPUT_DIR, file_name), "r") as f:
            # Check start and stop time are what we expect
            assert datetime.strptime(
                f["/entry/start_time"][()].decode(), "%Y-%m-%dT%H:%M:%S.%fZ"
            ) == datetime.utcfromtimestamp(start_time)
            assert datetime.strptime(
                f["/entry/end_time"][()].decode(), "%Y-%m-%dT%H:%M:%S.%fZ"
            ) == datetime.utcfromtimestamp(end_time)

    def test_writing_does_not_start_on_invalid_input(self, file_writer):
        """
        Tests that if the input is invalid then it responds on the pool status topic that it could not start.
        For this example, we just break the JSON but there are other ways to break it but the behaviour is the same.
        """
        consumer, topic_partitions = create_consumer(POOL_STATUS_TOPIC)
        producer = Producer({"bootstrap.servers": ",".join(get_brokers())})
        time_stamp = int(time.time())
        file_name = f"{time_stamp}.nxs"
        job_id = str(uuid.uuid4())

        # Mess the json up
        nexus_structure = json.dumps(NEXUS_STRUCTURE).replace("{", "", 1)

        start_filewriter(
            producer, file_name, job_id, time_stamp, nexus_structure=nexus_structure
        )

        # Collect messages on pool status topic
        messages = []
        start_time = time.time()
        while time.time() < start_time + 15:
            msg = consumer.poll(0.005)
            if msg:
                messages.append(msg.value())

        answer = None
        for msg in messages:
            if msg[4:8] == b"answ":
                answer = deserialise_answ(msg)
                break

        assert answer
        assert answer.outcome == ActionOutcome.Failure
