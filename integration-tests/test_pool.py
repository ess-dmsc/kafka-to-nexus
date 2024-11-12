import json
import os.path
import time
import uuid

from confluent_kafka import Producer

from common import create_consumer, deserialise_wrdn, start_filewriter, stop_filewriter
from conftest import INST_CONTROL_TOPIC_1, INST_CONTROL_TOPIC_2, OUTPUT_DIR, get_brokers


class TestPool:
    def test_two_writers_write_three_files(self, job_pool):
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
        messages_1 = []
        messages_2 = []
        while time.time() < start_time + 15:
            msg = consumer_1.poll(0.005)
            if msg:
                messages_1.append(msg)
            msg = consumer_2.poll(0.005)
            if msg:
                messages_2.append(msg)

        # Go through the messages and find the "wrdn" ones
        wrdn_files = []
        for msg in messages_1:
            if msg.value()[4:8] == b"wrdn":
                wrdn_files.append(deserialise_wrdn(msg.value()).file_name.split("/")[1])
        for msg in messages_2:
            if msg.value()[4:8] == b"wrdn":
                wrdn_files.append(deserialise_wrdn(msg.value()).file_name.split("/")[1])

        assert len(wrdn_files) == 3
        # Check the filename in the wrdn matches
        assert file_name_1 in wrdn_files
        assert file_name_2 in wrdn_files
        assert file_name_3 in wrdn_files
        # Finally check files exists
        assert os.path.exists(os.path.join(OUTPUT_DIR, file_name_1))
        assert os.path.exists(os.path.join(OUTPUT_DIR, file_name_2))
        assert os.path.exists(os.path.join(OUTPUT_DIR, file_name_3))
