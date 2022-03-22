from helpers.nexushelpers import OpenNexusFile
from helpers.kafkahelpers import (
    create_producer,
    publish_f142_message,
)
from datetime import datetime, timedelta
from file_writer_control.WriteJob import WriteJob
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
)
import numpy as np


def test_repeated_messages(worker_pool, kafka_address, file_name = "output_file_repeated_messages.nxs"):
    wait_writers_available(worker_pool, nr_of=1, timeout=10)
    producer = create_producer()

    data_topic = "TEST_repeatedMessages"

    start_time = datetime(year=2019, month=6, day=12, hour=11, minute=1, second=35)
    stop_time = start_time + timedelta(seconds=200)
    step_time = timedelta(seconds=1)

    # Create pre-start messages
    publish_f142_message(producer, data_topic, start_time - step_time * 5, value=1)
    publish_f142_message(producer, data_topic, start_time - step_time * 4, value=2)
    publish_f142_message(producer, data_topic, start_time - step_time * 3, value=3)
    for i in range(3):
        publish_f142_message(
            producer, data_topic, start_time - step_time * 2, value=10 + i
        )
    publish_f142_message(producer, data_topic, start_time - step_time, value=19)

    # Create post-start messages
    for i in range(3):
        publish_f142_message(producer, data_topic, start_time + step_time, value=20 + i)

    # Create post-stop messages
    for i in range(3):
        publish_f142_message(producer, data_topic, stop_time + step_time, value=30 + i)
    publish_f142_message(producer, data_topic, stop_time + step_time * 2, value=40)

    
    with open("commands/nexus_structure_repeated_messages.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_name,
        broker=kafka_address,
        start_time=start_time,
        stop_time=stop_time,
    )
    wait_start_job(worker_pool, write_job, timeout=20)

    wait_no_working_writers(worker_pool, timeout=30)

    file_path = f"output-files/{file_name}"
    with OpenNexusFile(file_path) as file:
        expected_elements = 3
        assert file["entry/repeated_messages/time"].len() == (
            expected_elements
        ), f"Expected there to be {expected_elements} saved messages"
        assert np.array_equal(
            file["entry/repeated_messages/value"][:], [[19], [20], [30]]
        ), "The wrong elements/messages were saved to file."
