from helpers.kafkahelpers import (
    create_producer,
    publish_f142_message,
)
from helpers.nexushelpers import OpenNexusFile
from datetime import datetime, timedelta
import pytest

from file_writer_control.WriteJob import WriteJob
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
)


def test_two_different_writer_modules_with_same_flatbuffer_id(
    writer_channel, kafka_address
):
    wait_writers_available(writer_channel, nr_of=1, timeout=10)
    producer = create_producer()
    start_time = datetime.now() - timedelta(seconds=10)
    for i in range(10):
        current_time = start_time + timedelta(seconds=1) * i
        publish_f142_message(
            producer,
            "TEST_sampleEnv",
            current_time,
            source_name="test_source_1",
        )
        publish_f142_message(
            producer,
            "TEST_sampleEnv",
            current_time,
            source_name="test_source_2",
        )
    file_name = "output_file_multiple_modules.nxs"
    with open("commands/nexus_structure_multiple_modules.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_name,
        broker=kafka_address,
        start_time=start_time,
        stop_time=datetime.now(),
    )
    wait_start_job(writer_channel, write_job, timeout=20)
    wait_no_working_writers(writer_channel, timeout=30)

    file_path = f"output-files/{file_name}"
    with OpenNexusFile(file_path) as file:
        assert (
            len(file["entry/sample/dataset1/time"][:]) > 0
            and len(file["entry/sample/dataset1/value"][:]) > 0
        ), "f142 module should have written this dataset, it should have written a value and time"

        assert (
            "cue_timestamp_zero" not in file["entry/sample/dataset2"]
        ), "f142_test module should have written this dataset, it writes cue_index but no cue_timestamp_zero"
        assert (
            len(file["entry/sample/dataset2/cue_index"][:]) > 0
        ), "Expected index values, found none."
        for i in range(len(file["entry/sample/dataset2/cue_index"][:])):
            assert (
                file["entry/sample/dataset2/cue_index"][i] == i
            ), "Expect consecutive integers to be written by f142_test"
