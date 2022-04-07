from helpers.kafkahelpers import (
    create_producer,
    publish_f142_message,
)
from helpers.nexushelpers import OpenNexusFile
from datetime import datetime, timedelta
import pytest

from file_writer_control.WriteJob import WriteJob
from helpers import full_file_path
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
)


def test_two_different_writer_modules_with_same_flatbuffer_id(
    worker_pool, kafka_address, hdf_file_name="output_file_multiple_modules.nxs"
):
    file_path = full_file_path(hdf_file_name)
    wait_writers_available(worker_pool, nr_of=1, timeout=20)
    producer = create_producer()
    now = datetime.now()
    start_time = now - timedelta(seconds=10)
    stop_time = now
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

    with open("commands/nexus_structure_multiple_modules.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_path,
        broker=kafka_address,
        start_time=start_time,
        stop_time=stop_time,
    )
    wait_start_job(worker_pool, write_job, timeout=20)
    wait_no_working_writers(worker_pool, timeout=35)

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
