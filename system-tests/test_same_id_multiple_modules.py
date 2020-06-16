from helpers.kafkahelpers import (
    create_producer,
    publish_run_start_message,
    publish_run_stop_message,
    publish_f142_message,
)
from helpers.nexushelpers import OpenNexusFileWhenAvailable
from helpers.timehelpers import unix_time_milliseconds
from time import sleep
from datetime import datetime
import pytest


def check(condition, fail_string):
    if not condition:
        pytest.fail(fail_string)


def test_two_different_writer_modules_with_same_flatbuffer_id(docker_compose):
    producer = create_producer()
    start_time = unix_time_milliseconds(datetime.utcnow()) - 10000
    for i in range(10):
        publish_f142_message(
            producer,
            "TEST_sampleEnv",
            int(start_time + i * 1000),
            source_name="test_source_1",
        )
        publish_f142_message(
            producer,
            "TEST_sampleEnv",
            int(start_time + i * 1000),
            source_name="test_source_2",
        )
    check(producer.flush(1.5) == 0, "Unable to flush kafka messages.")
    # Start file writing
    job_id = publish_run_start_message(
        producer,
        "commands/nexus_structure_multiple_modules.json",
        "output_file_multiple_modules.nxs",
        start_time=int(start_time),
        stop_time=int(start_time + 5 * 1000),
    )
    # Give it some time to accumulate data
    sleep(10)

    filepath = "output-files/output_file_multiple_modules.nxs"
    with OpenNexusFileWhenAvailable(filepath) as file:
        check(
            len(file["entry/sample/dataset1/time"][:]) > 0
            and len(file["entry/sample/dataset1/value"][:]) > 0,
            "f142 module should have written this dataset, it should have written a value and time",
        )

        check(
            "cue_timestamp_zero" not in file["entry/sample/dataset2"],
            "f142_test module should have written this dataset, it writes cue_index but no cue_timestamp_zero",
        )
        check(
            len(file["entry/sample/dataset2/cue_index"][:]) > 0,
            "Expected index values, found none.",
        )
        for i in range(len(file["entry/sample/dataset2/cue_index"][:])):
            check(
                file["entry/sample/dataset2/cue_index"][i] == i,
                "Expect consecutive integers to be written by f142_test",
            )
