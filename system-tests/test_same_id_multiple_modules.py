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


def test_two_different_writer_modules_with_same_flatbuffer_id(docker_compose):
    producer = create_producer()

    # Produce some f142 data
    publish_f142_message(
        producer,
        "TEST_sampleEnv",
        int(unix_time_milliseconds(datetime.utcnow())),
        source_name="test_source_1",
    )
    publish_f142_message(
        producer,
        "TEST_sampleEnv",
        int(unix_time_milliseconds(datetime.utcnow())),
        source_name="test_source_2",
    )
    publish_f142_message(
        producer,
        "TEST_sampleEnv",
        int(unix_time_milliseconds(datetime.utcnow())),
        source_name="test_source_2",
    )

    sleep(20)
    # Start file writing
    job_id = publish_run_start_message(producer, "commands/nexus_structure_multiple_modules.json",
                                       "output_file_multiple_modules.nxs", start_time=int(docker_compose))
    # Give it some time to accumulate data
    sleep(10)
    # Stop file writing
    publish_run_stop_message(producer, job_id=job_id)

    filepath = "output-files/output_file_multiple_modules.nxs"
    with OpenNexusFileWhenAvailable(filepath) as file:
        assert (
            len(file["entry/sample/dataset1/time"][:]) > 0
            and len(file["entry/sample/dataset1/value"][:]) > 0
        ), "f142 module should have written this dataset, it should have written a value and time"

        assert (
            "cue_timestamp_zero" not in file["entry/sample/dataset2"]
        ), "f142_test module should have written this dataset, it writes cue_index but no cue_timestamp_zero"
        assert (
            file["entry/sample/dataset2/cue_index"][0] == 0
        ), "Expect consecutive integers to be written by f142_test"
        assert (
            file["entry/sample/dataset2/cue_index"][1] == 1
        ), "Expect consecutive integers to be written by f142_test"
