from helpers.kafkahelpers import create_producer, send_writer_command
from helpers.nexushelpers import OpenNexusFileWhenAvailable
from time import sleep
import numpy as np
import pytest


def test_data_reaches_file(docker_compose):
    producer = create_producer()
    sleep(20)
    # Start file writing
    job_id = send_writer_command(
        "commands/start-command-multiple-modules.json",
        producer,
        start_time=int(docker_compose),
    )
    # Give it some time to accumulate data
    sleep(10)
    # Stop file writing
    send_writer_command("commands/stop-command.json", producer, job_id=job_id)

    filepath = "output-files/output_file_multiple_modules.nxs"
    with OpenNexusFileWhenAvailable(filepath) as file:
        assert len(file["entry/sample/dataset2/cue_index"][:]) > 1
        assert file["entry/sample/dataset2/cue_index"][0] == 0
        assert file["entry/sample/dataset2/cue_index"][1] == 1
