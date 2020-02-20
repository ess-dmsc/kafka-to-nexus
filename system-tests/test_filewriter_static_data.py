from helpers.kafkahelpers import create_producer, publish_run_start_message, publish_run_stop_message
from helpers.nexushelpers import OpenNexusFileWhenAvailable
from time import sleep
import numpy as np


def test_static_data_reaches_file(docker_compose):

    producer = create_producer()
    sleep(20)
    # Start file writing
    job_id = publish_run_start_message(producer, "commands/nexus_structure_static.json",
                                       "output_file_static.nxs", start_time=int(docker_compose))

    # Give it some time to accumulate data
    sleep(10)
    # Stop file writing
    publish_run_stop_message(producer, job_id=job_id)

    filepath = "output-files/output_file_static.nxs"
    with OpenNexusFileWhenAvailable(filepath) as file:
        assert not file.swmr_mode
        assert file["entry/start_time"].value == "2016-04-12T02:58:52"
        assert file["entry/end_time"].value == "2016-04-12T03:29:11"
        assert file["entry/duration"].value == 1817.0
        assert file["entry/features"][0] == 10138143369737381149
        assert file["entry/user_1/affiliation"].value == "ISIS, STFC"
        assert np.allclose(
            file["entry/instrument/monitor1/transformations/location"].attrs["vector"],
            np.array([0.0, 0.0, -1.0]),
        )
        assert (
            file["entry/instrument/monitor1/transformations/location"].attrs[
                "transformation_type"
            ]
            == "translation"
        )
