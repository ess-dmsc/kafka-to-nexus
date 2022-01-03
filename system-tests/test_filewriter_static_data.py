from helpers.kafkahelpers import (
    create_producer,
)
from helpers.nexushelpers import OpenNexusFile
from datetime import datetime, timedelta
import numpy as np
from file_writer_control.WriteJob import WriteJob
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
)


def test_static_data_reaches_file(worker_pool, kafka_address):
    wait_writers_available(worker_pool, nr_of=1, timeout=10)
    now = datetime.now()
    start_time = now - timedelta(seconds=10)
    stop_time = now
    file_name = "output_file_static.nxs"
    with open("commands/nexus_structure_static.json", "r") as f:
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
        assert not file.swmr_mode
        assert file["entry/start_time"][()][0].decode("utf-8") == "2016-04-12T02:58:52"
        assert file["entry/end_time"][()][0].decode("utf-8") == "2016-04-12T03:29:11"
        assert file["entry/duration"][()] == 1817.0
        assert file["entry/features"][0] == 10138143369737381149
        assert file["entry/user_1/affiliation"][()][0].decode("utf-8") == "ISIS, STFC"
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
