from helpers.nexushelpers import OpenNexusFile
from datetime import datetime, timedelta
import numpy as np
from file_writer_control.WriteJob import WriteJob
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
)


def test_static_data_reaches_file(writer_channel, worker_pool, kafka_address):
    wait_writers_available(writer_channel, nr_of=1, timeout=10)
    now = datetime.now()
    start_time = now - timedelta(seconds=10)
    stop_time = now
    file_name = "output_file_links.nxs"
    with open("commands/nexus_structure_links.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_name,
        broker=kafka_address,
        start_time=start_time,
        stop_time=stop_time,
    )
    wait_start_job(worker_pool, write_job, timeout=20)

    wait_no_working_writers(writer_channel, timeout=30)

    file_path = f"output-files/{file_name}"
    with OpenNexusFile(file_path) as file:
        assert not file.swmr_mode
        assert file["entry/link_to_features"][0] == 10138143369737381149
        assert file["entry//instrument/link_to_monitor1_det_id"][0] == 11
        assert np.allclose(
            file["entry/link_to_monitor1_transform/location"].attrs["vector"],
            np.array([0.0, 0.0, -1.0]),
        )
        assert (
            file["entry/link_to_monitor1_transform/location"].attrs[
                "transformation_type"
            ]
            == "translation"
        )
