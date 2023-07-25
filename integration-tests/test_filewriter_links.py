from helpers.nexushelpers import OpenNexusFile
from datetime import datetime, timedelta
import numpy as np
from file_writer_control.WriteJob import WriteJob
from helpers import build_relative_file_path
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
)


def test_links(worker_pool, kafka_address, hdf_file_name="output_file_links.nxs"):
    file_path = build_relative_file_path(hdf_file_name)
    wait_writers_available(worker_pool, nr_of=1, timeout=20)
    now = datetime.now()
    start_time = now - timedelta(seconds=10)
    stop_time = now

    with open("commands/nexus_structure_links.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_path,
        broker=kafka_address,
        start_time=start_time,
        stop_time=stop_time,
    )
    wait_start_job(worker_pool, write_job, timeout=20)

    wait_no_working_writers(worker_pool, timeout=30)

    with OpenNexusFile(file_path) as file:
        assert not file.swmr_mode
        assert file["entry/link_to_features"][0] == 10138143369737381149
        assert file["entry/instrument/link2_to_features"][0] == 10138143369737381149
        assert file["entry/instrument/link_to_monitor1_det_id"][0] == 11
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
