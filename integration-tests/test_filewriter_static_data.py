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


def test_static_data_reaches_file(
    worker_pool, kafka_address, hdf_file_name="output_file_static.nxs"
):
    file_path = build_relative_file_path(hdf_file_name)
    wait_writers_available(worker_pool, nr_of=1, timeout=10)
    now = datetime.now()
    start_time = now - timedelta(seconds=10)
    stop_time = now
    with open("commands/nexus_structure_static.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_path,
        broker=kafka_address,
        start_time=start_time,
        stop_time=stop_time,
        control_topic="TEST_writer_commands",
        metadata="{}",
    )
    wait_start_job(worker_pool, write_job, timeout=20)

    wait_no_working_writers(worker_pool, timeout=30)

    with OpenNexusFile(file_path) as file:
        assert not file.swmr_mode
        assert file["entry/start_time"][()].decode("utf-8") == "2016-04-12T02:58:52"
        assert file["entry/end_time"][()].decode("utf-8") == "2016-04-12T03:29:11"
        assert np.array_equal(file["entry/test_vector1"][:], np.array([1, 2, 3]))
        assert np.array_equal(
            file["entry/test_vector2"][:], np.array([[1, 2, 3], [-1, -2, -3]])
        )
        assert file["entry/duration"][()] == 1817.0
        assert file["entry/features"][0] == 10138143369737381149
        assert file["entry/user_1/affiliation"][()].decode("utf-8") == "ISIS, STFC"
        assert np.allclose(
            file["entry/instrument/monitor1/transformations/location"].attrs["vector"],
            np.array([0.0, 0.0, -1.0]),
        )
        assert np.allclose(
            file["entry/instrument/monitor1/transformations/location"].attrs["vector2"],
            np.array([[3.0, 2.0, -1.0], [-1.0, -2.0, -3.0]]),
        )
        assert (
            file["entry/instrument/monitor1/transformations/location"].attrs[
                "transformation_type"
            ]
            == "translation"
        )
