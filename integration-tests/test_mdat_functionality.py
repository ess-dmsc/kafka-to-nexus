from helpers.nexushelpers import OpenNexusFile
from datetime import datetime, timedelta
from file_writer_control.WriteJob import WriteJob
from helpers import build_relative_file_path
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
)


def test_mdat(worker_pool, kafka_address, hdf_file_name="mdat_output.nxs"):
    file_path = build_relative_file_path(hdf_file_name)
    wait_writers_available(worker_pool, nr_of=1, timeout=20)

    start_t = datetime(year=2023, month=7, day=7, hour=2, minute=0, second=0)
    stop_t = start_t + timedelta(seconds=10)

    with open("commands/nexus_structure_filewriter.json", "r") as f:
        structure = f.read()

    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_path,
        broker=kafka_address,
        start_time=start_t,
        stop_time=stop_t,
    )
    wait_start_job(worker_pool, write_job, timeout=20)
    wait_no_working_writers(worker_pool, timeout=30)
    with OpenNexusFile(file_path) as file:
        assert (
            file["entry/myFWStuff/start_time"][()][0].decode("utf-8")
            == "2023-07-07T00:00:00Z"
        )
        assert (
            file["entry/myFWStuff/end_time"][()][0].decode("utf-8")
            == "2023-07-07T00:00:10Z"
        )
