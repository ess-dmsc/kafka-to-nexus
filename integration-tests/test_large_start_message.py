import time

from helpers.nexushelpers import OpenNexusFile
from datetime import datetime, timedelta
import pytest
from file_writer_control.WriteJob import WriteJob
from helpers import build_relative_file_path
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
)


@pytest.mark.parametrize("json_padding", [(1024**2), (16 * 1024**2), (64 * 1024**2)])
def test_large_start_message(worker_pool, kafka_address, json_padding):
    file_path = build_relative_file_path(f"output_file_large_msg_{json_padding}.nxs")
    wait_writers_available(worker_pool, nr_of=1, timeout=20)
    now = datetime.now()
    start_time = now - timedelta(seconds=10)
    stop_time = now
    with open("commands/nexus_structure_static.json", "r") as f:
        structure = f.read()
    structure += " " * json_padding
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_path,
        broker=kafka_address,
        start_time=start_time,
        stop_time=stop_time,
        metadata="{}",
    )
    wait_start_job(worker_pool, write_job, timeout=70)

    wait_no_working_writers(worker_pool, timeout=40)
    time.sleep(5)  # test is prone to fail in worker_pool's stop_current_jobs finalizer

    with OpenNexusFile(file_path) as file:
        assert not file.swmr_mode
        assert file["entry/start_time"][()].decode("utf-8") == "2016-04-12T02:58:52"
