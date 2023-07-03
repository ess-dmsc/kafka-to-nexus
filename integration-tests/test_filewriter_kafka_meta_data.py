from datetime import datetime, timedelta
from file_writer_control.WriteJob import WriteJob
from helpers import build_relative_file_path
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
)


def test_end_message_metadata(
    worker_pool, kafka_address, hdf_file_name="output_file_kafka_meta_data.nxs"
):
    file_path = build_relative_file_path(hdf_file_name)
    wait_writers_available(worker_pool, nr_of=1, timeout=20)
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
    )
    wait_start_job(worker_pool, write_job, timeout=20)

    wait_no_working_writers(worker_pool, timeout=30)
    current_jobs = worker_pool.list_known_jobs()
    for c_job in current_jobs:
        if c_job.job_id == write_job.job_id:
            assert "extra" in c_job.metadata
            assert (
                "hdf_structure" in c_job.metadata
            ), f"Assert failed, got the metadata: {c_job.metadata}"
            assert c_job.metadata["hdf_structure"] == structure
            return

    assert False, "Unable to find current job."
