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


def test_static_data_reaches_file(writer_channel, worker_pool, kafka_address):
    wait_writers_available(writer_channel, nr_of=1, timeout=10)
    now = datetime.now()
    start_time = now - timedelta(seconds=10)
    stop_time = now
    file_name = "output_file_kafka_meta_data.nxs"
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

    wait_no_working_writers(writer_channel, timeout=30)
    current_jobs = writer_channel.list_known_jobs()
    for c_job in current_jobs:
        if c_job.job_id == write_job.job_id:
            assert "extra" in c_job.metadata
            assert "hdf_structure" in c_job.metadata
            assert c_job.metadata["hdf_structure"] == structure
            return

    assert False, "Unable to find current job."
