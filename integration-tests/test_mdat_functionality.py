from helpers.nexushelpers import OpenNexusFile
from helpers.kafkahelpers import (
    create_producer
)
from datetime import datetime, timedelta
from file_writer_control.WriteJob import WriteJob
from helpers import full_file_path
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
)
import numpy as np


def test_mdat(worker_pool, kafka_address, hdf_file_name="mdat_output.nxs"):
    file_path = full_file_path(hdf_file_name)
    wait_writers_available(worker_pool, nr_of=1, timeout=20)
    producer = create_producer(kafka_address)

    data_topic = "mdat_topic"
    source_name = "mdat_source"

    start_t = datetime(year=2023, month=7, day=7, hour=0, minute=0, second=0)
    # here we do the writing

    stop_time = start_t + timedelta(seconds=148)
    with open("commands/nexus_structure_mdat.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_path,
        broker=kafka_address,
        start_t=start_t,
        stop_time=stop_time,
    )
    wait_start_job(worker_pool, write_job, timeout=20)
    wait_no_working_writers(worker_pool, timeout=30)
    with OpenNexusFile(file_path) as file:
        assert (
            file["entry/myFWStuff/start_time"][:].flatten()
            == 1000
        ).all()
        assert (
            file["entry/myFWStuff/end_time"][:].flatten()
            == 10000
        ).all()
