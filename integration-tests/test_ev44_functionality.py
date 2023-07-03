from helpers.nexushelpers import OpenNexusFile
from helpers.kafkahelpers import (
    create_producer,
    publish_ev44_message,
)
from datetime import datetime, timedelta
from file_writer_control.WriteJob import WriteJob
from helpers import build_relative_file_path
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
)
import numpy as np


def test_ev44(worker_pool, kafka_address, hdf_file_name="ev44_output_file.nxs"):
    file_path = build_relative_file_path(hdf_file_name)
    wait_writers_available(worker_pool, nr_of=1, timeout=20)
    producer = create_producer(kafka_address)

    data_topic = "ev44_topic"
    source_name = "ev44_source"

    start_time = datetime(year=2020, month=6, day=12, hour=11, minute=1, second=35)
    publish_ev44_message(
        producer,
        data_topic,
        [19284265, 19284268, 19284269],
        [12, 11, 13],
        [0, 1, 2],
        [2, 3, 4],
        start_time + timedelta(seconds=10),
        source_name=source_name,
    )

    stop_time = start_time + timedelta(seconds=148)
    with open("commands/nexus_structure_ev44.json", "r") as f:
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
        assert (
            file["entry/test/event_data/event_id"][:].flatten() == np.array([2, 3, 4])
        ).all()
        assert (
            file["entry/test/event_data/event_time_offset"][:].flatten()
            == np.array([0, 1, 2])
        ).all()
        assert (
            file["entry/test/event_data/event_time_zero"][:].flatten()
            == np.array([19284265, 19284268, 19284269])
        ).all()
        assert (
            file["entry/test/event_data/event_time_zero_index"][:].flatten()
            == np.array([12, 11, 13])
        ).all()
