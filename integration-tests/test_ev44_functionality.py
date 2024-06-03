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


def test_ev44_several_pulses(
    worker_pool, kafka_address, hdf_file_name="ev44_output_file.nxs"
):
    file_path = build_relative_file_path(hdf_file_name)
    wait_writers_available(worker_pool, nr_of=1, timeout=20)
    producer = create_producer(kafka_address)

    data_topic = "ev44_topic"
    source_name = "ev44_source"

    reference_time = [1000, 2000, 3000]
    reference_time_index = [0, 3, 5]
    time_of_flight = [11, 12, 13, 21, 22, 31, 32, 33]
    pixel_id = [11, 12, 13, 21, 22, 31, 32, 33]
    start_time = datetime(year=2020, month=6, day=12, hour=11, minute=1, second=35)
    publish_ev44_message(
        producer=producer,
        topic=data_topic,
        reference_time=reference_time,
        reference_time_index=reference_time_index,
        time_of_flight=time_of_flight,
        pixel_id=pixel_id,
        timestamp=start_time + timedelta(seconds=10),
        source_name=source_name,
    )

    stop_time = start_time + timedelta(seconds=15)
    with open("commands/nexus_structure_ev44.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_path,
        broker=kafka_address,
        start_time=start_time,
        stop_time=stop_time,
        metadata="{}",
    )
    wait_start_job(worker_pool, write_job, timeout=20)
    wait_no_working_writers(worker_pool, timeout=30)
    with OpenNexusFile(file_path) as file:
        assert (
            file["entry/test/event_data/event_id"][:].flatten() == np.array(pixel_id)
        ).all()
        assert (
            file["entry/test/event_data/event_time_offset"][:].flatten()
            == np.array(time_of_flight)
        ).all()
        assert (
            file["entry/test/event_data/event_time_zero"][:].flatten()
            == np.array(reference_time)
        ).all()
        assert (
            file["entry/test/event_data/event_index"][:].flatten()
            == np.array(reference_time_index)
        ).all()
        # The message has more events than the configured cue_interval,
        # thus the last event must be recorded in the cue
        assert (
            file["entry/test/event_data/cue_timestamp_zero"][:].flatten()
            == np.array([3033])
        ).all()
        assert (
            file["entry/test/event_data/cue_index"][:].flatten() == np.array([7])
        ).all()
