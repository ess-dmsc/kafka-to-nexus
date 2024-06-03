from helpers.kafkahelpers import (
    create_producer,
    publish_ep01_message,
)
from helpers.nexushelpers import OpenNexusFile
from datetime import datetime, timedelta
from streaming_data_types.epics_connection_ep01 import ConnectionInfo
from file_writer_control import WriteJob
from helpers import build_relative_file_path
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
)


def test_ep01(worker_pool, kafka_address, hdf_file_name="output_file_ep01.nxs"):
    file_path = build_relative_file_path(hdf_file_name)
    wait_writers_available(worker_pool, nr_of=1, timeout=20)

    producer = create_producer(kafka_address)
    topic = "TEST_epicsConnectionStatus"
    source_name = "SIMPLE:DOUBLE"
    now = datetime.now()
    start_time = now - timedelta(seconds=5)
    stop_time = now
    publish_ep01_message(
        producer,
        topic,
        start_time,
        ConnectionInfo.NEVER_CONNECTED,
        source_name=source_name,
    )
    publish_ep01_message(
        producer,
        topic,
        start_time + timedelta(seconds=0.01),
        ConnectionInfo.CONNECTED,
        source_name=source_name,
    )
    publish_ep01_message(
        producer,
        topic,
        stop_time + timedelta(seconds=1),  # after stop time
        ConnectionInfo.CONNECTED,
        source_name=source_name,
    )

    with open("commands/nexus_structure_epics_status.json", "r") as f:
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

    wait_no_working_writers(worker_pool, timeout=35)

    with OpenNexusFile(file_path) as file:
        assert file["EpicsConnectionStatus/connection_status_time"][0] == int(
            start_time.timestamp() * 1e9
        )
        assert (
            file["EpicsConnectionStatus/connection_status"][0]
            == ConnectionInfo.NEVER_CONNECTED.value
        )
        assert file["EpicsConnectionStatus/connection_status_time"][1] == int(
            (start_time + timedelta(seconds=0.01)).timestamp() * 1e9
        )
        assert (
            file["EpicsConnectionStatus/connection_status"][1]
            == ConnectionInfo.CONNECTED.value
        )
