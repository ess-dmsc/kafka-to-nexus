from helpers.kafkahelpers import (
    create_producer,
    publish_ep00_message,
)
from helpers.nexushelpers import OpenNexusFile
from datetime import datetime, timedelta
from streaming_data_types.fbschemas.epics_connection_info_ep00.EventType import (
    EventType,
)
from file_writer_control import WriteJob
from helpers import build_relative_file_path
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
)


def test_ep00(worker_pool, kafka_address, hdf_file_name="output_file_ep00.nxs"):
    file_path = build_relative_file_path(hdf_file_name)
    wait_writers_available(worker_pool, nr_of=1, timeout=20)

    producer = create_producer(kafka_address)
    topic = "TEST_epicsConnectionStatus"
    now = datetime.now()
    start_time = now - timedelta(seconds=5)
    stop_time = now
    publish_ep00_message(producer, topic, EventType.NEVER_CONNECTED, start_time)
    publish_ep00_message(
        producer,
        topic,
        EventType.CONNECTED,
        timestamp=start_time + timedelta(seconds=0.01),
    )
    publish_ep00_message(
        producer,
        topic,
        EventType.CONNECTED,
        timestamp=stop_time + timedelta(seconds=1),
    )

    with open("commands/nexus_structure_epics_status.json", "r") as f:
        structure = f.read()

    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_path,
        broker=kafka_address,
        start_time=start_time,
        stop_time=stop_time,
    )
    wait_start_job(worker_pool, write_job, timeout=20)

    wait_no_working_writers(worker_pool, timeout=35)

    with OpenNexusFile(file_path) as file:
        assert file["EpicsConnectionStatus/connection_status_time"][0] == int(
            start_time.timestamp() * 1e9
        )
        assert file["EpicsConnectionStatus/connection_status"][0] == b"NEVER_CONNECTED"
        assert file["EpicsConnectionStatus/connection_status_time"][1] == int(
            (start_time + timedelta(seconds=0.01)).timestamp() * 1e9
        )
        assert file["EpicsConnectionStatus/connection_status"][1] == b"CONNECTED"
