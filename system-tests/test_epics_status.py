from helpers.kafkahelpers import (
    create_producer,
    publish_ep00_message,
)
from helpers.nexushelpers import OpenNexusFile
from datetime import datetime, timedelta
from streaming_data_types.fbschemas.epics_connection_info_ep00.EventType import (
    EventType,
)
from file_writer_control.WriteJob import WriteJob
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
)


def test_ep00(writer_channel):
    wait_writers_available(writer_channel, nr_of=1, timeout=10)
    producer = create_producer()
    topic = "TEST_epicsConnectionStatus"

    file_name = "output_file_ep00.nxs"
    with open("commands/nexus_structure_epics_status.json", "r") as f:
        structure = f.read()
    start_time = datetime.now() - timedelta(seconds=60)
    publish_ep00_message(producer, topic, EventType.NEVER_CONNECTED, start_time)
    publish_ep00_message(
        producer,
        topic,
        EventType.CONNECTED,
        timestamp=start_time + timedelta(seconds=0.01),
    )

    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_name,
        broker="localhost:9092",
        start_time=start_time,
        stop_time=datetime.now(),
    )
    wait_start_job(writer_channel, write_job, timeout=20)

    wait_no_working_writers(writer_channel, timeout=30)

    file_path = f"output-files/{file_name}"
    with OpenNexusFile(file_path) as file:
        assert file["EpicsConnectionStatus/connection_status_time"][0] == int(
            start_time.timestamp() * 1e9
        )
        assert file["EpicsConnectionStatus/connection_status"][0] == b"NEVER_CONNECTED"
        assert file["EpicsConnectionStatus/connection_status_time"][1] == int(
            (start_time + timedelta(seconds=0.01)).timestamp() * 1e9
        )
        assert file["EpicsConnectionStatus/connection_status"][1] == b"CONNECTED"
