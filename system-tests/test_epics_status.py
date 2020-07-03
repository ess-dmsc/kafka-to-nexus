from helpers.kafkahelpers import (
    create_producer,
    publish_run_start_message,
    publish_run_stop_message,
    publish_ep00_message,
)
from helpers.nexushelpers import OpenNexusFileWhenAvailable
from helpers.timehelpers import milliseconds_to_nanoseconds, current_unix_time_ms
from time import sleep
from streaming_data_types.fbschemas.epics_connection_info_ep00.EventType import (
    EventType,
)


def test_ep00(docker_compose):
    producer = create_producer()
    topic = "TEST_epicsConnectionStatus"
    sleep(10)

    # Start file writing
    job_id = publish_run_start_message(
        producer,
        "commands/nexus_structure_epics_status.json",
        "output_file_ep00.nxs",
        start_time=current_unix_time_ms(),
    )
    sleep(5)
    first_timestamp = current_unix_time_ms()
    publish_ep00_message(producer, topic, EventType.NEVER_CONNECTED, first_timestamp)
    second_timestamp = current_unix_time_ms()
    publish_ep00_message(
        producer, topic, EventType.CONNECTED, kafka_timestamp=second_timestamp
    )

    # Give it some time to accumulate data
    sleep(10)

    # Stop file writing
    publish_run_stop_message(producer, job_id, stop_time=current_unix_time_ms())

    filepath = "output-files/output_file_ep00.nxs"
    with OpenNexusFileWhenAvailable(filepath) as file:
        assert file["EpicsConnectionStatus/connection_status_time"][
            0
        ] == milliseconds_to_nanoseconds(first_timestamp)
        assert file["EpicsConnectionStatus/connection_status"][0] == b"NEVER_CONNECTED"
        assert file["EpicsConnectionStatus/connection_status_time"][
            1
        ] == milliseconds_to_nanoseconds(second_timestamp)
        assert file["EpicsConnectionStatus/connection_status"][1] == b"CONNECTED"
