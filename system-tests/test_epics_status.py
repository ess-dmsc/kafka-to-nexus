from helpers.kafkahelpers import (
    create_producer,
    send_writer_command,
    publish_ep00_message,
)
from helpers.nexushelpers import OpenNexusFileWhenAvailable
from helpers.flatbufferhelpers import _millseconds_to_nanoseconds
from time import sleep
from helpers.ep00.EventType import EventType


def test_ep00(docker_compose):
    producer = create_producer()
    # Push epics connection status
    topic = "TEST-epics-connection-status"
    timestamp = int(docker_compose)
    # Create topic in kafka broker
    publish_ep00_message(producer, topic, EventType.NEVER_CONNECTED, timestamp)
    sleep(10)

    # Start file writing
    send_writer_command(
        "commands/writing-epics-status-command.json",
        producer,
        start_time=docker_compose,
    )
    producer.flush()
    sleep(5)
    publish_ep00_message(
        producer, topic, EventType.CONNECTED, kafka_timestamp=timestamp + 1
    )

    # Give it some time to accumulate data
    sleep(10)

    # Stop file writing
    send_writer_command("commands/stop-command.json", producer)
    sleep(10)
    send_writer_command("commands/writer-exit.json", producer)
    producer.flush()

    filepath = "output-files/output_file_ep00.nxs"
    with OpenNexusFileWhenAvailable(filepath) as file:
        assert file["EpicsConnectionStatus/connection_status_time"][
            0
        ] == _millseconds_to_nanoseconds(timestamp)
        assert file["EpicsConnectionStatus/connection_status"][0] == b"NEVER_CONNECTED"
        assert file["EpicsConnectionStatus/connection_status_time"][
            1
        ] == _millseconds_to_nanoseconds(timestamp + 1)
        assert file["EpicsConnectionStatus/connection_status"][1] == b"CONNECTED"
