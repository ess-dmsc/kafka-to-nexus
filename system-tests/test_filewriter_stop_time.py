from helpers.nexushelpers import OpenNexusFileWhenAvailable
from helpers.kafkahelpers import (
    create_producer,
    send_writer_command,
    consume_everything,
    publish_f142_message,
)
from helpers.timehelpers import unix_time_milliseconds
from time import sleep
from datetime import datetime
import pytest


@pytest.mark.skip(
    reason="Test relies on status reporting which is currently being replaced"
)
def test_filewriter_clears_stop_time_between_jobs(docker_compose_stop_command):
    producer = create_producer()
    sleep(10)
    topic = "TEST_writerCommand"
    send_writer_command(
        "commands/start-command-generic.json",
        producer,
        topic=topic,
        stop_time=int(unix_time_milliseconds(datetime.utcnow())),
        job_id="should_start_then_stop",
        filename="output_file_with_stop_time.nxs",
    )
    sleep(10)
    job_id = send_writer_command(
        "commands/start-command-generic.json",
        producer,
        topic=topic,
        job_id="should_start_but_not_stop",
        filename="output_file_no_stop_time.nxs",
    )
    sleep(10)
    msgs = consume_everything("TEST_writerStatus")

    stopped = False
    started = False
    for message in msgs:
        message = str(message.value(), encoding="utf-8")
        if '"code":"START"' in message and f'"job_id":"{job_id}"' in message:
            started = True
        if '"code":"CLOSE"' in message and f'"job_id":"{job_id}"' in message:
            stopped = True

    assert started
    assert not stopped

    # Clean up by stopping writing
    send_writer_command("commands/stop-command.json", producer, job_id=job_id)
    sleep(10)


@pytest.mark.skip(
    reason="Test relies on status reporting which is currently being replaced"
)
def test_filewriter_can_write_data_when_start_and_stop_time_are_in_the_past(
    docker_compose_stop_command,
):
    producer = create_producer()

    sleep(5)
    data_topics = ["TEST_historicalData1", "TEST_historicalData2"]

    # Publish some data with timestamps in the past(these are from 2019 - 06 - 12)
    for data_topic in data_topics:
        for time_in_ms_after_epoch in range(1_560_330_000_000, 1_560_330_000_200):
            publish_f142_message(producer, data_topic, time_in_ms_after_epoch)

    sleep(5)

    command_topic = "TEST_writerCommand"
    start_time = 1_560_330_000_002
    stop_time = 1_560_330_000_148
    # Ask to write 147 messages from the middle of the 200 messages we published
    send_writer_command(
        "commands/start-command-for-historical-data.json",
        producer,
        topic=command_topic,
        start_time=start_time,
        stop_time=stop_time,
    )
    # The command also includes a stream for topic TEST_emptyTopic which exists but has no data in it, the
    # file writer should recognise there is no data in that topic and close the corresponding streamer without problem.
    filepath = "output-files/output_file_of_historical_data.nxs"
    with OpenNexusFileWhenAvailable(filepath) as file:
        # Expect to have recorded one value per ms between the start and stop time
        # +1 because time range for file writer is inclusive
        assert file["entry/historical_data_1/time"].len() == (
            stop_time - start_time + 1
        ), "Expected there to be one message per millisecond recorded between specified start and stop time"
        assert file["entry/historical_data_2/time"].len() == (
            stop_time - start_time + 1
        ), "Expected there to be one message per millisecond recorded between specified start and stop time"

        assert (
            file["entry/no_data/time"].len() == 0
        ), "Expect there to be no data as the source topic is empty"
