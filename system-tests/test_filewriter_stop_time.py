from helpers.kafkahelpers import create_producer, send_writer_command, consume_everything, publish_f142_message
from helpers.nexushelpers import OpenNexusFileWhenAvailable
from helpers.timehelpers import unix_time_milliseconds
from time import sleep
from datetime import datetime


def test_filewriter_clears_stop_time_between_jobs(docker_compose_stop_command):
    producer = create_producer()
    sleep(10)
    topic = "TEST_writerCommand"
    send_writer_command("commands/commandwithstoptime.json", producer, topic=topic,
                        stop_time=str(int(unix_time_milliseconds(datetime.utcnow()))))

    sleep(10)
    send_writer_command("commands/commandwithnostoptime.json", producer, topic=topic)

    sleep(10)
    msgs = consume_everything("TEST_writerStatus")

    stopped = False
    started = False
    for message in msgs:
        message = str(message.value(), encoding='utf-8')
        if "\"code\":\"START\"" in message and \
                "\"job_id\":\"a8e31c99-8df9-4123-8060-2e009d84a0df\"" in message:
            started = True
        if "\"code\":\"CLOSE\"" in message and \
                "\"job_id\":\"a8e31c99-8df9-4123-8060-2e009d84a0df\"" in message:
            stopped = True

    assert started
    assert not stopped


def test_filewriter_can_write_data_when_start_and_stop_time_are_in_the_past(docker_compose_stop_command):
    producer = create_producer()

    sleep(5)
    data_topic = "TEST_historicalData"

    # Publish some data with timestamps in the past(these are from 2019 - 06 - 12)
    for time_in_ms_after_epoch in range(1560330000000, 1560330000200):
        publish_f142_message(producer, data_topic, time_in_ms_after_epoch)

    sleep(5)

    command_topic = "TEST_writerCommand"
    start_time = 1560330000002
    stop_time = 1560330000148
    # Ask to write 196 messages from the middle of the 200 messages we published
    send_writer_command("commands/command-write-historical-data.json", producer, topic=command_topic,
                        start_time=str(start_time),
                        stop_time=str(stop_time))

    sleep(5)
    send_writer_command("commands/writer-exit.json", producer)
    filepath = "output-files/output_file_of_historical_data.nxs"
    with OpenNexusFileWhenAvailable(filepath) as file:
        # +1 because time range for file writer is inclusive
        assert file["entry/historical_data/time"].len() == (stop_time - start_time + 1), \
            "Expected there to be one message per millisecond recorded between specified start and stop time"
