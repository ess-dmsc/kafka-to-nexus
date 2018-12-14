from helpers.kafkahelpers import create_producer, send_writer_command, poll_everything
from helpers.timehelpers import unix_time_milliseconds
from time import sleep
from datetime import datetime


def test_filewriter_clears_stop_time(docker_compose_stop_command_does_not_persist):
    producer = create_producer()
    sleep(10)
    topic = "TEST_writerCommand"
    send_writer_command("commands/commandwithstoptime.json", producer, topic=topic, stop_time=str(int(unix_time_milliseconds(datetime.utcnow()))))

    sleep(5)
    send_writer_command("commands/commandwithnostoptime.json", producer, topic=topic)

    sleep(5)
    msgs = poll_everything("TEST_writerStatus")

    stopped = False
    started = False
    for message in msgs:
        message = str(message.value(), encoding='utf-8')
        if "\"code\":\"START\"" in message and "\"job_id\":\"a8e31c99-8df9-4123-8060-2e009d84a0df\"" in message:
            started = True
        if "\"code\":\"CLOSE\"" in message and "\"job_id\":\"a8e31c99-8df9-4123-8060-2e009d84a0df\"" in message:
            stopped = True

    assert started
    assert not stopped
