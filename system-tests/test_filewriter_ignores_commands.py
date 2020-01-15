from helpers.kafkahelpers import create_producer, send_writer_command, create_consumer
from time import sleep
import pytest


@pytest.mark.skip(reason="Test relies on status reporting which is currently being replaced")
def test_ignores_commands_with_incorrect_service_id(docker_compose_multiple_instances):
    producer = create_producer()
    sleep(20)
    job_id = send_writer_command(
        "commands/start-command-generic.json",
        producer,
        service_id="filewriter1",
        filename="output_file_ignores_stop_1.nxs",
    )
    send_writer_command(
        "commands/start-command-generic.json",
        producer,
        service_id="filewriter2",
        filename="output_file_ignores_stop_2.nxs",
    )

    sleep(10)

    send_writer_command(
        "commands/stop-command.json", producer, job_id=job_id, service_id="filewriter1"
    )

    consumer = create_consumer()
    consumer.subscribe(["TEST_writerStatus2"])

    # poll a few times on the status topic to see if the filewriter2 has stopped writing files.
    stopped = False

    for i in range(30):
        msg = consumer.poll()
        if b'"files":{}' in msg.value():
            # filewriter2 is not currently writing a file - stop command has been processed.
            stopped = True
            break
        sleep(1)

    assert stopped

    sleep(5)
    consumer.unsubscribe()
    consumer.subscribe(["TEST_writerStatus1"])
    writer1msg = consumer.poll()

    # Check filewriter1's job queue is not empty
    assert b'"files":{}' not in writer1msg.value()
