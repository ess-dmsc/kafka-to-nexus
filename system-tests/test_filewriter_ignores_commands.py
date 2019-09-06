from helpers.kafkahelpers import create_producer, send_writer_command, create_consumer
from time import sleep


def test_ignores_commands_with_incorrect_id(docker_compose_multiple_instances):
    producer = create_producer()
    sleep(20)
    send_writer_command("commands/add-command-never-ends.json", producer)
    send_writer_command("commands/add-command-never-ends2.json", producer)

    sleep(10)

    send_writer_command("commands/writer-stop-single.json", producer)

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
