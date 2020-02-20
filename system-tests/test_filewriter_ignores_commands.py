from helpers.kafkahelpers import create_producer, create_consumer, publish_run_start_message, publish_run_stop_message
from time import sleep
import pytest


def test_ignores_commands_with_incorrect_service_id(docker_compose_multiple_instances):
    producer = create_producer()
    sleep(20)
    service_id_1 = "filewriter1"
    job_id = publish_run_start_message(producer, "commands/nexus_structure.json",
                                       nexus_filename="output_file_ignores_stop_1.nxs", service_id=service_id_1)
    publish_run_start_message(producer, "commands/nexus_structure.json",
                              nexus_filename="output_file_ignores_stop_2.nxs", service_id="filewriter2")

    sleep(10)

    publish_run_stop_message(producer, job_id, service_id=service_id_1)

    consumer = create_consumer()
    consumer.subscribe(["TEST_writerStatus2"])

    # poll a few times on the status topic to see if the filewriter2 has stopped writing files.
    stopped = False

    for i in range(30):
        msg = consumer.poll()
        if b'"file_being_written":""' in msg.value():
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
    assert b'"file_being_written":""' not in writer1msg.value()
