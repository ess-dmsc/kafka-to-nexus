from helpers.kafkahelpers import (
    consume_everything,
    create_producer,
    create_consumer,
    publish_f142_message,
    publish_run_start_message,
    publish_run_stop_message,
)
from helpers.timehelpers import unix_time_milliseconds
from datetime import datetime
import json
from time import sleep
import pytest
from streaming_data_types.status_x5f2 import deserialise_x5f2


def test_ignores_commands_with_incorrect_service_id(docker_compose_multiple_instances):
    producer = create_producer()
    sleep(20)
    service_id_1 = "filewriter1"
    job_id = publish_run_start_message(
        producer,
        "commands/nexus_structure.json",
        nexus_filename="output_file_ignores_stop_1.nxs",
        service_id=service_id_1,
    )
    publish_run_start_message(
        producer,
        "commands/nexus_structure.json",
        nexus_filename="output_file_ignores_stop_2.nxs",
        service_id="filewriter2",
    )

    sleep(10)

    publish_run_stop_message(producer, job_id, service_id=service_id_1)

    consumer = create_consumer()
    consumer.subscribe(["TEST_writerStatus2"])

    # Poll a few times on the status topic to see if the filewriter2 has stopped writing.
    stopped = False

    for i in range(30):
        msg = consumer.poll()
        if b'"file_being_written":""' in msg.value():
            # Filewriter2 is not currently writing a file => stop command has been processed.
            stopped = True
            break
        sleep(1)

    assert stopped

    sleep(5)
    consumer.unsubscribe()
    consumer.subscribe(["TEST_writerStatus1"])
    writer1msg = consumer.poll()

    # Check filewriter1's job queue is not empty
    status_info = deserialise_x5f2(writer1msg.value())
    assert json.loads(str(status_info["status_json"], encoding="utf-8"))["file_being_written"] != ""


def test_ignores_commands_with_incorrect_job_id(docker_compose):
    producer = create_producer()
    sleep(10)

    # Ensure TEST_sampleEnv topic exists
    publish_f142_message(
        producer, "TEST_sampleEnv", int(unix_time_milliseconds(datetime.utcnow()))
    )

    sleep(10)

    # Start file writing
    job_id = publish_run_start_message(
        producer,
        "commands/nexus_structure.json",
        "output_file_jobid.nxs",
        start_time=int(docker_compose),
    )

    sleep(10)

    # Request stop but with slightly wrong job_id
    publish_run_stop_message(
        producer, job_id[:-1],
    )

    msgs = consume_everything("TEST_writerStatus")

    # Poll a few times on the status topic and check the final message read
    # indicates that is still running
    running = False
    for message in msgs:
        status_info = deserialise_x5f2(message.value())
        message = json.loads(str(status_info["status_json"], encoding="utf-8"))
        if message["file_being_written"] == "":
            running = False
        else:
            running = True

    assert running
