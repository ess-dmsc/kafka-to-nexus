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


pytest.skip("This behaviour is not implemented currently, ")
def test_ignores_commands_with_incorrect_service_id(docker_compose_multiple_instances):
    producer = create_producer()
    sleep(20)
    service_id_1 = "filewriter1"
    service_id_2 = "filewriter2"
    command_topic = "TEST_writerCommandMultiple"
    job_id = publish_run_start_message(
        producer,
        "commands/nexus_structure.json",
        nexus_filename="output_file_ignores_stop_1.nxs",
        topic=command_topic,
        service_id=service_id_1,
    )
    publish_run_start_message(
        producer,
        "commands/nexus_structure.json",
        nexus_filename="output_file_ignores_stop_2.nxs",
        topic=command_topic,
        service_id=service_id_2,
    )

    sleep(10)

    publish_run_stop_message(producer, job_id, topic=command_topic, service_id=service_id_2)

    consumer = create_consumer()
    consumer.subscribe(["TEST_writerStatus2"])

    # Poll a few times on the status topic to see if the filewriter2 has stopped writing.
    stopped = False
    maximum_tries = 30
    for i in range(maximum_tries):
        msg = consumer.poll()
        if msg is None or msg.error():
            continue
        status_info = deserialise_x5f2(msg.value())
        if json.loads(status_info.status_json)["file_being_written"] == "":
            # Filewriter2 is not currently writing a file => stop command has been processed.
            stopped = True
            break
        if i == maximum_tries - 1:
            pytest.fail("filewriter2 failed to stop after being sent stop message")
        sleep(1)

    assert stopped

    sleep(5)
    consumer.unsubscribe()
    consumer.subscribe(["TEST_writerStatus1"])
    writer1msg = consumer.poll()

    # Check filewriter1's job queue is not empty
    status_info = deserialise_x5f2(writer1msg.value())
    assert json.loads(json.loads(status_info.status_json))["file_being_written"] != ""


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
    message = msgs[-1]
    status_info = deserialise_x5f2(message.value())
    message = json.loads(status_info.status_json)
    if message["file_being_written"] == "":
        running = False
    else:
        running = True

    assert running
