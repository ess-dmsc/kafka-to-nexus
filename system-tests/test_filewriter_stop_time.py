from helpers.nexushelpers import OpenNexusFileWhenAvailable
from helpers.kafkahelpers import (
    create_producer,
    publish_run_start_message,
    publish_run_stop_message,
    consume_everything,
    publish_f142_message,
)
from helpers.timehelpers import unix_time_milliseconds
from time import sleep
from datetime import datetime
import json
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import AlarmStatus
from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity import AlarmSeverity
from streaming_data_types.status_x5f2 import deserialise_x5f2
import pytest


def check(condition, fail_string):
    if not condition:
        pytest.fail(fail_string)


def test_filewriter_clears_stop_time_between_jobs(docker_compose_stop_command):
    producer = create_producer()
    start_time = unix_time_milliseconds(datetime.utcnow()) - 1000
    stop_time = start_time + 1000
    # Ensure TEST_sampleEnv topic exists
    publish_f142_message(
        producer, "TEST_sampleEnv", int(unix_time_milliseconds(datetime.utcnow()))
    )
    check(producer.flush(5) == 0, "Unable to flush kafka messages.")

    topic = "TEST_writerCommand"
    publish_run_start_message(
        producer,
        "commands/nexus_structure.json",
        "output_file_with_stop_time.nxs",
        topic=topic,
        job_id="should_start_then_stop",
        start_time=int(start_time),
        stop_time=int(stop_time),
    )
    check(producer.flush(5) == 0, "Unable to flush kafka messages.")
    sleep(30)
    job_id = publish_run_start_message(
        producer,
        "commands/nexus_structure.json",
        "output_file_no_stop_time.nxs",
        topic=topic,
        job_id="should_start_but_not_stop",
    )
    check(producer.flush(5) == 0, "Unable to flush kafka messages.")
    sleep(30)
    msgs = consume_everything("TEST_writerStatus")

    stopped = False
    started = False
    message = msgs[-1]
    status_info = deserialise_x5f2(message.value())
    message = json.loads(status_info.status_json)
    if message["start_time"] > 0 and message["job_id"] == job_id:
        started = True
    if message["stop_time"] == 0 and message["job_id"] == "":
        stopped = True

    assert started
    assert not stopped

    # Clean up by stopping writing
    publish_run_stop_message(producer, job_id=job_id)
    check(producer.flush(5) == 0, "Unable to flush kafka messages.")
    sleep(3)


def test_filewriter_can_write_data_when_start_and_stop_time_are_in_the_past(
    docker_compose_stop_command,
):
    producer = create_producer()

    data_topics = ["TEST_historicalData1", "TEST_historicalData2"]

    first_alarm_change_time_ms = 1_560_330_000_050
    second_alarm_change_time_ms = 1_560_330_000_060

    # Publish some data with timestamps in the past(these are from 2019 - 06 - 12)
    for data_topic in data_topics:
        for time_in_ms_after_epoch in range(1_560_330_000_000, 1_560_330_000_200):
            if time_in_ms_after_epoch == first_alarm_change_time_ms:
                # EPICS alarm goes into HIGH state
                publish_f142_message(
                    producer,
                    data_topic,
                    time_in_ms_after_epoch,
                    alarm_status=AlarmStatus.HIGH,
                    alarm_severity=AlarmSeverity.MAJOR,
                )
            elif time_in_ms_after_epoch == second_alarm_change_time_ms:
                # EPICS alarm returns to NO_ALARM
                publish_f142_message(
                    producer,
                    data_topic,
                    time_in_ms_after_epoch,
                    alarm_status=AlarmStatus.NO_ALARM,
                    alarm_severity=AlarmSeverity.NO_ALARM,
                )
            else:
                publish_f142_message(producer, data_topic, time_in_ms_after_epoch)
    check(producer.flush(5) == 0, "Unable to flush kafka messages.")
    sleep(5)

    command_topic = "TEST_writerCommand"
    start_time = 1_560_330_000_002
    stop_time = 1_560_330_000_148
    # Ask to write 147 messages from the middle of the 200 messages we published
    publish_run_start_message(
        producer,
        "commands/nexus_structure_historical.json",
        "output_file_of_historical_data.nxs",
        start_time=start_time,
        stop_time=stop_time,
        topic=command_topic,
    )

    sleep(20)
    # The command also includes a stream for topic TEST_emptyTopic which exists but has no data in it, the
    # file writer should recognise there is no data in that topic and close the corresponding streamer without problem.
    filepath = "output-files/output_file_of_historical_data.nxs"
    with OpenNexusFileWhenAvailable(filepath) as file:
        # Expect to have recorded one value per ms between the start and stop time
        # +3 due to writing one message before start and one message after stop
        assert file["entry/historical_data_1/time"].len() == (
            stop_time - start_time + 3
        ), "Expected there to be one message per millisecond recorded between specified start and stop time"
        assert file["entry/historical_data_2/time"].len() == (
            stop_time - start_time + 3
        ), "Expected there to be one message per millisecond recorded between specified start and stop time"

        # EPICS alarms
        assert (
            file["entry/historical_data_1/alarm_status"].len() == 2
        ), "Expected there to have record two changes in EPICS alarm status"
        assert (
            file["entry/historical_data_1/alarm_severity"].len() == 2
        ), "Expected there to have record two changes in EPICS alarm status"
        # First alarm change
        assert file["entry/historical_data_1/alarm_status"][0] == b"HIGH"
        assert file["entry/historical_data_1/alarm_severity"][0] == b"MAJOR"
        assert (
            file["entry/historical_data_1/alarm_time"][0]
            == first_alarm_change_time_ms * 1000000
        )  # ns
        # Second alarm change
        assert file["entry/historical_data_1/alarm_status"][1] == b"NO_ALARM"
        assert file["entry/historical_data_1/alarm_severity"][1] == b"NO_ALARM"
        assert (
            file["entry/historical_data_1/alarm_time"][1]
            == second_alarm_change_time_ms * 1000000
        )  # ns

        assert (
            file["entry/no_data/time"].len() == 0
        ), "Expect there to be no data as the source topic is empty"


def test_filewriter_stops_when_only_receives_periodic_updates_from_forwarder(
    docker_compose_stop_command,
):
    producer = create_producer()

    data_topic = "TEST_sampleEnv"

    command_topic = "TEST_writerCommand"

    start_time = unix_time_milliseconds(datetime.utcnow())
    stop_time = start_time + 10000

    # Publish some data with repeat timestamps from just before start of run
    timestamp_of_last_pv_value_change = int(start_time) - 100
    for time_in_ms_after_epoch in (
        timestamp_of_last_pv_value_change,
        timestamp_of_last_pv_value_change,
        timestamp_of_last_pv_value_change,
    ):
        publish_f142_message(
            producer, data_topic, time_in_ms_after_epoch, source_name="epics_thing"
        )

    output_file = "output_file_periodic_data.nxs"
    publish_run_start_message(
        producer,
        "commands/periodic_update.json",
        output_file,
        start_time=int(start_time),
        stop_time=int(stop_time),
        topic=command_topic,
    )

    sleep(20)

    filepath = f"output-files/{output_file}"
    with OpenNexusFileWhenAvailable(filepath) as file:
        assert file["entry/EPICS_device/time"].len() == 1
