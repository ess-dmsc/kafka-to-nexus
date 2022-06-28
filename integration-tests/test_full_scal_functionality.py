from helpers.nexushelpers import OpenNexusFile
from helpers.kafkahelpers import (
    create_producer,
    publish_scal_message,
    publish_pvAl_message,
    AlarmState,
    AlarmSeverity,
    publish_pvCn_message,
    ConnectionInfo,
)
from datetime import datetime, timedelta
from file_writer_control.WriteJob import WriteJob
from helpers import full_file_path
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
)
import numpy as np


def test_scal(worker_pool, kafka_address, hdf_file_name="scal_output_file.nxs"):
    file_path = full_file_path(hdf_file_name)
    wait_writers_available(worker_pool, nr_of=1, timeout=20)
    producer = create_producer(kafka_address)

    data_topic = "TEST_scalData"
    source_name = "someSource"

    start_time = datetime(year=2020, month=6, day=12, hour=11, minute=1, second=35)
    step_time = timedelta(seconds=10)
    alarm_state = AlarmState.DEVICE
    alarm_severity = AlarmSeverity.MAJOR
    publish_pvAl_message(
        producer,
        topic=data_topic,
        timestamp=start_time + step_time,
        state=alarm_state,
        severity=alarm_severity,
    )
    connection_status = ConnectionInfo.REMOTE_ERROR
    publish_pvCn_message(
        producer,
        topic=data_topic,
        timestamp=start_time + step_time,
        status=connection_status,
    )
    Min = 5
    Mean = 10
    Max = 15
    values = (Min, Mean, Max)
    for i, c_value in enumerate(values):
        publish_scal_message(
            producer,
            data_topic,
            timestamp=start_time + step_time * (i + 1),
            value=c_value,
            source_name=source_name,
        )

    stop_time = start_time + timedelta(seconds=148)
    with open("commands/nexus_structure_scal.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_path,
        broker=kafka_address,
        start_time=start_time,
        stop_time=stop_time,
    )
    wait_start_job(worker_pool, write_job, timeout=20)

    wait_no_working_writers(worker_pool, timeout=30)

    with OpenNexusFile(file_path) as file:
        assert file["entry/scal_data/minimum_value"][0] == Min
        assert file["entry/scal_data/maximum_value"][0] == Max
        assert file["entry/scal_data/average_value"][0] == Mean
        assert (file["entry/scal_data/value"][:].flatten() == np.array(values)).all()
        assert file["entry/scal_data/alarm_status"][0] == str(alarm_state)
        assert file["entry/scal_data/alarm_severity"][0] == str(alarm_severity)
        assert (
            file["entry/scal_data/connection_status_time"][0]
            == file["entry/scal_data/alarm_time"][0]
        )
        assert file["entry/scal_data/time"][0] == file["entry/scal_data/alarm_time"][0]
