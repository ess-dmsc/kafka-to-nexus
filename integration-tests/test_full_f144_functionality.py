from helpers.nexushelpers import OpenNexusFile
from helpers.kafkahelpers import (
    create_producer,
    publish_f144_message,
    publish_al00_message,
    Severity,
    publish_ep01_message,
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


def test_f144(worker_pool, kafka_address, hdf_file_name="scal_output_file.nxs"):
    file_path = full_file_path(hdf_file_name)
    wait_writers_available(worker_pool, nr_of=1, timeout=20)
    producer = create_producer(kafka_address)

    data_topic = "TEST_scalData"
    source_name = "someSource"

    start_time = datetime(year=2020, month=6, day=12, hour=11, minute=1, second=35)
    step_time = timedelta(seconds=10)
    alarm_severity = Severity.MAJOR
    alarm_msg = "Some alarm message you might find informative."
    publish_al00_message(
        producer,
        topic=data_topic,
        timestamp=start_time + step_time,
        severity=alarm_severity,
        alarm_msg=alarm_msg,
        source_name=source_name,
    )
    connection_status = ConnectionInfo.REMOTE_ERROR
    publish_ep01_message(
        producer,
        topic=data_topic,
        timestamp=start_time + step_time,
        status=connection_status,
        source_name=source_name,
    )
    Min = 5
    Mean = 10
    Max = 15
    values = (Min, Mean, Max)
    for i, c_value in enumerate(values):
        publish_f144_message(
            producer,
            data_topic,
            timestamp=start_time + step_time * (i + 1),
            value=c_value,
            source_name=source_name,
        )

    stop_time = start_time + timedelta(seconds=148)
    with open("commands/nexus_structure_f144.json", "r") as f:
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

    connection_map = {
        getattr(ConnectionInfo, a): a for a in dir(ConnectionInfo) if a[0:2] != "__"
    }
    severity_map = {getattr(Severity, a): a for a in dir(Severity) if a[0:2] != "__"}

    with OpenNexusFile(file_path) as file:
        assert file["entry/scal_data/minimum_value"][0] == Min
        assert file["entry/scal_data/maximum_value"][0] == Max
        assert file["entry/scal_data/average_value"][0] == Mean
        assert file["entry/scal_data/alarm_message"][0].decode() == alarm_msg
        assert (file["entry/scal_data/value"][:].flatten() == np.array(values)).all()
        assert (
            file["entry/scal_data/alarm_severity"][0].decode()
            == severity_map[alarm_severity]
        )
        assert (
            file["entry/scal_data/connection_status"][0].decode()
            == connection_map[connection_status]
        )
        assert (
            file["entry/scal_data/connection_status_time"][0]
            == file["entry/scal_data/alarm_time"][0]
        )
        assert file["entry/scal_data/time"][0] == file["entry/scal_data/alarm_time"][0]