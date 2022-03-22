from helpers.nexushelpers import OpenNexusFile
from helpers.kafkahelpers import (
    create_producer,
    publish_f142_message,
)
from time import sleep
from datetime import datetime, timedelta
import json
from streaming_data_types.fbschemas.logdata_f142.AlarmStatus import AlarmStatus
from streaming_data_types.fbschemas.logdata_f142.AlarmSeverity import AlarmSeverity
import pytest
from file_writer_control.WriteJob import WriteJob
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
)
import numpy as np


def test_f142_meta_data(worker_pool, kafka_address, file_name = "output_file_with_meta_data.nxs"):
    wait_writers_available(worker_pool, nr_of=1, timeout=20)
    producer = create_producer()

    data_topic = "TEST_sampleEnv"
    source_name1 = "someSource1"
    source_name2 = "someSource2"

    start_time = datetime(year=2020, month=6, day=12, hour=11, minute=1, second=35)
    stop_time = start_time + timedelta(seconds=200)
    step_time = timedelta(seconds=1)
    Min = 5
    Mean = 10
    Max = 15
    publish_f142_message(
        producer,
        data_topic,
        start_time + step_time * 1,
        value=Min,
        source_name=source_name1,
    )
    publish_f142_message(
        producer,
        data_topic,
        start_time + step_time * 2,
        value=Mean,
        source_name=source_name1,
    )
    publish_f142_message(
        producer,
        data_topic,
        start_time + step_time * 3,
        value=Max,
        source_name=source_name1,
    )

    Array1 = np.array([4, 6, 2])
    Array2 = np.array([32, -5, 3])
    Array3 = np.array([0, 1, 0])
    publish_f142_message(
        producer,
        data_topic,
        start_time + step_time * 4,
        value=Array1,
        source_name=source_name2,
    )
    publish_f142_message(
        producer,
        data_topic,
        start_time + step_time * 5,
        value=Array2,
        source_name=source_name2,
    )
    publish_f142_message(
        producer, data_topic, stop_time, value=Array3, source_name=source_name2
    )

    file_start_time = start_time
    file_stop_time = start_time + timedelta(seconds=148)
    with open("commands/nexus_structure_meta_data.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_name,
        broker=kafka_address,
        start_time=file_start_time,
        stop_time=file_stop_time,
    )
    wait_start_job(worker_pool, write_job, timeout=20)

    wait_no_working_writers(worker_pool, timeout=30)

    # The command also includes a stream for topic TEST_emptyTopic which exists but has no data in it, the
    # file writer should recognise there is no data in that topic and close the corresponding streamer without problem.
    file_path = f"output-files/{file_name}"
    with OpenNexusFile(file_path) as file:
        assert file["entry/meta_data_1/minimum_value"][0] == Min
        assert file["entry/meta_data_1/maximum_value"][0] == Max
        assert file["entry/meta_data_1/average_value"][0] == Mean

        FullArr = np.concatenate([Array1, Array2, Array3])

        assert file["entry/meta_data_2/minimum_value"][0] == FullArr.min()
        assert file["entry/meta_data_2/maximum_value"][0] == FullArr.max()
        assert file["entry/meta_data_2/average_value"][0] == FullArr.mean()
