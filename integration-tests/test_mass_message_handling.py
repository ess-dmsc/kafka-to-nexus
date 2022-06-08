from helpers.nexushelpers import OpenNexusFile
from helpers.kafkahelpers import (
    create_producer,
    publish_f142_message,
)
import pytest
from datetime import datetime, timedelta
from file_writer_control.WriteJob import WriteJob
from helpers import full_file_path
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
)


def create_messages(kafka_address, start_time, stop_time, step_time):
    producer = create_producer(kafka_address)
    data_topic = "TEST_massAmountOfMessages"
    current_time = start_time
    while current_time < stop_time:
        try:
            publish_f142_message(producer, data_topic, current_time, flush=False)
        except BufferError:
            producer.flush()
            publish_f142_message(producer, data_topic, current_time, flush=False)
        current_time += step_time
    producer.flush()


# @pytest.mark.skip(reason="This test needs refinement before it is activated.")
def test_mass_message_handling(
    worker_pool, kafka_address, hdf_file_name="write_from_mass_message_topic.nxs"
):
    file_path = full_file_path(hdf_file_name)
    wait_writers_available(worker_pool, nr_of=1, timeout=20)

    start_time = datetime(year=2019, month=6, day=12, hour=11, minute=1, second=35)
    stop_time = start_time + timedelta(days=365 * 2)
    step_time = timedelta(seconds=1)

    create_messages(kafka_address, start_time, stop_time, step_time)

    file_start_time = start_time + timedelta(days=365)
    file_stop_time = file_start_time + timedelta(seconds=148)
    with open("commands/nexus_structure_mass_messages.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_path,
        broker=kafka_address,
        start_time=file_start_time,
        stop_time=file_stop_time,
    )
    wait_start_job(worker_pool, write_job, timeout=20)

    wait_no_working_writers(worker_pool, timeout=30)

    with OpenNexusFile(file_path) as file:
        # Expect to have recorded one value per ms between the start and stop time
        # +3 due to writing one message before start and one message after stop
        expected_elements = (file_stop_time - file_start_time) // step_time + 3
        assert file["entry/write_from_mass_messages/time"].len() == (
            expected_elements
        ), "Expected there to be one message per second recorded between specified start and stop time"


if __name__ == "__main__":
    begin_time = datetime(year=2019, month=6, day=12, hour=11, minute=1, second=35)
    end_time = begin_time + timedelta(days=20)
    stp_time = timedelta(seconds=1)
    create_messages("localhost:9093", begin_time, end_time, stp_time)
