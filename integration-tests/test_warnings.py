from datetime import datetime, timedelta
from file_writer_control import WriteJob
from helpers import build_relative_file_path
from helpers.writer import (
    wait_writers_available,
    wait_fail_start_job,
)


def test_int_vector_as_string(request, worker_pool, kafka_address):
    relative_file_path = build_relative_file_path(f"{request.node.name}.nxs")
    wait_writers_available(worker_pool, nr_of=1, timeout=10)
    now = datetime.now()
    start_time = now - timedelta(seconds=10)
    stop_time = now
    with open("commands/nexus_structure_warnings.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=relative_file_path,
        broker=kafka_address,
        start_time=start_time,
        stop_time=stop_time,
    )
    fail_message = wait_fail_start_job(worker_pool, write_job, timeout=20)
    assert "NeXus structure JSON" in fail_message, (
        'Unexpected content in "fail to start" message. Message was: ' + fail_message
    )
