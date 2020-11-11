import time
from datetime import datetime, timedelta
from pathlib import Path
from file_writer_control.CommandStatus import CommandState
from file_writer_control.WriteJob import WriteJob
from file_writer_control.InThreadStatusTracker import COMMAND_STATUS_TIMEOUT
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
    wait_fail_start_job,
    stop_all_jobs,
)


def test_ignores_commands_with_incorrect_id(writer_channel):
    wait_writers_available(writer_channel, nr_of=2, timeout=10)
    now = datetime.now()
    file_name = "output_file_stop_id.nxs"
    with open("commands/nexus_structure.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_name,
        broker="localhost:9092",
        start_time=now,
        stop_time=now + timedelta(days=30),
    )
    wait_start_job(writer_channel, write_job, timeout=20)

    cmd_handler = writer_channel.try_send_stop_now(
        "incorrect service id", write_job.job_id
    )

    time.sleep(COMMAND_STATUS_TIMEOUT.total_seconds() + 2)
    assert cmd_handler.get_state() == CommandState.TIMEOUT_RESPONSE

    cmd_handler = writer_channel.try_send_stop_now(write_job.service_id, "wrong job id")
    start_time = datetime.now()
    timeout = timedelta(seconds=10)
    while True:
        if cmd_handler.get_state() == CommandState.ERROR:
            break
        if start_time + timeout < datetime.now():
            assert False
        time.sleep(1)

    stop_all_jobs(writer_channel)
    wait_no_working_writers(writer_channel, timeout=0)
    file_path = f"output-files/{file_name}"
    assert Path(file_path).is_file()


def test_ignores_commands_with_incorrect_job_id(writer_channel):
    wait_writers_available(writer_channel, nr_of=1, timeout=10)
    now = datetime.now()
    file_name = "output_file_job_id.nxs"
    with open("commands/nexus_structure.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_name,
        broker="localhost:9092",
        start_time=now,
        stop_time=now + timedelta(days=30),
    )
    write_job.job_id = "invalid id"
    wait_fail_start_job(writer_channel, write_job, timeout=20)

    wait_no_working_writers(writer_channel, timeout=0)
    file_path = f"output-files/{file_name}"
    assert not Path(file_path).is_file()
