import time
from datetime import datetime, timedelta
from pathlib import Path
from file_writer_control.CommandStatus import CommandState
from file_writer_control.WriteJob import WriteJob
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
    wait_fail_start_job,
    stop_all_jobs,
)


def test_ignores_commands_with_incorrect_id(
    writer_channel, worker_pool, kafka_address, multiple_writers
):
    wait_writers_available(writer_channel, nr_of=2, timeout=10)
    now = datetime.now()
    file_name = "output_file_stop_id.nxs"
    with open("commands/nexus_structure.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_name,
        broker=kafka_address,
        start_time=now,
        stop_time=now + timedelta(days=30),
    )
    wait_start_job(worker_pool, write_job, timeout=20)

    cmd_handler = writer_channel.try_send_stop_now(
        "incorrect service id", write_job.job_id
    )

    used_timeout = timedelta(seconds=5)
    cmd_handler.set_timeout(used_timeout)

    time.sleep(used_timeout.total_seconds() + 2)
    assert (
        cmd_handler.get_state() == CommandState.TIMEOUT_RESPONSE
    ), f"State was {cmd_handler.get_state()} (cmd id: f{cmd_handler.command_id})"

    cmd_handler = writer_channel.try_send_stop_now(write_job.service_id, "wrong job id")
    cmd_handler.set_timeout(used_timeout)

    time.sleep(used_timeout.total_seconds() + 2)
    assert (
            cmd_handler.get_state() == CommandState.TIMEOUT_RESPONSE
    ), f"State was {cmd_handler.get_state()} (cmd id: f{cmd_handler.command_id})"

    stop_all_jobs(writer_channel)
    wait_no_working_writers(writer_channel, timeout=0)
    file_path = f"output-files/{file_name}"
    assert Path(file_path).is_file()


def test_ignores_commands_with_incorrect_job_id(
    writer_channel, worker_pool, kafka_address
):
    wait_writers_available(writer_channel, nr_of=1, timeout=10)
    now = datetime.now()
    file_name = "output_file_job_id.nxs"
    with open("commands/nexus_structure.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_name,
        broker=kafka_address,
        start_time=now,
        stop_time=now + timedelta(days=30),
    )
    write_job.job_id = "invalid id"
    wait_fail_start_job(worker_pool, write_job, timeout=20)

    wait_no_working_writers(writer_channel, timeout=0)
    file_path = f"output-files/{file_name}"
    assert not Path(file_path).is_file()
