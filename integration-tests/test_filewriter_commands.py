import time
from datetime import datetime, timedelta
from pathlib import Path
from file_writer_control.CommandStatus import CommandState
from file_writer_control.JobStatus import JobState
from file_writer_control.WriteJob import WriteJob
from helpers import build_relative_file_path, build_absolute_file_path
from helpers.writer import (
    wait_start_job,
    wait_writers_available,
    wait_no_working_writers,
    wait_fail_start_job,
    stop_all_jobs,
)


def test_ignores_stop_command_with_incorrect_service_id(
    request,
    worker_pool,
    kafka_address,
    multiple_writers,
):
    relative_file_path = build_relative_file_path(f"{request.node.name}.nxs")
    absolute_file_path = build_absolute_file_path(relative_file_path)
    wait_writers_available(worker_pool, nr_of=2, timeout=20)
    now = datetime.now()

    with open("commands/nexus_structure.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=relative_file_path,
        broker=kafka_address,
        start_time=now,
        stop_time=now + timedelta(days=30),
    )
    start_cmd_handler = wait_start_job(worker_pool, write_job, timeout=20)

    stop_cmd_handler = worker_pool.try_send_stop_now(
        "incorrect service id", write_job.job_id
    )

    used_timeout = timedelta(seconds=5)
    stop_cmd_handler.set_timeout(used_timeout)

    time.sleep(used_timeout.total_seconds() + 5)
    assert (
        stop_cmd_handler.get_state() == CommandState.TIMEOUT_RESPONSE
    ), f"Stop command not ignored. State was {stop_cmd_handler.get_state()} (cmd id: f{stop_cmd_handler.command_id})"
    assert start_cmd_handler.get_state() in [
        JobState.WRITING
    ], f"Start job may have been affected by Stop command. State was {start_cmd_handler.get_state()} (job id: {start_cmd_handler.job_id}): {start_cmd_handler.get_message()}"

    stop_all_jobs(worker_pool)
    wait_no_working_writers(worker_pool, timeout=10)
    assert Path(absolute_file_path).is_file()


def test_ignores_stop_command_with_incorrect_job_id(
    request,
    worker_pool,
    kafka_address,
    multiple_writers,
):
    relative_file_path = build_relative_file_path(f"{request.node.name}.nxs")
    absolute_file_path = build_absolute_file_path(relative_file_path)
    wait_writers_available(worker_pool, nr_of=2, timeout=20)
    now = datetime.now()

    with open("commands/nexus_structure.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=relative_file_path,
        broker=kafka_address,
        start_time=now,
        stop_time=now + timedelta(days=30),
    )
    start_cmd_handler = wait_start_job(worker_pool, write_job, timeout=20)

    cmd_handler = worker_pool.try_send_stop_now(write_job.service_id, "wrong job id")
    used_timeout = timedelta(seconds=5)
    cmd_handler.set_timeout(used_timeout)

    time.sleep(used_timeout.total_seconds() + 5)
    assert start_cmd_handler.get_state() in [
        JobState.WRITING
    ], f"Start job may have been affected by Stop command. State was {start_cmd_handler.get_state()} (job id: {start_cmd_handler.job_id}): {start_cmd_handler.get_message()}"

    stop_all_jobs(worker_pool)
    wait_no_working_writers(worker_pool, timeout=10)
    assert Path(absolute_file_path).is_file()


def test_accepts_stop_command_with_empty_service_id(
    request,
    worker_pool,
    kafka_address,
    multiple_writers,
):
    relative_file_path = build_relative_file_path(f"{request.node.name}.nxs")
    absolute_file_path = build_absolute_file_path(relative_file_path)
    wait_writers_available(worker_pool, nr_of=2, timeout=20)
    now = datetime.now()

    with open("commands/nexus_structure.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=relative_file_path,
        broker=kafka_address,
        start_time=now,
        stop_time=now + timedelta(days=30),
    )
    start_cmd_handler = wait_start_job(worker_pool, write_job, timeout=20)

    stop_cmd_handler = worker_pool.try_send_stop_now(None, write_job.job_id)

    used_timeout = timedelta(seconds=5)
    stop_cmd_handler.set_timeout(used_timeout)

    time.sleep(used_timeout.total_seconds() + 5)
    start_job_state = start_cmd_handler.get_state()
    assert start_job_state in [
        JobState.DONE
    ], f"Start job was not stopped after Stop command. State was {start_job_state} (job id: {start_cmd_handler.job_id}): {start_cmd_handler.get_message()}"

    wait_no_working_writers(worker_pool, timeout=15)
    assert Path(absolute_file_path).is_file()


def test_ignores_start_command_with_incorrect_job_id(
    request, worker_pool, kafka_address
):
    relative_file_path = build_relative_file_path(f"{request.node.name}.nxs")
    absolute_file_path = build_absolute_file_path(relative_file_path)
    wait_writers_available(worker_pool, nr_of=1, timeout=10)
    now = datetime.now()
    with open("commands/nexus_structure.json", "r") as f:
        structure = f.read()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=relative_file_path,
        broker=kafka_address,
        start_time=now,
        stop_time=now + timedelta(days=30),
    )
    write_job.job_id = "invalid id"
    wait_fail_start_job(worker_pool, write_job, timeout=20)

    wait_no_working_writers(worker_pool, timeout=10)
    assert not Path(absolute_file_path).is_file()


def test_reject_bad_json(request, worker_pool, kafka_address):
    relative_file_path = build_relative_file_path(f"{request.node.name}.nxs")
    absolute_file_path = build_absolute_file_path(relative_file_path)
    wait_writers_available(worker_pool, nr_of=1, timeout=10)
    now = datetime.now()
    start_time = now - timedelta(seconds=10)
    stop_time = now
    with open("commands/nexus_structure_bad_json.json", "r") as f:
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
