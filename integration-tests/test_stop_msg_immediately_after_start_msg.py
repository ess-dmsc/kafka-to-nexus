from datetime import datetime, timedelta

from file_writer_control.WorkerJobPool import WorkerJobPool
from file_writer_control.WriteJob import WriteJob
from file_writer_control.JobHandler import JobHandler
from helpers.nexushelpers import OpenNexusFile
from helpers import build_relative_file_path
from helpers.writer import (
    wait_command_is_done,
    wait_writers_available,
    wait_no_working_writers,
)


def test_stop_msg_immediately_after_start_msg_on_alternative_command_topic(
    worker_pool, kafka_address
):
    file_path = build_relative_file_path(
        f"output_file_stop_msg_immediately_after_start_msg_on_alternative_command_topic.nxs"
    )
    wait_writers_available(worker_pool, nr_of=1, timeout=10)
    now = datetime.now()
    with open("commands/nexus_structure_static.json", "r") as f:
        structure = f.read()

    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_path,
        broker=kafka_address,
        start_time=now,
        control_topic="TEST_writer_commands_alternative",
    )
    # ECDC-3333 file-writer-control does not correctly handle jobs with
    # alternative command topics. For now we create a secondary WorkerJobPool
    # to do so.
    worker_pool_alternative = WorkerJobPool(
        job_topic_url=f"{kafka_address}/TEST_writer_jobs",
        command_topic_url=f"{kafka_address}/TEST_writer_commands_alternative",
        max_message_size=1048576 * 500,
    )
    job_handler = JobHandler(worker_finder=worker_pool)
    job_handler_alternative = JobHandler(
        worker_finder=worker_pool_alternative, job_id=write_job.job_id
    )
    start_handler = job_handler.start_job(write_job)
    stop_handler = job_handler_alternative.set_stop_time(now)

    # wait_command_is_done(start_handler, 10)  # disabled: file-writer-control does not support topic switching
    wait_command_is_done(stop_handler, 15)
    wait_no_working_writers(worker_pool, timeout=10)
    with OpenNexusFile(file_path) as file:
        assert not file.swmr_mode
        assert file["entry/start_time"][()][0].decode("utf-8") == "2016-04-12T02:58:52"
