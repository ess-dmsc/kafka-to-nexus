from file_writer_control.WorkerCommandChannel import WorkerCommandChannel
from file_writer_control.WorkerStatus import WorkerState
from file_writer_control.JobStatus import JobState
from file_writer_control.CommandStatus import CommandState
from file_writer_control.WriteJob import WriteJob
from file_writer_control.JobHandler import JobHandler
from datetime import datetime, timedelta
import time


def wait_writers_available(
    worker_command: WorkerCommandChannel, timeout: float, nr_of: int
):
    start_time = datetime.now()
    while True:
        workers = worker_command.get_idle_workers()
        if len(workers) >= nr_of:
            return
        time.sleep(1.0)
        if start_time + timedelta(seconds=timeout) < datetime.now():
            raise RuntimeError(f"Timed out when waiting for {nr_of} idle workers")


def wait_no_working_writers(worker_command: WorkerCommandChannel, timeout: float):
    start_time = datetime.now()
    while True:
        workers = worker_command.list_known_workers()
        if len(workers) > 0:
            all_idle = True
            for worker in workers:
                if worker.state != WorkerState.IDLE:
                    all_idle = False
            if all_idle:
                return
        time.sleep(1.0)
        if start_time + timedelta(seconds=timeout) < datetime.now():
            raise RuntimeError("Timed out when waiting for workers to finish")


def wait_start_job(
    worker_command: WorkerCommandChannel, write_job: WriteJob, timeout: float
):
    job_handler = JobHandler(worker_finder=worker_command)
    start_handler = job_handler.start_job(write_job)
    start_time = datetime.now()
    while not start_handler.is_done():
        if start_time + timedelta(seconds=timeout) < datetime.now():
            raise RuntimeError("Timed out when waiting for write job to start")
        elif start_handler.get_state() == CommandState.ERROR:
            raise RuntimeError(f"Got error when trying to start job. Message was: {start_handler.get_message()}")
        time.sleep(0.5)
    return job_handler


def wait_set_stop_now(job: JobHandler, timeout: float):
    stop_handler = job.set_stop_time(datetime.now())
    start_time = datetime.now()
    while not stop_handler.is_done():
        if start_time + timedelta(seconds=timeout) < datetime.now():
            raise RuntimeError("Timed out when setting new stop time for job.")
        elif stop_handler.get_state() == CommandState.ERROR:
            raise RuntimeError(f"Got error when trying to stop job. Message was: {start_handler.get_message()}")
        time.sleep(0.5)


def wait_fail_start_job(
    worker_command: WorkerCommandChannel, write_job: WriteJob, timeout: float
):
    job_handler = JobHandler(worker_finder=worker_command)
    start_handler = job_handler.start_job(write_job)
    start_time = datetime.now()
    while start_handler.get_state() != CommandState.ERROR:
        if start_time + timedelta(seconds=timeout) < datetime.now():
            raise RuntimeError("Timed out when waiting for write job to FAIL to start")
        time.sleep(0.5)


def stop_all_jobs(worker_command: WorkerCommandChannel):
    jobs = worker_command.list_known_jobs()
    for job in jobs:
        job_handler = JobHandler(worker_command, job.job_id)
        if job_handler.get_state() == JobState.WRITING:
            job_handler.stop_now()
        else:
            continue
        while not job_handler.is_done():
            print(f'Waiting for job id "{job.job_id}" to finish.')
            time.sleep(1)
        print(f'Job with id "{job.job_id}" stopped')
