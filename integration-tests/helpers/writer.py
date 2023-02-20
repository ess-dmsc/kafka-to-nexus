from file_writer_control import WorkerJobPool
from file_writer_control.WorkerStatus import WorkerState
from file_writer_control import JobState
from file_writer_control import CommandHandler
from file_writer_control import CommandState
from file_writer_control import WriteJob
from file_writer_control import JobHandler
from datetime import datetime, timedelta
import time

SLEEP_TIME = 1.5


def wait_writers_available(worker_pool: WorkerJobPool, timeout: float, nr_of: int):
    start_time = datetime.now()
    time.sleep(SLEEP_TIME)
    while True:
        nbr_of_idle_workers = 0
        known_workers = worker_pool.list_known_workers()
        for worker in known_workers:
            if worker.state == WorkerState.IDLE:
                nbr_of_idle_workers += 1
        if nbr_of_idle_workers >= nr_of:
            return
        time.sleep(1.0)
        if start_time + timedelta(seconds=timeout) < datetime.now():
            raise RuntimeError(f"Timed out when waiting for {nr_of} idle workers")


def wait_no_working_writers(worker_pool: WorkerJobPool, timeout: float):
    start_time = datetime.now()
    time.sleep(SLEEP_TIME)
    while True:
        workers = worker_pool.list_known_workers()
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


def wait_command_is_done(command_handler: CommandHandler, timeout: float):
    start_time = datetime.now()
    try:
        while not command_handler.is_done():
            if start_time + timedelta(seconds=timeout) < datetime.now():
                raise RuntimeError("Timed out when waiting for command to finish")
            elif command_handler.get_state() == CommandState.ERROR:
                raise RuntimeError(
                    f"Command failed. Message was: {command_handler.get_message()}"
                )
            time.sleep(0.5)
    except RuntimeError as e:
        raise RuntimeError(
            e.__str__() + f" The message was: {command_handler.get_message()}"
        )


def wait_start_job(
    worker_pool: WorkerJobPool, write_job: WriteJob, timeout: float
) -> JobHandler:
    job_handler = JobHandler(worker_finder=worker_pool)
    start_handler = job_handler.start_job(write_job)
    wait_command_is_done(start_handler, timeout)
    return job_handler


def wait_set_stop_now(job: JobHandler, timeout: float):
    stop_handler = job.set_stop_time(datetime.now())
    wait_command_is_done(stop_handler, timeout)


def wait_fail_start_job(
    worker_pool: WorkerJobPool, write_job: WriteJob, timeout: float
) -> str:
    job_handler = JobHandler(worker_finder=worker_pool)
    start_handler = job_handler.start_job(write_job)
    start_time = datetime.now()
    while start_handler.get_state() != CommandState.ERROR:
        if start_time + timedelta(seconds=timeout) < datetime.now():
            raise RuntimeError("Timed out when waiting for write job to FAIL to start")
        time.sleep(0.5)
    return start_handler.get_message()


def stop_all_jobs(worker_pool: WorkerJobPool):
    time.sleep(SLEEP_TIME)
    jobs = worker_pool.list_known_jobs()
    for job in jobs:
        job_handler = JobHandler(worker_pool, job.job_id)
        if job_handler.get_state() == JobState.WRITING:
            job_handler.stop_now()
        else:
            continue
        while not job_handler.is_done():
            print(f'Waiting for job id "{job.job_id}" to finish.')
            time.sleep(1)
        print(f'Job with id "{job.job_id}" stopped')
