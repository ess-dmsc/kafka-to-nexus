import flatbuffers
from .f142_logdata import LogData
from .f142_logdata.Value import Value
from .f142_logdata.Int import IntStart, IntAddValue, IntEnd
from .run_start_stop import RunStop
from typing import Optional


def _millseconds_to_nanoseconds(time_ms: int) -> int:
    return int(time_ms * 1000000)


def create_f142_message(timestamp_unix_ms=None, source_name: str = "fw-test-helpers"):
    file_identifier = b"f142"
    builder = flatbuffers.Builder(1024)
    source = builder.CreateString(source_name)
    IntStart(builder)
    IntAddValue(builder, 42)
    int_position = IntEnd(builder)

    # Build the actual buffer
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, int_position)
    LogData.LogDataAddValueType(builder, Value.Int)
    LogData.LogDataAddTimestamp(builder, _millseconds_to_nanoseconds(timestamp_unix_ms))
    log_msg = LogData.LogDataEnd(builder)
    builder.Finish(log_msg)

    # Generate the output and replace the file_identifier
    buff = builder.Output()
    buff[4:8] = file_identifier
    return bytes(buff)


def create_runstop_message(
    job_id: str,
    run_name: str = "test_run",
    service_id: str = "",
    stop_time: Optional[int] = None,
) -> bytes:
    builder = flatbuffers.Builder(136)

    if service_id is None:
        service_id = ""
    if stop_time is None:
        stop_time = 0

    service_id_offset = builder.CreateString(service_id)
    job_id_offset = builder.CreateString(job_id)
    run_name_offset = builder.CreateString(run_name)

    # Build the actual buffer
    RunStop.RunStopStart(builder)
    RunStop.RunStopAddServiceId(builder, service_id_offset)
    RunStop.RunStopAddJobId(builder, job_id_offset)
    RunStop.RunStopAddRunName(builder, run_name_offset)
    RunStop.RunStopAddStopTime(builder, stop_time)

    run_stop_message = RunStop.RunStopEnd(builder)
    builder.Finish(run_stop_message)

    # Generate the output and replace the file_identifier
    buff = builder.Output()
    buff[4:8] = b"6s4t"
    return bytes(buff)
