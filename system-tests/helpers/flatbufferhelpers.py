import flatbuffers
from .f142_logdata import LogData
from .f142_logdata.Value import Value
from .f142_logdata.Int import IntStart, IntAddValue, IntEnd
from .run_start_stop import RunStart, RunStop
from time import time
from typing import Optional


def _millseconds_to_nanoseconds(time_ms: int) -> int:
    return int(time_ms * 1000000)


def _seconds_to_nanoseconds(time_s: float) -> int:
    return int(time_s * 1000000000)


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


def create_runstart_message(job_id: str, filename: str, start_time: Optional[int] = None,
                            stop_time: Optional[int] = None, run_name: str = "test_run", nexus_structure: str = "{}",
                            service_id: str = "", instrument_name: str = "TEST", broker: str = "localhost:9092") -> bytes:
    builder = flatbuffers.Builder(1024)

    if start_time is None:
        start_time = _seconds_to_nanoseconds(time())
    if service_id is None:
        service_id = ""
    if stop_time is None:
        stop_time = 0

    filename_offset = builder.CreateString(filename)
    service_id_offset = builder.CreateString(service_id)
    broker_offset = builder.CreateString(broker)
    job_id_offset = builder.CreateString(job_id)
    nexus_structure_offset = builder.CreateString(nexus_structure)
    instrument_name_offset = builder.CreateString(instrument_name)
    run_name_offset = builder.CreateString(run_name)

    # Build the actual buffer
    RunStart.RunStartStart(builder)
    # RunStart.RunStartAddDetectorSpectrumMap(builder, None)
    RunStart.RunStartAddNPeriods(builder, 1)
    RunStart.RunStartAddFilename(builder, filename_offset)
    RunStart.RunStartAddServiceId(builder, service_id_offset)
    RunStart.RunStartAddBroker(builder, broker_offset)
    RunStart.RunStartAddJobId(builder, job_id_offset)
    RunStart.RunStartAddNexusStructure(builder, nexus_structure_offset)
    RunStart.RunStartAddInstrumentName(builder, instrument_name_offset)
    RunStart.RunStartAddRunName(builder, run_name_offset)
    RunStart.RunStartAddStopTime(builder, stop_time)
    RunStart.RunStartAddStartTime(builder, start_time)
    run_start_message = RunStart.RunStartEnd(builder)
    builder.Finish(run_start_message)

    # Generate the output and replace the file_identifier
    buff = builder.Output()
    buff[4:8] = b"pl72"
    return bytes(buff)


def create_runstop_message(job_id: str, run_name: str = "test_run", service_id: str = "",
                           stop_time: Optional[int] = None) -> bytes:
    builder = flatbuffers.Builder(1024)

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
