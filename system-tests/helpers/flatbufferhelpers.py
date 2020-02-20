import flatbuffers
from .f142_logdata import LogData
from .f142_logdata.Value import Value
from .f142_logdata.Int import IntStart, IntAddValue, IntEnd
from .run_start_stop import RunStart, RunStop
from time import time


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


def create_runstart_message(job_id: str, filename: str, start_time: int = _seconds_to_nanoseconds(time()),
                            stop_time: int = 0, run_name: str = "test_run", nexus_structure: str = "{}",
                            service_id: str = "", instrument_name: str = "TEST", broker: str = "localhost:9092"):
    file_identifier = b"pl72"
    builder = flatbuffers.Builder(1024)

    run_name_offset = builder.CreateString(run_name)
    instrument_name_offset = builder.CreateString(instrument_name)
    nexus_structure_offset = builder.CreateString(nexus_structure)
    job_id_offset = builder.CreateString(job_id)
    broker_offset = builder.CreateString(broker)
    service_id_offset = builder.CreateString(service_id)
    filename_offset = builder.CreateString(filename)

    # Build the actual buffer
    RunStart.RunStartStart(builder)
    RunStart.RunStartAddStartTime(builder, start_time)
    RunStart.RunStartAddStopTime(builder, stop_time)
    RunStart.RunStartAddRunName(builder, run_name_offset)
    RunStart.RunStartAddInstrumentName(builder, instrument_name_offset)
    RunStart.RunStartAddNexusStructure(builder, nexus_structure_offset)
    RunStart.RunStartAddJobId(builder, job_id_offset)
    RunStart.RunStartAddBroker(builder, broker_offset)
    RunStart.RunStartAddServiceId(builder, service_id_offset)
    RunStart.RunStartAddFilename(builder, filename_offset)
    run_start_message = RunStart.RunStartEnd(builder)
    builder.Finish(run_start_message)

    # Generate the output and replace the file_identifier
    buff = builder.Output()
    buff[4:8] = file_identifier
    return bytes(buff)


def create_runstop_message(job_id: str, run_name: str = "test_run", service_id: str = "",
                           stop_time: int = _seconds_to_nanoseconds(time())):
    file_identifier = b"6s4t"
    builder = flatbuffers.Builder(1024)

    run_name_offset = builder.CreateString(run_name)
    job_id_offset = builder.CreateString(job_id)
    service_id_offset = builder.CreateString(service_id)

    # Build the actual buffer
    RunStop.RunStopStart(builder)
    RunStop.RunStopAddStopTime(builder, stop_time)
    RunStop.RunStopAddRunName(builder, run_name_offset)
    RunStop.RunStopAddJobId(builder, job_id_offset)
    RunStop.RunStopAddServiceId(builder, service_id_offset)
    run_stop_message = RunStop.RunStopEnd(builder)
    builder.Finish(run_stop_message)

    # Generate the output and replace the file_identifier
    buff = builder.Output()
    buff[4:8] = file_identifier
    return bytes(buff)
