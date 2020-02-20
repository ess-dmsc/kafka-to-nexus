import flatbuffers
from .f142_logdata import LogData
from .f142_logdata.Value import Value
from .f142_logdata.Int import IntStart, IntAddValue, IntEnd
from .run_start_stop import RunStart, RunStop


def _millseconds_to_nanoseconds(time_ms):
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


def create_runstart_message(start_time: int, stop_time: int, run_name: str, nexus_structure: str, job_id: str,
                            filename: str, service_id: str = "", instrument_name: str = "TEST",
                            broker: str = "localhost:9092"):
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


def create_runstop_message(stop_time: int, run_name: str, job_id: str, service_id: str):
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
