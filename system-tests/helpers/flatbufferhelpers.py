import flatbuffers
from .ep00.EpicsConnectionInfo import (
    EpicsConnectionInfoAddType,
    EpicsConnectionInfoAddTimestamp,
    EpicsConnectionInfoAddSourceName,
    EpicsConnectionInfoStart,
    EpicsConnectionInfoEnd,
)
from .f142_logdata import LogData
from .f142_logdata.Value import Value
from .f142_logdata.Int import IntStart, IntAddValue, IntEnd


def _millseconds_to_nanoseconds(time_ms):
    return int(time_ms * 1000000)


def create_f142_message(timestamp_unix_ms=None):
    file_identifier = b"f142"
    builder = flatbuffers.Builder(1024)
    source = builder.CreateString("fw-test-helpers")
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


def create_ep00_message(status, timestamp):
    file_identifier = b"ep00"
    builder = flatbuffers.Builder(1024)
    source = builder.CreateString("SIMPLE:DOUBLE")
    EpicsConnectionInfoStart(builder)
    EpicsConnectionInfoAddType(builder, status)
    EpicsConnectionInfoAddSourceName(builder, source)

    EpicsConnectionInfoAddTimestamp(builder, _millseconds_to_nanoseconds(timestamp))
    end = EpicsConnectionInfoEnd(builder)
    builder.Finish(end)
    # Generate the output and replace the file_identifier
    buff = builder.Output()
    buff[4:8] = file_identifier
    return bytes(buff)
