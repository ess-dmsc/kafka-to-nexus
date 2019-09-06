import flatbuffers
from .ep00.EpicsConnectionInfo import (
    EpicsConnectionInfoAddType,
    EpicsConnectionInfoAddTimestamp,
    EpicsConnectionInfoAddSourceName,
    EpicsConnectionInfoStart,
    EpicsConnectionInfoEnd,
)


def _millseconds_to_nanoseconds(time_ms):
    return int(time_ms * 1000000)


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
