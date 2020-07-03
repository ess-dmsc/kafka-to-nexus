from datetime import datetime
from time import time


def unix_time_milliseconds(dt):
    epoch = datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0


def milliseconds_to_nanoseconds(time_ms: int) -> int:
    return time_ms * 1_000_000


def current_unix_time_ms() -> int:
    return int(time() * 1000)
