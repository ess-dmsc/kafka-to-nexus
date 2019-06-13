from datetime import datetime


def unix_time_milliseconds(dt):
    epoch = datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0
