from datetime import datetime
from os import path
from time import sleep


def unix_time_milliseconds(dt):
    epoch = datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0


def wait_for_file_writing_to_complete(filepath):
    for i in range(100):
        if path.isfile(filepath):
            break
        sleep(0.5)
