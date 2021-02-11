import h5py
from datetime import datetime, timedelta
import time


class OpenNexusFile:
    """
    Context manager for opening NeXus files.
    """

    def __init__(self, filepath: str, timeout: float = 10):
        used_timeout = timedelta(seconds=timeout)
        start_time = datetime.now()
        while True:
            try:
                self.file = h5py.File(filepath, mode="r")
            except OSError as e:
                if datetime.now() > start_time + used_timeout:
                    print(f'Failed to open file "{filepath}"')
                    raise e
                time.sleep(0.5)
                continue
            break

    def __enter__(self):
        return self.file

    def __exit__(self, ex_type, ex_value, ex_traceback):
        self.file.close()
