import h5py
from datetime import datetime, timedelta
import time
import os

NEXUS_FILES_DIR = os.getcwd()  # default used by the filewriter if hdf-output-prefix not set

def build_relative_file_path(file_name: str) -> str:
    return os.path.join("output-files", file_name)

def build_absolute_file_path(rel_file_name: str) -> str:
    return os.path.join(NEXUS_FILES_DIR, rel_file_name)

class OpenNexusFile:
    """
    Context manager for opening NeXus files.
    """

    def __init__(self, filepath: str, timeout: float = 10):
        used_timeout = timedelta(seconds=timeout)
        start_time = datetime.now()
        full_file_path = build_absolute_file_path(filepath)
        while True:
            try:
                self.file = h5py.File(full_file_path, mode="r")
            except OSError as e:
                if datetime.now() > start_time + used_timeout:
                    print(f'Failed to open file "{full_file_path}"')
                    raise e
                time.sleep(0.5)
                continue
            break

    def __enter__(self):
        return self.file

    def __exit__(self, ex_type, ex_value, ex_traceback):
        self.file.close()
