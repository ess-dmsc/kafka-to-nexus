import h5py
from time import sleep


class OpenNexusFileWhenAvailable:
    """
    Context manager for opening NeXus files
    Retries opening until successful
    """

    def __init__(self, filepath):
        attempts = 0
        while attempts < 50:
            try:
                self.file = h5py.File(filepath, mode="r")
            except OSError:
                attempts += 1
                sleep(1)
                continue
            return
        raise OSError(f"Failed to open NeXus file {filepath} after {attempts} attempts")

    def __enter__(self):
        return self.file

    def __exit__(self, ex_type, ex_value, ex_traceback):
        self.file.close()
