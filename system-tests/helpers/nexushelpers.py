import h5py


class OpenNexusFile:
    """
    Context manager for opening NeXus files
    Retries opening until successful
    """

    def __init__(self, filepath):
        try:
            self.file = h5py.File(filepath, mode="r")
        except OSError as e:
            print(f'Failed to open file "{filepath}"')
            raise e

    def __enter__(self):
        return self.file

    def __exit__(self, ex_type, ex_value, ex_traceback):
        self.file.close()
