find_path(MPI_INCLUDE_DIR NAMES mpi.h)
find_library(MPI_LIBRARIES NAMES mpi)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(MPI DEFAULT_MSG
	MPI_INCLUDE_DIR
	MPI_LIBRARIES
)
