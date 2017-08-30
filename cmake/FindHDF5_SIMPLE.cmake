find_path(HDF5_INCLUDE_DIRS NAMES hdf5.h HINTS ${HDF5_ROOT}/include)
find_library(HDF5_C_LIBRARIES NAMES hdf5 hdf5-shared HINTS ${HDF5_ROOT}/lib)
find_package_handle_standard_args(HDF5_SIMPLE DEFAULT_MSG
	HDF5_INCLUDE_DIRS HDF5_C_LIBRARIES
)
