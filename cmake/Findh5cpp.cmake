find_path(H5CPP_INCLUDE_DIR NAMES h5cpp/hdf5.hpp)
find_library(H5CPP_LIBRARIES NAMES h5cpp)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(H5CPP DEFAULT_MSG
  H5CPP_INCLUDE_DIR
  H5CPP_LIBRARIES
)
