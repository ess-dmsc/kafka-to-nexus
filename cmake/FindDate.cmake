find_path(DATE_INCLUDE_DIR NAMES date/date.h)
find_library(DATE_LIBRARIES_C NAMES date)
find_library(DATE_LIBRARIES_CXX NAMES date)
set(DATE_LIBRARIES ${DATE_LIBRARIES_C} ${DATE_LIBRARIES_CXX})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(DATE DEFAULT_MSG
		DATE_INCLUDE_DIR
		DATE_LIBRARIES
)
