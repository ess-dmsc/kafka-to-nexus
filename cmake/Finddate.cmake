find_path(DATE_INCLUDE_DIR NAMES date/date.h)
include(FindPackageHandleStandardArgs)
find_library(DATE_LIBRARY NAMES date)

find_package_handle_standard_args(DATE DEFAULT_MSG
		DATE_INCLUDE_DIR
		DATE_LIBRARY
)
