find_path(JEMALLOC_INCLUDE_DIR NAMES jemalloc/jemalloc.h)
find_library(JEMALLOC_LIBRARIES NAMES jemalloc)

#message(FATAL_ERROR "${JEMALLOC_INCLUDE_DIR}")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(JEMALLOC DEFAULT_MSG
	JEMALLOC_INCLUDE_DIR
	JEMALLOC_LIBRARIES
)
