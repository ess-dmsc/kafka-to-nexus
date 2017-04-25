find_path(RDKAFKA_INCLUDE_DIR NAMES librdkafka/rdkafka.h)
find_library(RDKAFKA_LIBRARIES_C NAMES rdkafka)
find_library(RDKAFKA_LIBRARIES_CXX NAMES rdkafka++)
set(RDKAFKA_LIBRARIES ${RDKAFKA_LIBRARIES_C} ${RDKAFKA_LIBRARIES_CXX})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RDKAFKA DEFAULT_MSG
	RDKAFKA_INCLUDE_DIR
	RDKAFKA_LIBRARIES
)
