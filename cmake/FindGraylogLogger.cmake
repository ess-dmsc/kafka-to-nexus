# opt-in (as requested)
set(USE_GRAYLOG_LOGGER TRUE CACHE BOOL "Try to use graylog_logger")
find_path(GRAYLOGLOGGER_INCLUDE_DIR NAMES graylog_logger/Log.hpp)
find_library(GRAYLOGLOGGER_LIBRARY NAMES graylog_logger)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(GRAYLOGLOGGER DEFAULT_MSG
    GRAYLOGLOGGER_INCLUDE_DIR
    GRAYLOGLOGGER_LIBRARY
)

add_library(libgraylog_logger SHARED IMPORTED)
set_property(TARGET libgraylog_logger PROPERTY IMPORTED_LOCATION ${GRAYLOGLOGGER_LIBRARY})
