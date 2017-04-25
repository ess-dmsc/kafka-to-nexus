find_path(RAPIDJSON_INCLUDE_DIR NAMES rapidjson/document.h)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RAPIDJSON DEFAULT_MSG
    RAPIDJSON_INCLUDE_DIR
)
