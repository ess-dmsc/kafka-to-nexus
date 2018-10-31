# Extract the current git version to use for compilation.
# Does not cause a recompile if the commit did not change.
# To use, make your target depend on 'git_commit_current'.
# It will create ${CMAKE_CURRENT_BINARY_DIR}/git_commit_current.cxx which
# you should add to your list of sources.

file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/git_commit_current.cxx "extern \"C\" char const GIT_COMMIT[] = \"NOTSET\";\n")

add_custom_target(git_commit_now ALL
COMMAND echo 'extern \"C\" char const GIT_COMMIT[] = \"'`git rev-parse HEAD`'\"\;' > ${CMAKE_CURRENT_BINARY_DIR}/git_commit_now
WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_custom_target(git_commit_current ALL
COMMAND git diff --name-only --no-index ${CMAKE_CURRENT_BINARY_DIR}/git_commit_now ${CMAKE_CURRENT_BINARY_DIR}/git_commit_current.cxx || cp ${CMAKE_CURRENT_BINARY_DIR}/git_commit_now ${CMAKE_CURRENT_BINARY_DIR}/git_commit_current.cxx
DEPENDS git_commit_now
)
