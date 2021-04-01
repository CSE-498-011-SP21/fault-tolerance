# Note that headers are optional, and do not affect add_library, but they will not
# show up in IDEs unless they are listed in add_library.

# Optionally glob, but only for CMake 3.12 or later:
file(GLOB HEADER_LIST CONFIGURE_DEPENDS "${FaultTolerance_SOURCE_DIR}/include/faulttolerance/*.h")

# Make an automatic library - will be static or dynamic based on user setting
add_library(faulttolerance client.cc fault_tolerance.cc kvcg_config.cc server.cc ${HEADER_LIST})

# We need this directory, and users of our library will need it too
target_include_directories(faulttolerance PUBLIC ../include)

# This depends on (header only) boost
target_link_libraries(faulttolerance PRIVATE Boost::boost kvcg_stuff fabricBased)

# All users of this library will need at least C++17
target_compile_features(faulttolerance PUBLIC cxx_std_17)

target_compile_definitions(faulttolerance PUBLIC FT_ONE_SIDED_LOGGING)

# IDEs should put the headers in a nice place
source_group(
  TREE "${PROJECT_SOURCE_DIR}/include"
  PREFIX "Header Files"
  FILES ${HEADER_LIST})

