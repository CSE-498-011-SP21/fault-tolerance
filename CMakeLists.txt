cmake_minimum_required(VERSION 3.11)

project(FaultTolerance VERSION 0.1.0)

find_package(Boost REQUIRED)

set(CMAKE_CXX_STANDARD 17)

include (FetchContent)

FetchContent_Declare(
        common
        GIT_REPOSITORY git@github.com:CSE-498-011-SP21/common.git
        GIT_TAG main
)

FetchContent_MakeAvailable(common)

add_library(Common::kvcg_stuff ALIAS kvcg_stuff)

FetchContent_Declare(
        NetworkLayer
        GIT_REPOSITORY git@github.com:CSE-498-011-SP21/network-layer.git
        GIT_TAG main
)

FetchContent_MakeAvailable(NetworkLayer)

add_library(NetworkLayer::fabricBased ALIAS fabricBased)

# Library code
add_subdirectory(src)

# Executable code
add_subdirectory(test)

set(CPACK_PACKAGE_VENDOR "Cody, Jacob, and Olivia")
SET(CPACK_GENERATOR "DEB")
SET(CPACK_DEBIAN_PACKAGE_MAINTAINER "Cody, Jacob, and Olivia")
set(CPACK_SOURCE_GENERATOR "DEB")
set(CPACK_SOURCE_IGNORE_FILES
        /.git
        /dist
        /.*build.*
        /\\\\.DS_Store
        )
