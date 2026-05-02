cmake_minimum_required(VERSION 3.25)

project(ontrade
    VERSION 0.0.1
    LANGUAGES CXX
    DESCRIPTION "OnTrade systematic trading platform"
)

# C++23 floor per docs/architecture.md §5.4. Selected C++26 features may be
# enabled per-target behind feature flags once compiler support is stable.
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON CACHE BOOL "" FORCE)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(ONTRADE_BUILD_TESTS "Build unit tests" ON)
option(ONTRADE_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
include(CompilerWarnings)

if(ONTRADE_BUILD_TESTS)
    enable_testing()
    include(FetchContent)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.14.0
        GIT_SHALLOW TRUE
    )
    FetchContent_MakeAvailable(googletest)
    include(GoogleTest)
endif()

add_subdirectory(core)
