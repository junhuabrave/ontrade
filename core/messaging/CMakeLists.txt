add_library(ontrade_messaging INTERFACE)
target_include_directories(ontrade_messaging INTERFACE ${PROJECT_SOURCE_DIR})
target_compile_features(ontrade_messaging INTERFACE cxx_std_23)

if(ONTRADE_BUILD_TESTS)
    add_executable(shm_ring_test shm_ring_test.cc)
    target_link_libraries(shm_ring_test PRIVATE
        ontrade_messaging
        GTest::gtest_main
    )
    ontrade_apply_warnings(shm_ring_test)
    gtest_discover_tests(shm_ring_test)
endif()
