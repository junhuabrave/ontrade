add_library(ontrade_runtime INTERFACE)
target_include_directories(ontrade_runtime INTERFACE ${PROJECT_SOURCE_DIR})
target_compile_features(ontrade_runtime INTERFACE cxx_std_23)

if(ONTRADE_BUILD_TESTS)
    add_executable(clock_test clock_test.cc)
    target_link_libraries(clock_test PRIVATE
        ontrade_runtime
        GTest::gtest_main
    )
    ontrade_apply_warnings(clock_test)
    gtest_discover_tests(clock_test)

    add_executable(latency_test latency_test.cc)
    target_link_libraries(latency_test PRIVATE
        ontrade_runtime
        ontrade_proto_hot
        GTest::gtest_main
    )
    ontrade_apply_warnings(latency_test)
    gtest_discover_tests(latency_test)
endif()
