add_library(ontrade_strategy INTERFACE)
target_include_directories(ontrade_strategy INTERFACE ${PROJECT_SOURCE_DIR})
target_compile_features(ontrade_strategy INTERFACE cxx_std_23)
target_link_libraries(ontrade_strategy INTERFACE
    ontrade_messaging
    ontrade_proto_hot
    ontrade_runtime
)

if(ONTRADE_BUILD_TESTS)
    add_executable(strategy_runner_test runner_test.cc)
    target_link_libraries(strategy_runner_test PRIVATE
        ontrade_strategy
        GTest::gtest_main
    )
    ontrade_apply_warnings(strategy_runner_test)
    gtest_discover_tests(strategy_runner_test)
endif()
