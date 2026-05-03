add_library(ontrade_oms INTERFACE)
target_include_directories(ontrade_oms INTERFACE ${PROJECT_SOURCE_DIR})
target_compile_features(ontrade_oms INTERFACE cxx_std_23)
target_link_libraries(ontrade_oms INTERFACE
    ontrade_messaging
    ontrade_proto_hot
    ontrade_risk
    ontrade_runtime
)

if(ONTRADE_BUILD_TESTS)
    add_executable(oms_test oms_test.cc)
    target_link_libraries(oms_test PRIVATE
        ontrade_oms
        GTest::gtest_main
    )
    ontrade_apply_warnings(oms_test)
    gtest_discover_tests(oms_test)
endif()
