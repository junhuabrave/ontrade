add_library(ontrade_gateways INTERFACE)
target_include_directories(ontrade_gateways INTERFACE ${PROJECT_SOURCE_DIR})
target_compile_features(ontrade_gateways INTERFACE cxx_std_23)
target_link_libraries(ontrade_gateways INTERFACE
    ontrade_messaging
    ontrade_proto_hot
    ontrade_runtime
)

if(ONTRADE_BUILD_TESTS)
    add_executable(loopback_test loopback_test.cc)
    target_link_libraries(loopback_test PRIVATE
        ontrade_gateways
        ontrade_oms
        GTest::gtest_main
    )
    ontrade_apply_warnings(loopback_test)
    gtest_discover_tests(loopback_test)
endif()
