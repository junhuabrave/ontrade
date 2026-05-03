add_executable(ontrade_smoke smoke.cc)
target_link_libraries(ontrade_smoke PRIVATE
    ontrade_oms
    ontrade_gateways
    ontrade_strategy
    ontrade_runtime
    ontrade_proto_hot
    ontrade_messaging
    ontrade_risk
)
ontrade_apply_warnings(ontrade_smoke)

if(ONTRADE_BUILD_TESTS)
    add_executable(smoke_test smoke_test.cc)
    target_link_libraries(smoke_test PRIVATE
        ontrade_oms
        ontrade_gateways
        ontrade_strategy
        GTest::gtest_main
    )
    ontrade_apply_warnings(smoke_test)
    gtest_discover_tests(smoke_test)
endif()
