add_library(ontrade_risk INTERFACE)
target_include_directories(ontrade_risk INTERFACE ${PROJECT_SOURCE_DIR})
target_compile_features(ontrade_risk INTERFACE cxx_std_23)
target_link_libraries(ontrade_risk INTERFACE ontrade_proto_hot)

if(ONTRADE_BUILD_TESTS)
    add_executable(limits_test limits_test.cc)
    target_link_libraries(limits_test PRIVATE
        ontrade_risk
        GTest::gtest_main
    )
    ontrade_apply_warnings(limits_test)
    gtest_discover_tests(limits_test)
endif()
