add_library(ontrade_proto_hot INTERFACE)
target_include_directories(ontrade_proto_hot INTERFACE ${PROJECT_SOURCE_DIR})
target_compile_features(ontrade_proto_hot INTERFACE cxx_std_23)

if(ONTRADE_BUILD_TESTS)
    add_executable(proto_hot_test messages_test.cc)
    target_link_libraries(proto_hot_test PRIVATE
        ontrade_proto_hot
        GTest::gtest_main
    )
    ontrade_apply_warnings(proto_hot_test)
    gtest_discover_tests(proto_hot_test)
endif()
