add_library(ontrade_memory INTERFACE)
target_include_directories(ontrade_memory INTERFACE ${PROJECT_SOURCE_DIR})
target_compile_features(ontrade_memory INTERFACE cxx_std_23)

if(ONTRADE_BUILD_TESTS)
    add_executable(arena_test arena_test.cc)
    target_link_libraries(arena_test PRIVATE
        ontrade_memory
        GTest::gtest_main
    )
    ontrade_apply_warnings(arena_test)
    gtest_discover_tests(arena_test)

    add_executable(pool_test pool_test.cc)
    target_link_libraries(pool_test PRIVATE
        ontrade_memory
        GTest::gtest_main
    )
    ontrade_apply_warnings(pool_test)
    gtest_discover_tests(pool_test)
endif()
