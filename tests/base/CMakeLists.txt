# tests/base/CMakeLists.txt

add_library(tdb_test_base OBJECT
    TDBEnv.cpp
    TDBNonDBTest.cpp
	TDBDBTest.cpp
    tdb_test_main.cpp
)
target_link_libraries(
    tdb_test_base
    tdb_test
    ${GMOCK_TARGET}
)

# add_tdb_test_binary(<test_name>)
# This only adds an executable target without adding a test target.
function(add_tdb_test_binary test_name)
    add_executable(${test_name} "${test_name}.cpp")
    target_link_libraries(${test_name} tdb_test_base)
endfunction()

# add_tdb_test(<test_name>)
function(add_tdb_test test_name)
    add_tdb_test_binary(${test_name})
    gtest_add_tests(
        TARGET ${test_name}
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}")
endfunction()
