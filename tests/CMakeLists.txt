# tests/CMakeLists.txt

# Add the current directory in the include path. The test only headers are
# dirctly placed under tests.
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# base must be added first -- this will add the tdb_test_base library that all
# tests should link to, and a convenient function add_tdb_test() for adding
# tests
add_subdirectory(base)

add_tdb_test(BasicTestRepoCompilesAndRuns)

# add the tests
add_subdirectory(storage)

# The example_test target shows the usages of the predefined test fixtures.
# It should be disabled in the assignment distribution.
#add_tdb_test(example_test)

