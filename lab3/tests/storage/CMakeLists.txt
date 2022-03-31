# tests/storage/CMakeLists.txt

add_tdb_test(BasicTestFSFile)

# Unfortunately we have to add additional test targets here because ctest
# doesn't support passing through extra args at run time.
#
# There's an open issue https://gitlab.kitware.com/cmake/cmake/-/issues/20470
# for that.
gtest_add_tests(
    TARGET BasicTestFSFile
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
    EXTRA_ARGS
        --test_never_call_fallocate
    TEST_SUFFIX "NoFallocate"
)

add_tdb_test(BasicTestBufferManager)
target_link_options(BasicTestBufferManager
    PUBLIC -Wl,--wrap,taco_fileman_readpage_impl
    PUBLIC -Wl,--wrap,taco_fileman_writepage_impl)

add_tdb_test(BasicTestEvictionPolicy)
target_link_options(BasicTestEvictionPolicy
    PUBLIC -Wl,--wrap,taco_fileman_readpage_impl
    PUBLIC -Wl,--wrap,taco_fileman_writepage_impl)

add_tdb_test(BasicTestVarlenDataPage)

add_tdb_test_binary(BasicTestTable)
gtest_discover_tests(BasicTestTable)


add_tdb_test(BasicTestSortedDataPage)

