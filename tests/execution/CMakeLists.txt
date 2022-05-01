# tests/query/CMakeLists.txt

#ifndef PRJ_NO_QP_OPT_TEST
add_tdb_test(BasicTestTableScan)
add_tdb_test(BasicTestTableDelete)
add_tdb_test(BasicTestTableInsert)
add_tdb_test(BasicTestTempTable)
add_tdb_test(BasicTestProjection)
add_tdb_test(BasicTestLimit)
#endif

add_tdb_test(BasicTestSelection)
add_tdb_test(BasicTestAggregation)
add_tdb_test(BasicTestCartesianProduct)
add_tdb_test(BasicTestIndexScan)
add_tdb_test(BasicTestSort)
add_tdb_test(BasicTestCompositePlan)

# The system and bonus TPC-H tests are not added to ctest test list so that
# they are not run by default with ctest. These tests must be invoked directly
# using their executables.

