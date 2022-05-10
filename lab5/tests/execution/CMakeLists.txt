# tests/query/CMakeLists.txt

add_tdb_test(BasicTestTableScan)
add_tdb_test(BasicTestTableDelete)
add_tdb_test(BasicTestTableInsert)
add_tdb_test(BasicTestTempTable)
add_tdb_test(BasicTestProjection)
add_tdb_test(BasicTestLimit)

add_tdb_test(BasicTestSelection)
add_tdb_test(BasicTestAggregation)
add_tdb_test(BasicTestCartesianProduct)
add_tdb_test(BasicTestIndexScan)
add_tdb_test(BasicTestSort)
add_tdb_test(BasicTestCompositePlan)

add_tdb_test(BasicTestMergeJoin)
add_tdb_test(BasicTestIndexNestedLoop)

add_library(tpch_testlib OBJECT
    TPCHTest.cpp
)
target_link_libraries(
    tpch_testlib
    tdb_test_base
)

# The system and bonus TPC-H tests are not added to ctest test list so that
# they are not run by default with ctest. These tests must be invoked directly
# using their executables.

add_executable(BonusTestTPCHQ3 BonusTestTPCHQ3.cpp BonusTestTPCHQ3Plan.cpp)
target_link_libraries(BonusTestTPCHQ3 tdb_test_base tpch_testlib)
#gtest_discover_tests(
#    BonusTestTPCHQ3
#    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
#)

add_executable(BonusTestTPCHQS BonusTestTPCHQS.cpp BonusTestTPCHQSPlan.cpp)
target_link_libraries(BonusTestTPCHQS tdb_test_base tpch_testlib)
#gtest_discover_tests(
#    BonusTestTPCHQS
#    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
#)

add_executable(BonusTestTPCHQ5 BonusTestTPCHQ5.cpp BonusTestTPCHQ5Plan.cpp)
target_link_libraries(BonusTestTPCHQ5 tdb_test_base tpch_testlib)
#gtest_discover_tests(
#    BonusTestTPCHQ5
#    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
#)

