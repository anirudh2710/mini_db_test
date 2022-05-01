# tests/storage/CMakeLists.txt

add_library(extsort_testlib OBJECT
    PseudoRandomItemIterator.cpp
)
target_link_libraries(extsort_testlib tdb_test_base)

add_tdb_test(BasicTestExternalSort)
target_link_libraries(BasicTestExternalSort extsort_testlib)
