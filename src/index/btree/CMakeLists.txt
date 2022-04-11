# src/index/btree/CMakeLists.txt

add_tdb_object_library(index_btree
    BTree.cpp
    BTreeBulkLoad.cpp
    BTreeIterator.cpp
    BTreeSearch.cpp
    BTreeUtils.cpp
    BTreeDelete.cpp
    BTreeInsert.cpp
)
