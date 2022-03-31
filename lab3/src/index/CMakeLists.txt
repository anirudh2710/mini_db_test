# src/index/CMakeLists.txt

add_subdirectory(volatiletree)
add_subdirectory(btree)

set(INDEX_LIB_SRC
    idxtyps.cpp
    Index.cpp
    tuple_compare.cpp
    TableBulkLoadIterator.cpp
)

add_tdb_object_library(index ${INDEX_LIB_SRC})

