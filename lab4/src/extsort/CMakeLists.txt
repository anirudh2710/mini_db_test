# src/query/CMakeLists.txt

set(EXTSORT_LIB_SRC
    ExternalSort.cpp
    )

add_tdb_object_library(extsort ${EXTSORT_LIB_SRC})

