# src/plan/CMakeLists.txt

set(PLAN_LIB_SRC
	TableScan.cpp
    Selection.cpp
    Projection.cpp
    CartesianProduct.cpp
    TempTable.cpp
    TableInsert.cpp
    TableDelete.cpp
    IndexScan.cpp
    Aggregation.cpp
    Sort.cpp
    Limit.cpp
)

add_tdb_object_library(plan ${PLAN_LIB_SRC})

