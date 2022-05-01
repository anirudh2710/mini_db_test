# src/query/CMakeLists.txt

set(EXECUTION_LIB_SRC
	TableScanState.cpp
    SelectionState.cpp
    ProjectionState.cpp
    CartesianProductState.cpp
    TempTableState.cpp
    IndexScanState.cpp
    TableInsertState.cpp
    TableDeleteState.cpp
    AggregationState.cpp
    SortState.cpp
    LimitState.cpp
)

add_tdb_object_library(execution ${EXECUTION_LIB_SRC})

