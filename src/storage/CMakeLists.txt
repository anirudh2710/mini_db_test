# src/storage/CMakeLists.txt

set(DATAPAGE_SRC VarlenDataPage.cpp)

set(STORAGE_LIB_SRC
    FSFile.cpp
    FSFile_private.cpp
)

add_tdb_object_library(storage ${STORAGE_LIB_SRC})

