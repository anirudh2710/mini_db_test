# src/storage/CMakeLists.txt

set(DATAPAGE_SRC VarlenDataPage.cpp)

set(STORAGE_LIB_SRC
    FSFile.cpp
    FSFile_private.cpp
    FileManager.cpp
    FileManager_wrappable.cpp
    BufferManager.cpp
    BufferManager_private.cpp
    ${DATAPAGE_SRC}
    Table.cpp
)

add_tdb_object_library(storage ${STORAGE_LIB_SRC})

