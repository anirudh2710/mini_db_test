# src/catalog/CMakeLists.txt

add_subdirectory(systables)

set(CATALOG_LIB_SRC
    BootstrapCatCache.cpp
    InitDataFileReader.cpp
    Schema.cpp
    TableDesc.cpp
    VolatileCatCache.cpp
    IndexDesc.cpp
)

add_tdb_object_library(catalog ${CATALOG_LIB_SRC})

