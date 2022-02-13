# src/utils/CMakeLists.txt

add_subdirectory(typsupp)

set(UTILS_LIB_SRC
    builtin_funcs.cpp
    fsutils.cpp
    misc.cpp
    pgmkdirp.cpp
    zerobuf.cpp
)

add_tdb_object_library(utils ${UTILS_LIB_SRC})

set(FILES_WITH_BUILTIN_FUNCS
    ${CMAKE_SOURCE_DIR}/src/utils/typsupp/bool.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/typsupp/char.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/typsupp/int1.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/typsupp/int2.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/typsupp/int4.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/typsupp/int8.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/typsupp/oid.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/typsupp/uint1.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/typsupp/uint2.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/typsupp/uint4.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/typsupp/uint8.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/typsupp/varchar.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/typsupp/float.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/typsupp/double.cpp
    PARENT_SCOPE)

