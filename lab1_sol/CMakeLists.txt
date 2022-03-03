# CMakeLists.txt

cmake_minimum_required(VERSION 3.13)

find_package(PkgConfig REQUIRED)
include(CheckCXXSourceCompiles)
include(CheckSymbolExists)

project(taco-db
    VERSION 0.1.0
    LANGUAGES C CXX)

option(PREBUILD_ABSL_AND_GTEST
    "whether to prebuild and install Abseil and GoogleTest. If defined, we will try to find the prebuilt binary in the search paths as well."
    ON)

option(USE_JEMALLOC
    "whether to use jemalloc(3) in place of malloc(3)"
    ON)

# We prefer our custom built jemalloc by default. If you have libjemalloc
# already installed in a path that can be found, you may override that by
# passing -DPREFER_PREBUILT_JEMALLOC=ON.
option(PREFER_PREBUILT_JEMALLOC
    "whether to try to find an existing jemalloc build instead of building it in external/jemalloc"
    OFF)

# set default build type
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "" FORCE)
  message(NOTICE "-- No build mode chosen - using Debug by default")
endif(NOT CMAKE_BUILD_TYPE)

# find a few programs

find_program(SHELL bash)
if(NOT SHELL)
    message(FATAL_ERROR "can't find bash")
endif()

find_program(SED sed)
if (NOT SED)
    message(FATAL_ERROR "can't find sed")
endif()

find_program(GREP grep)
if (NOT GREP)
    message(FATAL_ERROR "can't find grep")
endif()

# c++ compiler settings

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
## disable strict aliasing
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-strict-aliasing")
## disable warning about missing fields in an aggregate initializer
## This will avoid the warnings about the usage like this:
## struct A { int x, y, z; };
## A a{.x = 1, .y = 2}; // default initializes a.z to 0
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-field-initializers")
if("${CMAKE_BUILD_TYPE}x" STREQUAL "Debugx")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g2 -Wall -Wextra -Wno-unused-parameter -fno-omit-frame-pointer")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
endif()

# check if -E option works for the c++ compiler
execute_process(
    COMMAND bash -c "echo 'FOO' | ${CMAKE_CXX_COMPILER} -E -DFOO=BAR -"
    RESULT_VARIABLE CPP_TEST_RES
    OUTPUT_VARIABLE CPP_TEST_OUT)
set(CPP_FOUND 0)
if("${CPP_TEST_RES}" EQUAL 0)
    string(FIND "${CPP_TEST_OUT}" "FOO" CPP_TEST_POS)
    if("${CPP_TEST_POS}" EQUAL -1)
        set(CPP_FOUND 1)
    endif()
endif()
if(NOT CPP_FOUND)
    message(FATAL_ERROR
        "${CMAKE_CXX_COMPILER} -E does not work as a preprocessor")
endif()

# Check and configure external libraries

set(EXTERNAL_LIBS "${CMAKE_SOURCE_DIR}/external")
set(EXTERNAL_LIBS_INSTALL_DIR "${EXTERNAL_LIBS}/install")
list(APPEND CMAKE_PREFIX_PATH "${EXTERNAL_LIBS_INSTALL_DIR}")

# Check if -latomic is needed
file(READ ${PROJECT_SOURCE_DIR}/cmake/check_libatomic.cpp CHECK_LIBATOMIC_SRC)
check_cxx_source_compiles("${CHECK_LIBATOMIC_SRC}" NO_LIBATOMIC_NEEDED)
if (NOT NO_LIBATOMIC_NEEDED)
    # see https://github.com/HewlettPackard/foedus_code/blob/master/foedus-core/cmake/FindGccAtomic.cmake
    find_library(LIBATOMIC NAMES atomic atomic.so.1 libatomic.so.1)
    if (NOT LIBATOMIC)
        message(FATAL_ERROR "unable to find libatomic")
    endif()
    message(${LIBATOMIC})
    link_libraries(${LIBATOMIC})
    check_cxx_source_compiles("${CHECK_LIBATOMIC_SRC}" LIBATOMIC_OK)
    if (NOT LIBATOMIC_OK)
        message(FATAL_ERROR "libatomic does not work")
    endif()
endif()

# Check if <atomic> satisfies our requirements
file(READ ${PROJECT_SOURCE_DIR}/cmake/check_atomic.cpp CHECK_ATOMIC_SRC)
set(CMAKE_REQUIRED_LIBRARIES ${LIBATOMIC})
check_cxx_source_compiles("${CHECK_ATOMIC_SRC}" ATOMIC_OK)
if (NOT ATOMIC_OK)
    message(FATAL_ERROR "The <atomic> library does not satisfy the requirement")
endif()

# check if we have fallocate(2)
file(READ ${PROJECT_SOURCE_DIR}/cmake/check_fallocate.cpp CHECK_FALLOCATE_SRC)
check_cxx_source_compiles("${CHECK_FALLOCATE_SRC}" HAVE_FALLOCATE)

# check if we have strncasecmp(3)
check_symbol_exists(strncasecmp "strings.h" HAVE_STRNCASECMP)
if (NOT HAVE_STRNCASECMP)
    message(FATAL_ERROR "strncasecmp(3) not found")
endif()

# build and load jemalloc if needed
if(USE_JEMALLOC)
    if (PREFER_PREBUILT_JEMALLOC)
        # If not found, we'll build it below.
        pkg_check_modules(JEMALLOC jemalloc)
    endif()
    
    if (NOT JEMALLOC_FOUND)
        message(NOTICE "Building and loading jemalloc")
        set(JEMALLOC_SRCDIR "${EXTERNAL_LIBS}/jemalloc")
        string(CONCAT JEMALLOC_BUILD_SCRIPT
        "[ -f '${EXTERNAL_LIBS_INSTALL_DIR}/include/jemalloc/jemalloc.h' ] && "
        "echo 'jemalloc is already built. If the projects fails to build, "
        "please remove external/install/include/jemalloc/jemalloc.h to force a rebuild.' || ( "
        "CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} "
        "./autogen.sh --prefix='${EXTERNAL_LIBS_INSTALL_DIR}' && "
        "make clean && make && "
        "make install "
        ")"
        )
        execute_process(
            COMMAND bash -c "${JEMALLOC_BUILD_SCRIPT}"
            WORKING_DIRECTORY "${JEMALLOC_SRCDIR}"
            RESULT_VARIABLE EXIT_CODE
        )
        if (NOT ${EXIT_CODE} EQUAL 0)
            message(FATAL_ERROR "failed to build jemalloc")
        endif()

        # This time it is required to have the libjemalloc.
        pkg_check_modules(JEMALLOC REQUIRED jemalloc)
    endif()
    include_directories(${JEMALLOC_INCLUDEDIR})
    link_libraries(-L${JEMALLOC_LIBDIR} -l${JEMALLOC_LIBRARIES} -Wl,-rpath,${JEMALLOC_LIBDIR})
endif()

# adding pthread, add this before absl and gtest
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)

# need to enable testing before adding GoogleTest
enable_testing()

# This is a list of the names (without the absl:: or absl_ prefix) of the absl
# target we directly use.
set(ABSL_TARGET_NAMES
    log_severity
    flat_hash_map
    flat_hash_set
    flags
    flags_parse
    flags_usage
    memory
    strings
    str_format
)

if (PREBUILD_ABSL_AND_GTEST)
    pkg_check_modules(GMOCK gmock IMPORTED_TARGET GLOBAL)
    if (NOT GMOCK_FOUND)
        message(NOTICE "GoogleTest is not found, building...")
        set(GTEST_SRC_DIR "${EXTERNAL_LIBS}/googletest")
        set(GTEST_BUILD_DIR "${GTEST_SRC_DIR}/build")
        string(CONCAT GTEST_BUILD_SCRIPT
            "rm -rf '${GTEST_BUILD_DIR}' && '${CMAKE_COMMAND}' "
            "   -B '${GTEST_BUILD_DIR}'"
            "   -S '${GTEST_SRC_DIR}'"
            "   -DCMAKE_BUILD_TYPE=Release"
            "   -DCMAKE_CXX_STANDARD=11"
            "   -DCMAKE_INSTALL_PREFIX='${EXTERNAL_LIBS_INSTALL_DIR}'"
            "   -DCMAKE_CXX_COMPILER='${CMAKE_CXX_COMPILER}'"
            "   -DCMAKE_C_COMPILER='${CMAKE_C_COMPILER}'"
            "   -DBUILD_GMOCK=ON && "
            "'${CMAKE_COMMAND}' --build '${GTEST_BUILD_DIR}' --target install"
        )
        execute_process(
            COMMAND bash -c "${GTEST_BUILD_SCRIPT}"
            RESULT_VARIABLE EXIT_CODE)
        if (NOT ${EXIT_CODE} EQUAL 0)
            message(FATAL_ERROR "failed to build GoogleTest")
        endif()
        pkg_check_modules(GMOCK REQUIRED IMPORTED_TARGET GLOBAL gmock)
        if (NOT GMOCK_FOUND)
            message(FATAL_ERROR "unable to find GoogleTest")
        endif()
    endif()
    
    list(TRANSFORM ABSL_TARGET_NAMES PREPEND absl_
        OUTPUT_VARIABLE ABSL_MODULE_NAMES)
    pkg_check_modules(ABSL IMPORTED_TARGET GLOBAL ${ABSL_MODULE_NAMES})
    if (NOT ABSL_FOUND)
        message(NOTICE "Abeisl is not found, building...")
        set(ABSL_SRC_DIR "${EXTERNAL_LIBS}/abseil-cpp")
        set(ABSL_BUILD_DIR "${ABSL_SRC_DIR}/build")
        string(CONCAT ABSL_BUILD_SCRIPT
            "rm -rf '${ABSL_BUILD_DIR}' && '${CMAKE_COMMAND}' "
            "   -B '${ABSL_BUILD_DIR}'"
            "   -S '${ABSL_SRC_DIR}'"
            "   -DCMAKE_BUILD_TYPE=Release"
            "   -DCMAKE_CXX_STANDARD=11"
            "   -DCMAKE_INSTALL_PREFIX='${EXTERNAL_LIBS_INSTALL_DIR}'"
            "   -DCMAKE_CXX_COMPILER='${CMAKE_CXX_COMPILER}'"
            "   -DCMAKE_C_COMPILER='${CMAKE_C_COMPILER}'"
            "   -DABSL_PROPAGATE_CXX_STD=ON"
            "   -DABSL_ENABLE_INSTALL=ON && "
            "'${CMAKE_COMMAND}' --build '${ABSL_BUILD_DIR}' --target install"
        )
        execute_process(
            COMMAND bash -c "${ABSL_BUILD_SCRIPT}"
            RESULT_VARIABLE EXIT_CODE
        )
        if (NOT ${EXIT_CODE} EQUAL 0)
            message(FATAL_ERROR "failed to build Abeisl")
        endif()
        pkg_check_modules(ABSL REQUIRED IMPORTED_TARGET GLOBAL ${ABSL_MODULE_NAMES})
        if (NOT ABSL_FOUND)
            message(FATAL_ERROR "unable to find Abeisl")
        endif()
    endif()
    
    # See below in else branch.
    include_directories(${GMOCK_INCLUDE_DIRS})
    set(GMOCK_TARGET PkgConfig::GMOCK)
    include_directories(${ABSL_INCLUDE_DIRS})
    set(ABSL_TARGET PkgConfig::ABSL)
else()
    # adding gtest 
    add_subdirectory(${EXTERNAL_LIBS}/googletest)

    # adding abseil
    set(ABSL_PROPAGATE_CXX_STD ON)
    add_subdirectory(${EXTERNAL_LIBS}/abseil-cpp)
    
    # We add all these include paths globally as some of the object library
    # targets are not explicitly linked to the targets.
    # XXX this goes against the purpose of the imported targets, but it's
    # a bit too much effort to track the exact absl module dependencies for
    # each individual target...
    include_directories(
        ${EXTERNAL_LIBS}/googletest/googletest/include
        ${EXTERNAL_LIBS}/googletest/googlemock/include
        ${EXTERNAL_LIBS}/abseil-cpp
    )
    set(GMOCK_TARGET gmock)
    list(TRANSFORM ABSL_TARGET_NAMES PREPEND absl::
        OUTPUT_VARIABLE ABSL_TARGET)
endif()

# adding the GoogleTest module for gtest_add_test()
include(GoogleTest)

# Taco-DB project configuration
execute_process(
    COMMAND getconf LEVEL1_DCACHE_LINESIZE
    OUTPUT_VARIABLE CONFIG_L1_CACHELINE_SIZE
    RESULT_VARIABLE EXIT_CODE)
if (NOT ${EXIT_CODE} EQUAL 0)
    # l1 cache line size defaults to 64 bytes if getconf is N/A
    set(CONFIG_L1_CACHELINE_SIZE 64)
endif()

## Configurations

# 4KB page
# XXX This is currently fixed as many places assume 4KB page.
set(CONFIG_PAGE_SIZE 4096)

# don't force fsync by default
set(FORCE_FSYNC, 0)

if (NOT DEFINED USE_VOLATILE_CATCACHE)
    # defaults to use volatile catalog cache before we have heap file
    set(USE_VOLATILE_CATCACHE ON)
endif()

if (USE_VOLATILE_CATCACHE)
set(CATALOG_CACHE_HEADER "catalog/VolatileCatCache.h")
set(CATALOG_CACHE_CLASS "::taco::VolatileCatCache")
else()
message(FATAL_ERROR
    "can't use persistent catalog before we have a heap file implementation")
endif()

set(ALWAYS_USE_FIXEDLEN_DATAPAGE OFF)
set(ALWAYS_USE_VARLEN_DATAPAGE ON)

if (ALWAYS_USE_FIXEDLEN_DATAPAGE AND ALWAYS_USE_VARLEN_DATAPAGE)
    message(FATAL_ERROR
    "at most one of ALWAYS_USE_FIXEDLEN_DATAPAGE and ALWAYS_USE_VARLEN_DTAPAGE may be set to ON")
endif()


set(GENERATED_SOURCE_DIR ${CMAKE_BINARY_DIR}/generated_source)

# the temporary directory for test data
set(TEST_TMPDIR ${CMAKE_BINARY_DIR}/tmp)
file(MAKE_DIRECTORY "${TEST_TMPDIR}")
execute_process(
    COMMAND bash -c "chmod 0700 ${TEST_TMPDIR}"
    RESULT_VARIABLE CHMOD_RES
)
if(CHMOD_RES)
    message(FATAL_ERROR "unable to change the permission of ${TEST_TMPDIR} to 0700")
endif()

configure_file(config.h.in
	${GENERATED_SOURCE_DIR}/config.h)
configure_file(include/catalog/CatCache.h.in
    ${GENERATED_SOURCE_DIR}/catalog/CatCache.h)
include_directories("${GENERATED_SOURCE_DIR}" include)

## Targets

### The target for all generated source code (except config.h).
add_custom_target(AllGeneratedSource)

### subdirectories
add_subdirectory(src)

### add unit tests
add_subdirectory(tests)

