# src/catalog/systables/CMakeLists.txt

set(SysTableNames "")
list(APPEND SysTableNames
		Column
		Function
		Table
		Type
        FunctionArgs
        Operator
        Index
        IndexColumn)
list(TRANSFORM SysTableNames APPEND .inc OUTPUT_VARIABLE SysTableIncludeFiles)

find_package(Python3)

set(AllGeneratedCppFiles)

add_custom_command(
    OUTPUT "${GENERATED_SOURCE_DIR}/catalog/systables/FunctionMapping.py.inc"
        "${GENERATED_SOURCE_DIR}/catalog/systables/Function.dat"
        "${GENERATED_SOURCE_DIR}/catalog/systables/FunctionArgs.dat"
        "${GENERATED_SOURCE_DIR}/catalog/systables/Operator.dat"
        "${GENERATED_SOURCE_DIR}/catalog/systables/builtin_func_table.h"
        "${GENERATED_SOURCE_DIR}/catalog/systables/builtin_func_table.cpp"
    COMMAND "${CMAKE_COMMAND}" -E env
        PYTHON3="${Python3_EXECUTABLE}"
        CXX="${CMAKE_CXX_COMPILER}"
        bash "${CMAKE_CURRENT_SOURCE_DIR}/gen_builtin_func_data.sh"
        "${GENERATED_SOURCE_DIR}/catalog/systables"
        ${FILES_WITH_BUILTIN_FUNCS}
    MAIN_DEPENDENCY gen_builtin_func_data.sh
    DEPENDS
        init_systable_gen.py.inc
        "${GENERATED_SOURCE_DIR}/config.h"
        # FunctionMapping.py.inc is generated by this command
        Type.inc
        Type.dat
        Function.inc
        load_table.py.inc
        "${CMAKE_SOURCE_DIR}/include/utils/misc.h"
        ${FILES_WITH_BUILTIN_FUNCS}
)
add_custom_target(FunctionData
    DEPENDS 
        "${GENERATED_SOURCE_DIR}/catalog/systables/FunctionMapping.py.inc"
        "${GENERATED_SOURCE_DIR}/catalog/systables/Function.dat"
        "${GENERATED_SOURCE_DIR}/catalog/systables/FunctionArgs.dat"
        "${GENERATED_SOURCE_DIR}/catalog/systables/Operator.dat"
        "${GENERATED_SOURCE_DIR}/catalog/systables/builtin_func_table.h"
        "${GENERATED_SOURCE_DIR}/catalog/systables/builtin_func_table.cpp")
list(APPEND AllGeneratedCppFiles
    "${GENERATED_SOURCE_DIR}/catalog/systables/builtin_func_table.cpp")

add_custom_target(AllSysTables)
foreach(SysTable IN LISTS SysTableNames)
	add_custom_command(
        OUTPUT "${GENERATED_SOURCE_DIR}/catalog/systables/${SysTable}.h"
               "${GENERATED_SOURCE_DIR}/catalog/systables/${SysTable}.cpp"
		COMMAND "${CMAKE_COMMAND}" -E env
            PYTHON3="${Python3_EXECUTABLE}"
            CXX="${CMAKE_CXX_COMPILER}"
			bash "${CMAKE_CURRENT_SOURCE_DIR}/gen_systable.sh"
			"${CMAKE_CURRENT_SOURCE_DIR}/${SysTable}.inc"
			"${GENERATED_SOURCE_DIR}/catalog/systables"
		MAIN_DEPENDENCY gen_systable.sh
		DEPENDS
            init_systable_gen.py.inc
            "${GENERATED_SOURCE_DIR}/config.h"
            "${GENERATED_SOURCE_DIR}/catalog/systables/FunctionMapping.py.inc"
            Type.inc
            Type.dat
            Function.inc
            "${CMAKE_SOURCE_DIR}/include/utils/misc.h"
			${SysTable}.inc
			systable.h.template
            systable.cpp.template
    )
	add_custom_target(SysTable_${SysTable}
		DEPENDS "${GENERATED_SOURCE_DIR}/catalog/systables/${SysTable}.h"
                "${GENERATED_SOURCE_DIR}/catalog/systables/${SysTable}.cpp")
    list(APPEND AllGeneratedCppFiles
        "${GENERATED_SOURCE_DIR}/catalog/systables/${SysTable}.cpp")
	add_dependencies(AllSysTables SysTable_${SysTable})
endforeach()

add_custom_command(
    OUTPUT "${GENERATED_SOURCE_DIR}/catalog/systables/bootstrap_data.h"
           "${GENERATED_SOURCE_DIR}/catalog/systables/bootstrap_data.cpp"
    COMMAND "${CMAKE_COMMAND}" -E env
        PYTHON3="${Python3_EXECUTABLE}"
        CXX="${CMAKE_CXX_COMPILER}"
        bash "${CMAKE_CURRENT_SOURCE_DIR}/gen_bootstrap_data.sh"
        "${GENERATED_SOURCE_DIR}/catalog/systables"
    MAIN_DEPENDENCY gen_bootstrap_data.sh
    DEPENDS
        init_systable_gen.py.inc
        "${GENERATED_SOURCE_DIR}/config.h"
        "${GENERATED_SOURCE_DIR}/catalog/systables/FunctionMapping.py.inc"
        Type.inc
        Type.dat
        Function.inc
        "${CMAKE_SOURCE_DIR}/include/utils/misc.h"
        Column.inc
        Table.inc
        "${GENERATED_SOURCE_DIR}/catalog/systables/Table.dat"
        "${GENERATED_SOURCE_DIR}/catalog/systables/Function.dat"
        load_table.py.inc
)
add_custom_target(bootstrap_data
    DEPENDS "${GENERATED_SOURCE_DIR}/catalog/systables/bootstrap_data.h"
            "${GENERATED_SOURCE_DIR}/catalog/systables/bootstrap_data.cpp")
list(APPEND AllGeneratedCppFiles
     "${GENERATED_SOURCE_DIR}/catalog/systables/bootstrap_data.cpp")

add_custom_command(
    OUTPUT "${GENERATED_SOURCE_DIR}/catalog/systables/Table.dat"
        "${GENERATED_SOURCE_DIR}/catalog/systables/Column.dat"
        "${GENERATED_SOURCE_DIR}/catalog/systables/Index.dat"
        "${GENERATED_SOURCE_DIR}/catalog/systables/IndexColumn.dat"
        "${GENERATED_SOURCE_DIR}/catalog/systables/init.dat"
        "${GENERATED_SOURCE_DIR}/catalog/systables/initoids.h"
        "${GENERATED_SOURCE_DIR}/catalog/systables/CatCacheBase_gen.cpp"
    COMMAND "${CMAKE_COMMAND}" -E env
        PYTHON3="${Python3_EXECUTABLE}"
        CXX="${CMAKE_CXX_COMPILER}"
        bash "${CMAKE_CURRENT_SOURCE_DIR}/gen_init_data.sh"
        "${GENERATED_SOURCE_DIR}/catalog/systables"
    MAIN_DEPENDENCY gen_init_data.sh
    DEPENDS 
        "${GENERATED_SOURCE_DIR}/config.h"
        "${GENERATED_SOURCE_DIR}/catalog/systables/FunctionMapping.py.inc"
        "${CMAKE_SOURCE_DIR}/include/utils/misc.h"
        ${SysTableIncludeFiles}
        Type.dat
    )
add_custom_target(init_data
    DEPENDS "${GENERATED_SOURCE_DIR}/catalog/systables/Table.dat"
        "${GENERATED_SOURCE_DIR}/catalog/systables/Column.dat"
        "${GENERATED_SOURCE_DIR}/catalog/systables/Index.dat"
        "${GENERATED_SOURCE_DIR}/catalog/systables/IndexColumn.dat"
        "${GENERATED_SOURCE_DIR}/catalog/systables/init.dat"
        "${GENERATED_SOURCE_DIR}/catalog/systables/initoids.h"
        "${GENERATED_SOURCE_DIR}/catalog/systables/CatCacheBase_gen.cpp")
list(APPEND AllGeneratedCppFiles
    "${GENERATED_SOURCE_DIR}/catalog/systables/CatCacheBase_gen.cpp")

add_custom_command(TARGET init_data POST_BUILD
    COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/check_oids.sh"
            "${GENERATED_SOURCE_DIR}/catalog/systables/init.dat"
)

add_dependencies(AllGeneratedSource AllSysTables)
add_dependencies(AllGeneratedSource bootstrap_data)
add_dependencies(AllGeneratedSource FunctionData)
add_dependencies(AllGeneratedSource init_data)


add_tdb_object_library(catalog_systables ${AllGeneratedCppFiles})

