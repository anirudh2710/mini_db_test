# src/expr/CMakeLists.txt

set(EXPR_LIB_SRC
    optypes.cpp
    AndOperator.cpp
    BinaryOperator.cpp
    Cast.cpp
    Literal.cpp
    OrOperator.cpp
    UnaryOperator.cpp
    Variable.cpp
    FuncCallOperator.cpp
    )

add_tdb_object_library(expr ${EXPR_LIB_SRC})

