#ifndef EXPR_OPTYPES_H
#define EXPR_OPTYPES_H

#define OPTYPE_INVALID 0
#define OPTYPE_IMPLICIT_CAST 1
#define OPTYPE_CAST 2
#define OPTYPE_ADD 3
#define OPTYPE_SUB 4
#define OPTYPE_MUL 5
#define OPTYPE_DIV 6
#define OPTYPE_MOD 7
#define OPTYPE_LSH 8
#define OPTYPE_RSH 9
#define OPTYPE_EQ 10
#define OPTYPE_NE 11
#define OPTYPE_LT 12
#define OPTYPE_LE 13
#define OPTYPE_GT 14
#define OPTYPE_GE 15
#define OPTYPE_NOT 16
#define OPTYPE_OR 17
#define OPTYPE_AND 18
#define OPTYPE_BITNOT 19
#define OPTYPE_BITOR 20
#define OPTYPE_BITAND 21
#define OPTYPE_BITXOR 22
#define OPTYPE_NEG 23

#define NUM_OPTYPES 23

#ifdef OPTYPE_CONSTANT_ONLY
#define OPTYPE(optype) CONCAT(OPTYPE_, optype)

#else

#include "tdb.h"

namespace taco {

// typedef OpType moved to base/tdb_base.h

#define OPTYPE(optype) ((OpType) CONCAT(OPTYPE_, optype))

void InitOpTypes();
absl::string_view GetOpTypeSymbol(OpType optype);
OpType ParseOpTypeSymbol(absl::string_view str);
bool OpTypeIsUnary(OpType optype);
bool OpTypeIsBinary(OpType optype);

}   // namespace taco
#endif

#endif  // QUERY_EXPR_OPTYPES_H
