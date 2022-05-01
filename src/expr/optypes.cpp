#include "expr/optypes.h"

#include <absl/container/flat_hash_map.h>

namespace taco {

static const char* s_optype_symbols[] = {
    "#invalid",
    "#implicit cast",
    "#cast",
    "+",
    "-",
    "*",
    "/",
    "%",
    "<<",
    ">>",
    "=",
    "<>",
    "<",
    "<=",
    ">",
    ">=",
    "NOT",
    "AND",
    "OR",
    "~",
    "|",
    "&",
    "^",
    "-",
};

static FieldId s_num_operands[] = {
    0,
    1, // implicit cast
    1, // cast
    2, // +
    2, // -
    2, // *
    2, // /
    2, // %
    2, // <<
    2, // >>
    2, // =
    2, // <>
    2, // <
    2, // <=
    2, // >
    2, // >=
    1, // NOT
    2, // AND
    2, // OR
    1, // ~
    2, // |
    2, // &
    2, // ^
    1,  // -
};

static absl::flat_hash_map<std::string, OpType> s_optype_lookup_table;

void
InitOpTypes() {
    s_optype_lookup_table.clear();
    for (OpType optype = 1; optype <= NUM_OPTYPES; ++optype) {
        s_optype_lookup_table.emplace(s_optype_symbols[optype], optype);
    }
}

absl::string_view
GetOpTypeSymbol(OpType optype) {
    if (optype > NUM_OPTYPES) {
        return s_optype_symbols[OPTYPE(INVALID)];
    }
    return s_optype_symbols[optype];
}

OpType
ParseOpTypesSymbol(absl::string_view str) {
    auto iter = s_optype_lookup_table.find(str);
    if (iter == s_optype_lookup_table.end()) {
        return OPTYPE(INVALID);
    }
    return iter->second;
}

bool
OpTypeIsUnary(OpType optype) {
    if (optype > NUM_OPTYPES)
        return false;
    return s_num_operands[optype] == 1;
}

bool
OpTypeIsBinary(OpType optype) {
    if (optype > NUM_OPTYPES)
        return false;
    return s_num_operands[optype] == 2;
}


}   // namespace taco
