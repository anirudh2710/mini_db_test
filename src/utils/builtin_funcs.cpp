#include "utils/builtin_funcs.h"

#include <absl/container/flat_hash_map.h>

#include "catalog/systables/builtin_func_table.h"

namespace taco {

namespace {
    absl::flat_hash_map<Oid, FunctionPtr> builtin_func_lookup_table;
}   // anonymous namespace

void
InitBuiltinFunctions() {
    builtin_func_lookup_table.clear();

    for (const auto &p : builtin_func_table) {
        builtin_func_lookup_table[p.first] = p.second;
    }
}

FunctionInfo
FindBuiltinFunction(Oid oid) {
    auto iter = builtin_func_lookup_table.find(oid);
    if (iter == builtin_func_lookup_table.end()) {
        return FunctionInfo();
    }
    return FunctionInfo(iter->second);
}

}   // namespace taco
