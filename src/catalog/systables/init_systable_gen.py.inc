// This file must be first processed by a C/C++ preprocessor.
// Must use C/C++-style comments.

// Load the util macros
#define INCLUDE_MACROS_ONLY
#include "config.h"
#include "utils/misc.h"

import sys

// Adding these two allows us to write C++ style true/false in the data files
true = True
false = False

#ifdef ALWAYS_USE_FIXEDLEN_DATAPAGE
config_always_use_fixedlen_datapage = True
#else
config_always_use_fixedlen_datapage = False
#endif

INVALID_OID = 0

// Load the funcname -> funcid mapping
#include "FunctionMapping.py.inc"

// Load the type column names as strings
// This is a special version of load_table.py.inc that does not resolve typid
optional_fields = {}
#define DEFINE_SYSTABLE(...)
#define DEFINE_SYSTABLE_FIELD(_1, colname, ...) \
    colname = STRINGIFY(colname)
#define DEFINE_SYSTABLE_FIELD_OPT(_1, colname, defaultval, ...) \
    colname, optional_fields[STRINGIFY(colname)] = \
    STRINGIFY(colname), defaultval
#define DEFINE_SYSTABLE_INDEX(...)
#include "Type.inc"
#undef DEFINE_SYSTABLE
#undef DEFINE_SYSTABLE_FIELD
#undef DEFINE_SYSTABLE_FIELD_OPT
#undef DEFINE_SYSTABLE_INDEX

datalist = [
#include "Type.dat"
]

// Complete the default values
for t in datalist:
    for col in optional_fields:
        if col not in t:
            t[col] = optional_fields[col]

optional_typfields = optional_fields
typlist = datalist

// Build the type mapping
typname2idx = {}
for i in range(len(typlist)):
    typname2idx[typlist[i][typname]] = i

// Only load the types we need for building the system catalog into the python
// environment
bootstrap_typnames = ["OID", "BOOL", "UINT1", "INT2", "UINT8", "UINT4",
                      "VARCHAR"]
for tname in bootstrap_typnames:
    t = typlist[typname2idx[tname]]
    if t[typname] == "VARCHAR":
        exec("{}_oid = {}".format(t[typname], t[typid]))
        exec("def {}(x):\n    return {}_oid".format(t[typname], t[typname]))
    else:
        exec("{} = {}".format(t[typname], t[typid]))

ctype_mapping = {
    OID : "Oid",
    BOOL : "bool",
    UINT1 : "uint8_t",
    INT2 : "int16_t",
    UINT8 : "uint64_t",
    UINT4 : "uint32_t",
    VARCHAR_oid : "std::string",
}

def get_ctype(oid):
    if oid < 0:
        return "std::vector<" + get_ctype(-oid) + ">"
    if oid not in ctype_mapping:
        print("unsupported systable type: {}", oid)
        sys.exit(1)
    return ctype_mapping[oid]

datum_getter_mapping = {
    OID : "GetOid",
    BOOL : "GetBool",
    UINT1 : "GetUInt8",
    INT2 : "GetInt16",
    UINT8 : "GetUInt64",
    UINT4 : "GetUInt32",
    VARCHAR_oid : "GetThis"
}

def get_datum_getter(oid):
    if oid < 0:
        return "GetVarlenAsStringView"
    if oid not in datum_getter_mapping:
        print("unsupported systable type: {}", oid)
        sys.exit(1)
    return datum_getter_mapping[oid]

datum_creator_mapping = {
    OID : "Datum::From",
    BOOL : "Datum::From",
    UINT1 : "Datum::From",
    INT2 : "Datum::From",
    UINT8 : "Datum::From",
    UINT4 : "Datum::From",
    VARCHAR_oid : "STRING_TO_VARCHAR"
}

def get_datum_creator(oid):
    if oid < 0:
        return "array not supported yet"
    if oid not in datum_creator_mapping:
        print("unsupported systable type: {}", oid)
        sys.exit(1)
    return datum_creator_mapping[oid]


#define ARRAY * -1

// support for extracting typparam
X_OID = 0
X_BOOL = 0
X_UINT1 = 0
X_INT2 = 0
X_UINT8 = 0
X_UINT4 = 0
def X_VARCHAR(x):
    return x


