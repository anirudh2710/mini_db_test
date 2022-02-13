#ifndef UTILS_TYPSUPP_OID_H
#define UTILS_TYPSUPP_OID_H


#include "tdb.h"

#include <cinttypes>

#include "utils/builtin_funcs.h"
#include "utils/numbers.h"

namespace taco {

namespace {

// The longest 64-bit integer is 20 digits long. Adding a possible sign and
// the space for '\0' gives us 22.
constexpr const size_t STRING_OUTPUT_BUFLEN = 22;
}

BUILTIN_RETTYPE(OID)
BUILTIN_FUNC(OID_in, 740)
BUILTIN_ARGTYPE(__STRING)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    Oid val;
    if (!SimpleAtoiWrapper(str, &val)) {
        LOG(kError, "cannot parse \"%s\" as a OID", str);
    }
    return Datum::From(val);
}

BUILTIN_RETTYPE(__STRING)
BUILTIN_FUNC(OID_out, 741)
BUILTIN_ARGTYPE(OID)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    Oid val = FMGR_ARG(0).GetOid();
    auto buffer = unique_malloc(STRING_OUTPUT_BUFLEN);
    absl::SNPrintF((char*) buffer.get(), STRING_OUTPUT_BUFLEN, "%" PRIu32, val);
    return Datum::FromVarlenBytes(std::move(buffer),
                                  strlen((char*) buffer.get()));
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(OID_eq, 742)
BUILTIN_ARGTYPE(OID, OID)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    Oid arg0 = FMGR_ARG(0).GetUInt32();
    Oid arg1 = FMGR_ARG(1).GetUInt32();
    bool res = arg0 == arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(OID_ne, 743)
BUILTIN_ARGTYPE(OID, OID)
BUILTIN_OPR(NE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    Oid arg0 = FMGR_ARG(0).GetUInt32();
    Oid arg1 = FMGR_ARG(1).GetUInt32();
    bool res = arg0 != arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(OID_lt, 744)
BUILTIN_ARGTYPE(OID, OID)
BUILTIN_OPR(LT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    Oid arg0 = FMGR_ARG(0).GetUInt32();
    Oid arg1 = FMGR_ARG(1).GetUInt32();
    bool res = arg0 < arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(OID_le, 745)
BUILTIN_ARGTYPE(OID, OID)
BUILTIN_OPR(LE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    Oid arg0 = FMGR_ARG(0).GetUInt32();
    Oid arg1 = FMGR_ARG(1).GetUInt32();
    bool res = arg0 <= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(OID_gt, 746)
BUILTIN_ARGTYPE(OID, OID)
BUILTIN_OPR(GT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    Oid arg0 = FMGR_ARG(0).GetUInt32();
    Oid arg1 = FMGR_ARG(1).GetUInt32();
    bool res = arg0 > arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(OID_ge, 747)
BUILTIN_ARGTYPE(OID, OID)
BUILTIN_OPR(GE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    Oid arg0 = FMGR_ARG(0).GetUInt32();
    Oid arg1 = FMGR_ARG(1).GetUInt32();
    bool res = arg0 >= arg1;
    return Datum::From(res);
}

}   // namespace taco

#endif      // UTILS_TYPSUPP_OID_H
