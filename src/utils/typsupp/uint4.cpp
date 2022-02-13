#ifndef UTILS_TYPSUPP_UINT4_H
#define UTILS_TYPSUPP_UINT4_H


#include "tdb.h"

#include <cinttypes>

#include <absl/strings/numbers.h>

#include "utils/builtin_funcs.h"
#include "utils/numbers.h"

namespace taco {

namespace {

// The longest 64-bit integer is 20 digits long. Adding a possible sign and
// the space for '\0' gives us 22.
constexpr const size_t STRING_OUTPUT_BUFLEN = 22;
}

BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(UINT4_in, 680)
BUILTIN_ARGTYPE(__STRING)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    uint32_t val;
    if (!SimpleAtoiWrapper(str, &val)) {
        LOG(kError, "cannot parse \"%s\" as a UINT4", str);
    }
    return Datum::From(val);
}

BUILTIN_RETTYPE(__STRING)
BUILTIN_FUNC(UINT4_out, 681)
BUILTIN_ARGTYPE(UINT4)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint32_t val = FMGR_ARG(0).GetUInt32();
    auto buffer = unique_malloc(STRING_OUTPUT_BUFLEN);
    absl::SNPrintF((char*) buffer.get(), STRING_OUTPUT_BUFLEN, "%" PRIu32, val);
    return Datum::FromVarlenBytes(std::move(buffer),
                                  strlen((char*) buffer.get()));
}

BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(UINT4_add, 682)
BUILTIN_ARGTYPE(UINT4, UINT4)
BUILTIN_OPR(ADD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t arg1 = FMGR_ARG(1).GetUInt32();
    uint32_t res = arg0 + arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(UINT4_sub, 683)
BUILTIN_ARGTYPE(UINT4, UINT4)
BUILTIN_OPR(SUB)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t arg1 = FMGR_ARG(1).GetUInt32();
    uint32_t res = arg0 - arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(UINT4_mul, 684)
BUILTIN_ARGTYPE(UINT4, UINT4)
BUILTIN_OPR(MUL)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t arg1 = FMGR_ARG(1).GetUInt32();
    uint32_t res = arg0 * arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(UINT4_div, 685)
BUILTIN_ARGTYPE(UINT4, UINT4)
BUILTIN_OPR(DIV)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg1 = FMGR_ARG(1).GetUInt32();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t res = arg0 / arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(UINT4_mod, 686)
BUILTIN_ARGTYPE(UINT4, UINT4)
BUILTIN_OPR(MOD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg1 = FMGR_ARG(1).GetUInt32();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t res = arg0 % arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(UINT4_neg, 687)
BUILTIN_ARGTYPE(UINT4)
BUILTIN_OPR(NEG)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t res = -arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT4_eq, 688)
BUILTIN_ARGTYPE(UINT4, UINT4)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t arg1 = FMGR_ARG(1).GetUInt32();
    bool res = arg0 == arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT4_ne, 689)
BUILTIN_ARGTYPE(UINT4, UINT4)
BUILTIN_OPR(NE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t arg1 = FMGR_ARG(1).GetUInt32();
    bool res = arg0 != arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT4_lt, 690)
BUILTIN_ARGTYPE(UINT4, UINT4)
BUILTIN_OPR(LT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t arg1 = FMGR_ARG(1).GetUInt32();
    bool res = arg0 < arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT4_le, 691)
BUILTIN_ARGTYPE(UINT4, UINT4)
BUILTIN_OPR(LE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t arg1 = FMGR_ARG(1).GetUInt32();
    bool res = arg0 <= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT4_gt, 692)
BUILTIN_ARGTYPE(UINT4, UINT4)
BUILTIN_OPR(GT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t arg1 = FMGR_ARG(1).GetUInt32();
    bool res = arg0 > arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT4_ge, 693)
BUILTIN_ARGTYPE(UINT4, UINT4)
BUILTIN_OPR(GE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t arg1 = FMGR_ARG(1).GetUInt32();
    bool res = arg0 >= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(UINT4_not, 694)
BUILTIN_ARGTYPE(UINT4)
BUILTIN_OPR(BITNOT)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t res = ~arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(UINT4_and, 695)
BUILTIN_ARGTYPE(UINT4, UINT4)
BUILTIN_OPR(BITAND)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t arg1 = FMGR_ARG(1).GetUInt32();
    uint32_t res = (arg0 & arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(UINT4_or, 696)
BUILTIN_ARGTYPE(UINT4, UINT4)
BUILTIN_OPR(BITOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t arg1 = FMGR_ARG(1).GetUInt32();
    uint32_t res = (arg0 | arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(UINT4_xor, 697)
BUILTIN_ARGTYPE(UINT4, UINT4)
BUILTIN_OPR(BITXOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t arg1 = FMGR_ARG(1).GetUInt32();
    uint32_t res = (arg0 ^ arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(UINT4_lsh, 698)
BUILTIN_ARGTYPE(UINT4, UINT1)
BUILTIN_OPR(LSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t arg1 = FMGR_ARG(1).GetUInt32();
    uint32_t res = (arg0 << arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(UINT4_rsh, 699)
BUILTIN_ARGTYPE(UINT4, UINT1)
BUILTIN_OPR(RSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint32_t arg0 = FMGR_ARG(0).GetUInt32();
    uint32_t arg1 = FMGR_ARG(1).GetUInt8();
    uint32_t res = (arg0 >> arg1);
    return Datum::From(res);
}

}   // namespace taco

#endif      // UTILS_TYPSUPP_UINT4_H
