#ifndef UTILS_TYPSUPP_UINT2_H
#define UTILS_TYPSUPP_UINT2_H


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

BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(UINT2_in, 650)
BUILTIN_ARGTYPE(__STRING)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    uint16_t val;
    if (!SimpleAtoiWrapper(str, &val)) {
        LOG(kError, "cannot parse \"%s\" as a UINT2", str);
    }
    return Datum::From(val);
}

BUILTIN_RETTYPE(__STRING)
BUILTIN_FUNC(UINT2_out, 651)
BUILTIN_ARGTYPE(UINT2)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint16_t val = FMGR_ARG(0).GetUInt16();
    auto buffer = unique_malloc(STRING_OUTPUT_BUFLEN);
    absl::SNPrintF((char*) buffer.get(), STRING_OUTPUT_BUFLEN, "%" PRIu16, val);
    return Datum::FromVarlenBytes(std::move(buffer),
                                  strlen((char*) buffer.get()));
}

BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(UINT2_add, 652)
BUILTIN_ARGTYPE(UINT2, UINT2)
BUILTIN_OPR(ADD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t arg1 = FMGR_ARG(1).GetUInt16();
    uint16_t res = arg0 + arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(UINT2_sub, 653)
BUILTIN_ARGTYPE(UINT2, UINT2)
BUILTIN_OPR(SUB)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t arg1 = FMGR_ARG(1).GetUInt16();
    uint16_t res = arg0 - arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(UINT2_mul, 654)
BUILTIN_ARGTYPE(UINT2, UINT2)
BUILTIN_OPR(MUL)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t arg1 = FMGR_ARG(1).GetUInt16();
    uint16_t res = arg0 * arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(UINT2_div, 655)
BUILTIN_ARGTYPE(UINT2, UINT2)
BUILTIN_OPR(DIV)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg1 = FMGR_ARG(1).GetUInt16();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t res = arg0 / arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(UINT2_mod, 656)
BUILTIN_ARGTYPE(UINT2, UINT2)
BUILTIN_OPR(MOD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg1 = FMGR_ARG(1).GetUInt16();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t res = arg0 % arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(UINT2_neg, 657)
BUILTIN_ARGTYPE(UINT2)
BUILTIN_OPR(NEG)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t res = -arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT2_eq, 658)
BUILTIN_ARGTYPE(UINT2, UINT2)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t arg1 = FMGR_ARG(1).GetUInt16();
    bool res = arg0 == arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT2_ne, 659)
BUILTIN_ARGTYPE(UINT2, UINT2)
BUILTIN_OPR(NE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t arg1 = FMGR_ARG(1).GetUInt16();
    bool res = arg0 != arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT2_lt, 660)
BUILTIN_ARGTYPE(UINT2, UINT2)
BUILTIN_OPR(LT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t arg1 = FMGR_ARG(1).GetUInt16();
    bool res = arg0 < arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT2_le, 661)
BUILTIN_ARGTYPE(UINT2, UINT2)
BUILTIN_OPR(LE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t arg1 = FMGR_ARG(1).GetUInt16();
    bool res = arg0 <= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT2_gt, 662)
BUILTIN_ARGTYPE(UINT2, UINT2)
BUILTIN_OPR(GT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t arg1 = FMGR_ARG(1).GetUInt16();
    bool res = arg0 > arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT2_ge, 663)
BUILTIN_ARGTYPE(UINT2, UINT2)
BUILTIN_OPR(GE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t arg1 = FMGR_ARG(1).GetUInt16();
    bool res = arg0 >= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(UINT2_not, 664)
BUILTIN_ARGTYPE(UINT2)
BUILTIN_OPR(BITNOT)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t res = ~arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(UINT2_and, 665)
BUILTIN_ARGTYPE(UINT2, UINT2)
BUILTIN_OPR(BITAND)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t arg1 = FMGR_ARG(1).GetUInt16();
    uint16_t res = (arg0 & arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(UINT2_or, 666)
BUILTIN_ARGTYPE(UINT2, UINT2)
BUILTIN_OPR(BITOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t arg1 = FMGR_ARG(1).GetUInt16();
    uint16_t res = (arg0 | arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(UINT2_xor, 667)
BUILTIN_ARGTYPE(UINT2, UINT2)
BUILTIN_OPR(BITXOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t arg1 = FMGR_ARG(1).GetUInt16();
    uint16_t res = (arg0 ^ arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(UINT2_lsh, 668)
BUILTIN_ARGTYPE(UINT2, UINT1)
BUILTIN_OPR(LSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t arg1 = FMGR_ARG(1).GetUInt16();
    uint16_t res = (arg0 << arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(UINT2_rsh, 669)
BUILTIN_ARGTYPE(UINT2, UINT1)
BUILTIN_OPR(RSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint16_t arg0 = FMGR_ARG(0).GetUInt16();
    uint16_t arg1 = FMGR_ARG(1).GetUInt8();
    uint16_t res = (arg0 >> arg1);
    return Datum::From(res);
}

}   // namespace taco

#endif      // UTILS_TYPSUPP_UINT2_H
