#ifndef UTILS_TYPSUPP_UINT1_H
#define UTILS_TYPSUPP_UINT1_H


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

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_in, 620)
BUILTIN_ARGTYPE(__STRING)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    uint8_t val;
    if (!SimpleAtoiWrapper(str, &val)) {
        LOG(kError, "cannot parse \"%s\" as a UINT1", str);
    }
    return Datum::From(val);
}

BUILTIN_RETTYPE(__STRING)
BUILTIN_FUNC(UINT1_out, 621)
BUILTIN_ARGTYPE(UINT1)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint8_t val = FMGR_ARG(0).GetUInt8();
    auto buffer = unique_malloc(STRING_OUTPUT_BUFLEN);
    absl::SNPrintF((char*) buffer.get(), STRING_OUTPUT_BUFLEN, "%" PRIu8, val);
    return Datum::FromVarlenBytes(std::move(buffer),
                                  strlen((char*) buffer.get()));
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_add, 622)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(ADD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    uint8_t res = arg0 + arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_sub, 623)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(SUB)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    uint8_t res = arg0 - arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_mul, 624)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(MUL)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    uint8_t res = arg0 * arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_div, 625)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(DIV)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t res = arg0 / arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_mod, 626)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(MOD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t res = arg0 % arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_neg, 627)
BUILTIN_ARGTYPE(UINT1)
BUILTIN_OPR(NEG)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t res = -arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT1_eq, 628)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    bool res = arg0 == arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT1_ne, 629)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(NE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    bool res = arg0 != arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT1_lt, 630)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(LT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    bool res = arg0 < arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT1_le, 631)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(LE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    bool res = arg0 <= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT1_gt, 632)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(GT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    bool res = arg0 > arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT1_ge, 633)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(GE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    bool res = arg0 >= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_not, 634)
BUILTIN_ARGTYPE(UINT1)
BUILTIN_OPR(BITNOT)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t res = ~arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_and, 635)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(BITAND)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    uint8_t res = (arg0 & arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_or, 636)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(BITOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    uint8_t res = (arg0 | arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_xor, 637)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(BITXOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    uint8_t res = (arg0 ^ arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_lsh, 638)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(LSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    uint8_t res = (arg0 << arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_rsh, 639)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(RSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    uint8_t res = (arg0 >> arg1);
    return Datum::From(res);
}

}   // namespace taco

#endif      // UTILS_TYPSUPP_UINT1_H
