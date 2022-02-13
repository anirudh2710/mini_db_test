#ifndef UTILS_TYPSUPP_UINT8_H
#define UTILS_TYPSUPP_UINT8_H


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

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_in, 710)
BUILTIN_ARGTYPE(__STRING)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    uint64_t val;
    if (!SimpleAtoiWrapper(str, &val)) {
        LOG(kError, "cannot parse \"%s\" as a UINT8", str);
    }
    return Datum::From(val);
}

BUILTIN_RETTYPE(__STRING)
BUILTIN_FUNC(UINT8_out, 711)
BUILTIN_ARGTYPE(UINT8)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint64_t val = FMGR_ARG(0).GetUInt64();
    auto buffer = unique_malloc(STRING_OUTPUT_BUFLEN);
    absl::SNPrintF((char*) buffer.get(), STRING_OUTPUT_BUFLEN, "%" PRIu64, val);
    return Datum::FromVarlenBytes(std::move(buffer),
                                  strlen((char*) buffer.get()));
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_add, 712)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(ADD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    uint64_t res = arg0 + arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_sub, 713)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(SUB)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    uint64_t res = arg0 - arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_mul, 714)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(MUL)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    uint64_t res = arg0 * arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_div, 715)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(DIV)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t res = arg0 / arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_mod, 716)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(MOD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t res = arg0 % arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_neg, 717)
BUILTIN_ARGTYPE(UINT8)
BUILTIN_OPR(NEG)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t res = -arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT8_eq, 718)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    bool res = arg0 == arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT8_ne, 719)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(NE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    bool res = arg0 != arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT8_lt, 720)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(LT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    bool res = arg0 < arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT8_le, 721)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(LE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    bool res = arg0 <= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT8_gt, 722)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(GT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    bool res = arg0 > arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT8_ge, 723)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(GE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    bool res = arg0 >= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_not, 724)
BUILTIN_ARGTYPE(UINT8)
BUILTIN_OPR(BITNOT)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t res = ~arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_and, 725)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(BITAND)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    uint64_t res = (arg0 & arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_or, 726)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(BITOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    uint64_t res = (arg0 | arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_xor, 727)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(BITXOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    uint64_t res = (arg0 ^ arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_lsh, 728)
BUILTIN_ARGTYPE(UINT8, UINT1)
BUILTIN_OPR(LSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    uint64_t res = (arg0 << arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_rsh, 729)
BUILTIN_ARGTYPE(UINT8, UINT1)
BUILTIN_OPR(RSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt8();
    uint64_t res = (arg0 >> arg1);
    return Datum::From(res);
}

}   // namespace taco

#endif      // UTILS_TYPSUPP_UINT8_H
