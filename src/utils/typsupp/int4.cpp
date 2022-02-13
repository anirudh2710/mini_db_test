#ifndef UTILS_TYPSUPP_INT4_H
#define UTILS_TYPSUPP_INT4_H


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

BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(INT4_in, 560)
BUILTIN_ARGTYPE(__STRING)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    int32_t val;
    if (!SimpleAtoiWrapper(str, &val)) {
        LOG(kError, "cannot parse \"%s\" as a INT4", str);
    }
    return Datum::From(val);
}

BUILTIN_RETTYPE(__STRING)
BUILTIN_FUNC(INT4_out, 561)
BUILTIN_ARGTYPE(INT4)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    int32_t val = FMGR_ARG(0).GetInt32();
    auto buffer = unique_malloc(STRING_OUTPUT_BUFLEN);
    absl::SNPrintF((char*) buffer.get(), STRING_OUTPUT_BUFLEN, "%" PRId32, val);
    return Datum::FromVarlenBytes(std::move(buffer),
                                  strlen((char*) buffer.get()));
}

BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(INT4_add, 562)
BUILTIN_ARGTYPE(INT4, INT4)
BUILTIN_OPR(ADD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t arg1 = FMGR_ARG(1).GetInt32();
    int32_t res = arg0 + arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(INT4_sub, 563)
BUILTIN_ARGTYPE(INT4, INT4)
BUILTIN_OPR(SUB)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t arg1 = FMGR_ARG(1).GetInt32();
    int32_t res = arg0 - arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(INT4_mul, 564)
BUILTIN_ARGTYPE(INT4, INT4)
BUILTIN_OPR(MUL)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t arg1 = FMGR_ARG(1).GetInt32();
    int32_t res = arg0 * arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(INT4_div, 565)
BUILTIN_ARGTYPE(INT4, INT4)
BUILTIN_OPR(DIV)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg1 = FMGR_ARG(1).GetInt32();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t res = arg0 / arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(INT4_mod, 566)
BUILTIN_ARGTYPE(INT4, INT4)
BUILTIN_OPR(MOD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg1 = FMGR_ARG(1).GetInt32();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t res = arg0 % arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(INT4_neg, 567)
BUILTIN_ARGTYPE(INT4)
BUILTIN_OPR(NEG)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t res = -arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT4_eq, 568)
BUILTIN_ARGTYPE(INT4, INT4)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t arg1 = FMGR_ARG(1).GetInt32();
    bool res = arg0 == arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT4_ne, 569)
BUILTIN_ARGTYPE(INT4, INT4)
BUILTIN_OPR(NE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t arg1 = FMGR_ARG(1).GetInt32();
    bool res = arg0 != arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT4_lt, 570)
BUILTIN_ARGTYPE(INT4, INT4)
BUILTIN_OPR(LT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t arg1 = FMGR_ARG(1).GetInt32();
    bool res = arg0 < arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT4_le, 571)
BUILTIN_ARGTYPE(INT4, INT4)
BUILTIN_OPR(LE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t arg1 = FMGR_ARG(1).GetInt32();
    bool res = arg0 <= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT4_gt, 572)
BUILTIN_ARGTYPE(INT4, INT4)
BUILTIN_OPR(GT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t arg1 = FMGR_ARG(1).GetInt32();
    bool res = arg0 > arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT4_ge, 573)
BUILTIN_ARGTYPE(INT4, INT4)
BUILTIN_OPR(GE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t arg1 = FMGR_ARG(1).GetInt32();
    bool res = arg0 >= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(INT4_not, 574)
BUILTIN_ARGTYPE(INT4)
BUILTIN_OPR(BITNOT)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t res = ~arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(INT4_and, 575)
BUILTIN_ARGTYPE(INT4, INT4)
BUILTIN_OPR(BITAND)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t arg1 = FMGR_ARG(1).GetInt32();
    int32_t res = (arg0 & arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(INT4_or, 576)
BUILTIN_ARGTYPE(INT4, INT4)
BUILTIN_OPR(BITOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t arg1 = FMGR_ARG(1).GetInt32();
    int32_t res = (arg0 | arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(INT4_xor, 577)
BUILTIN_ARGTYPE(INT4, INT4)
BUILTIN_OPR(BITXOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t arg1 = FMGR_ARG(1).GetInt32();
    int32_t res = (arg0 ^ arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(INT4_lsh, 578)
BUILTIN_ARGTYPE(INT4, UINT1)
BUILTIN_OPR(LSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t arg1 = FMGR_ARG(1).GetInt32();
    int32_t res = (arg0 << arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(INT4_rsh, 579)
BUILTIN_ARGTYPE(INT4, UINT1)
BUILTIN_OPR(RSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t arg0 = FMGR_ARG(0).GetInt32();
    int32_t arg1 = FMGR_ARG(1).GetUInt8();
    int32_t res = (arg0 >> arg1);
    return Datum::From(res);
}

}   // namespace taco

#endif      // UTILS_TYPSUPP_INT4_H
