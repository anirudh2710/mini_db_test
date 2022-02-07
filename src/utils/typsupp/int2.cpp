#ifndef UTILS_TYPSUPP_INT2_H
#define UTILS_TYPSUPP_INT2_H


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

BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(INT2_in, 530)
BUILTIN_ARGTYPE(__STRING)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    int16_t val;
    if (!SimpleAtoiWrapper(str, &val)) {
        LOG(kError, "cannot parse \"%s\" as a INT2", str);
    }
    return Datum::From(val);
}

BUILTIN_RETTYPE(__STRING)
BUILTIN_FUNC(INT2_out, 531)
BUILTIN_ARGTYPE(INT2)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    int16_t val = FMGR_ARG(0).GetInt16();
    auto buffer = unique_malloc(STRING_OUTPUT_BUFLEN);
    absl::SNPrintF((char*) buffer.get(), STRING_OUTPUT_BUFLEN, "%" PRId16, val);
    return Datum::FromVarlenBytes(std::move(buffer),
                                  strlen((char*) buffer.get()));
}

BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(INT2_add, 532)
BUILTIN_ARGTYPE(INT2, INT2)
BUILTIN_OPR(ADD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t arg1 = FMGR_ARG(1).GetInt16();
    int16_t res = arg0 + arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(INT2_sub, 533)
BUILTIN_ARGTYPE(INT2, INT2)
BUILTIN_OPR(SUB)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t arg1 = FMGR_ARG(1).GetInt16();
    int16_t res = arg0 - arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(INT2_mul, 534)
BUILTIN_ARGTYPE(INT2, INT2)
BUILTIN_OPR(MUL)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t arg1 = FMGR_ARG(1).GetInt16();
    int16_t res = arg0 * arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(INT2_div, 535)
BUILTIN_ARGTYPE(INT2, INT2)
BUILTIN_OPR(DIV)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg1 = FMGR_ARG(1).GetInt16();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t res = arg0 / arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(INT2_mod, 536)
BUILTIN_ARGTYPE(INT2, INT2)
BUILTIN_OPR(MOD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg1 = FMGR_ARG(1).GetInt16();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t res = arg0 % arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(INT2_neg, 537)
BUILTIN_ARGTYPE(INT2)
BUILTIN_OPR(NEG)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t res = -arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT2_eq, 538)
BUILTIN_ARGTYPE(INT2, INT2)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t arg1 = FMGR_ARG(1).GetInt16();
    bool res = arg0 == arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT2_ne, 539)
BUILTIN_ARGTYPE(INT2, INT2)
BUILTIN_OPR(NE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t arg1 = FMGR_ARG(1).GetInt16();
    bool res = arg0 != arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT2_lt, 540)
BUILTIN_ARGTYPE(INT2, INT2)
BUILTIN_OPR(LT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t arg1 = FMGR_ARG(1).GetInt16();
    bool res = arg0 < arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT2_le, 541)
BUILTIN_ARGTYPE(INT2, INT2)
BUILTIN_OPR(LE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t arg1 = FMGR_ARG(1).GetInt16();
    bool res = arg0 <= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT2_gt, 542)
BUILTIN_ARGTYPE(INT2, INT2)
BUILTIN_OPR(GT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t arg1 = FMGR_ARG(1).GetInt16();
    bool res = arg0 > arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT2_ge, 543)
BUILTIN_ARGTYPE(INT2, INT2)
BUILTIN_OPR(GE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t arg1 = FMGR_ARG(1).GetInt16();
    bool res = arg0 >= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(INT2_not, 544)
BUILTIN_ARGTYPE(INT2)
BUILTIN_OPR(BITNOT)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t res = ~arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(INT2_and, 545)
BUILTIN_ARGTYPE(INT2, INT2)
BUILTIN_OPR(BITAND)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t arg1 = FMGR_ARG(1).GetInt16();
    int16_t res = (arg0 & arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(INT2_or, 546)
BUILTIN_ARGTYPE(INT2, INT2)
BUILTIN_OPR(BITOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t arg1 = FMGR_ARG(1).GetInt16();
    int16_t res = (arg0 | arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(INT2_xor, 547)
BUILTIN_ARGTYPE(INT2, INT2)
BUILTIN_OPR(BITXOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t arg1 = FMGR_ARG(1).GetInt16();
    int16_t res = (arg0 ^ arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(INT2_lsh, 548)
BUILTIN_ARGTYPE(INT2, UINT1)
BUILTIN_OPR(LSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t arg1 = FMGR_ARG(1).GetInt16();
    int16_t res = (arg0 << arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(INT2_rsh, 549)
BUILTIN_ARGTYPE(INT2, UINT1)
BUILTIN_OPR(RSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int16_t arg0 = FMGR_ARG(0).GetInt16();
    int16_t arg1 = FMGR_ARG(1).GetUInt8();
    int16_t res = (arg0 >> arg1);
    return Datum::From(res);
}

}   // namespace taco

#endif      // UTILS_TYPSUPP_INT2_H
