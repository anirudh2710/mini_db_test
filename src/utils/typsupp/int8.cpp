#ifndef UTILS_TYPSUPP_INT8_H
#define UTILS_TYPSUPP_INT8_H


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

BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(INT8_in, 590)
BUILTIN_ARGTYPE(__STRING)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    int64_t val;
    if (!SimpleAtoiWrapper(str, &val)) {
        LOG(kError, "cannot parse \"%s\" as a INT8", str);
    }
    return Datum::From(val);
}

BUILTIN_RETTYPE(__STRING)
BUILTIN_FUNC(INT8_out, 591)
BUILTIN_ARGTYPE(INT8)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    int64_t val = FMGR_ARG(0).GetInt64();
    auto buffer = unique_malloc(STRING_OUTPUT_BUFLEN);
    absl::SNPrintF((char*) buffer.get(), STRING_OUTPUT_BUFLEN, "%" PRId64, val);
    return Datum::FromVarlenBytes(std::move(buffer),
                                  strlen((char*) buffer.get()));
}

BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(INT8_add, 592)
BUILTIN_ARGTYPE(INT8, INT8)
BUILTIN_OPR(ADD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t arg1 = FMGR_ARG(1).GetInt64();
    int64_t res = arg0 + arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(INT8_sub, 593)
BUILTIN_ARGTYPE(INT8, INT8)
BUILTIN_OPR(SUB)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t arg1 = FMGR_ARG(1).GetInt64();
    int64_t res = arg0 - arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(INT8_mul, 594)
BUILTIN_ARGTYPE(INT8, INT8)
BUILTIN_OPR(MUL)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t arg1 = FMGR_ARG(1).GetInt64();
    int64_t res = arg0 * arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(INT8_div, 595)
BUILTIN_ARGTYPE(INT8, INT8)
BUILTIN_OPR(DIV)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg1 = FMGR_ARG(1).GetInt64();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t res = arg0 / arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(INT8_mod, 596)
BUILTIN_ARGTYPE(INT8, INT8)
BUILTIN_OPR(MOD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg1 = FMGR_ARG(1).GetInt64();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t res = arg0 % arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(INT8_neg, 597)
BUILTIN_ARGTYPE(INT8)
BUILTIN_OPR(NEG)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t res = -arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT8_eq, 598)
BUILTIN_ARGTYPE(INT8, INT8)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t arg1 = FMGR_ARG(1).GetInt64();
    bool res = arg0 == arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT8_ne, 599)
BUILTIN_ARGTYPE(INT8, INT8)
BUILTIN_OPR(NE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t arg1 = FMGR_ARG(1).GetInt64();
    bool res = arg0 != arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT8_lt, 600)
BUILTIN_ARGTYPE(INT8, INT8)
BUILTIN_OPR(LT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t arg1 = FMGR_ARG(1).GetInt64();
    bool res = arg0 < arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT8_le, 601)
BUILTIN_ARGTYPE(INT8, INT8)
BUILTIN_OPR(LE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t arg1 = FMGR_ARG(1).GetInt64();
    bool res = arg0 <= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT8_gt, 602)
BUILTIN_ARGTYPE(INT8, INT8)
BUILTIN_OPR(GT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t arg1 = FMGR_ARG(1).GetInt64();
    bool res = arg0 > arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT8_ge, 603)
BUILTIN_ARGTYPE(INT8, INT8)
BUILTIN_OPR(GE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t arg1 = FMGR_ARG(1).GetInt64();
    bool res = arg0 >= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(INT8_not, 604)
BUILTIN_ARGTYPE(INT8)
BUILTIN_OPR(BITNOT)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t res = ~arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(INT8_and, 605)
BUILTIN_ARGTYPE(INT8, INT8)
BUILTIN_OPR(BITAND)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t arg1 = FMGR_ARG(1).GetInt64();
    int64_t res = (arg0 & arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(INT8_or, 606)
BUILTIN_ARGTYPE(INT8, INT8)
BUILTIN_OPR(BITOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t arg1 = FMGR_ARG(1).GetInt64();
    int64_t res = (arg0 | arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(INT8_xor, 607)
BUILTIN_ARGTYPE(INT8, INT8)
BUILTIN_OPR(BITXOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t arg1 = FMGR_ARG(1).GetInt64();
    int64_t res = (arg0 ^ arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(INT8_lsh, 608)
BUILTIN_ARGTYPE(INT8, UINT1)
BUILTIN_OPR(LSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t arg1 = FMGR_ARG(1).GetInt64();
    int64_t res = (arg0 << arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(INT8_rsh, 609)
BUILTIN_ARGTYPE(INT8, UINT1)
BUILTIN_OPR(RSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int64_t arg0 = FMGR_ARG(0).GetInt64();
    int64_t arg1 = FMGR_ARG(1).GetUInt8();
    int64_t res = (arg0 >> arg1);
    return Datum::From(res);
}

}   // namespace taco

#endif      // UTILS_TYPSUPP_INT8_H
