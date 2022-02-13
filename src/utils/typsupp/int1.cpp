#ifndef UTILS_TYPSUPP_INT1_H
#define UTILS_TYPSUPP_INT1_H


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

BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(INT1_in, 500)
BUILTIN_ARGTYPE(__STRING)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    int8_t val;
    if (!SimpleAtoiWrapper(str, &val)) {
        LOG(kError, "cannot parse \"%s\" as a INT1", str);
    }
    return Datum::From(val);
}

BUILTIN_RETTYPE(__STRING)
BUILTIN_FUNC(INT1_out, 501)
BUILTIN_ARGTYPE(INT1)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    int8_t val = FMGR_ARG(0).GetInt8();
    auto buffer = unique_malloc(STRING_OUTPUT_BUFLEN);
    absl::SNPrintF((char*) buffer.get(), STRING_OUTPUT_BUFLEN, "%" PRId8, val);
    return Datum::FromVarlenBytes(std::move(buffer),
                                  strlen((char*) buffer.get()));
}

BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(INT1_add, 502)
BUILTIN_ARGTYPE(INT1, INT1)
BUILTIN_OPR(ADD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t arg1 = FMGR_ARG(1).GetInt8();
    int8_t res = arg0 + arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(INT1_sub, 503)
BUILTIN_ARGTYPE(INT1, INT1)
BUILTIN_OPR(SUB)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t arg1 = FMGR_ARG(1).GetInt8();
    int8_t res = arg0 - arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(INT1_mul, 504)
BUILTIN_ARGTYPE(INT1, INT1)
BUILTIN_OPR(MUL)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t arg1 = FMGR_ARG(1).GetInt8();
    int8_t res = arg0 * arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(INT1_div, 505)
BUILTIN_ARGTYPE(INT1, INT1)
BUILTIN_OPR(DIV)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg1 = FMGR_ARG(1).GetInt8();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t res = arg0 / arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(INT1_mod, 506)
BUILTIN_ARGTYPE(INT1, INT1)
BUILTIN_OPR(MOD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg1 = FMGR_ARG(1).GetInt8();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t res = arg0 % arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(INT1_neg, 507)
BUILTIN_ARGTYPE(INT1)
BUILTIN_OPR(NEG)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t res = -arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT1_eq, 508)
BUILTIN_ARGTYPE(INT1, INT1)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t arg1 = FMGR_ARG(1).GetInt8();
    bool res = arg0 == arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT1_ne, 509)
BUILTIN_ARGTYPE(INT1, INT1)
BUILTIN_OPR(NE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t arg1 = FMGR_ARG(1).GetInt8();
    bool res = arg0 != arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT1_lt, 510)
BUILTIN_ARGTYPE(INT1, INT1)
BUILTIN_OPR(LT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t arg1 = FMGR_ARG(1).GetInt8();
    bool res = arg0 < arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT1_le, 511)
BUILTIN_ARGTYPE(INT1, INT1)
BUILTIN_OPR(LE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t arg1 = FMGR_ARG(1).GetInt8();
    bool res = arg0 <= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT1_gt, 512)
BUILTIN_ARGTYPE(INT1, INT1)
BUILTIN_OPR(GT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t arg1 = FMGR_ARG(1).GetInt8();
    bool res = arg0 > arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(INT1_ge, 513)
BUILTIN_ARGTYPE(INT1, INT1)
BUILTIN_OPR(GE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t arg1 = FMGR_ARG(1).GetInt8();
    bool res = arg0 >= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(INT1_not, 514)
BUILTIN_ARGTYPE(INT1)
BUILTIN_OPR(BITNOT)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t res = ~arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(INT1_and, 515)
BUILTIN_ARGTYPE(INT1, INT1)
BUILTIN_OPR(BITAND)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t arg1 = FMGR_ARG(1).GetInt8();
    int8_t res = (arg0 & arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(INT1_or, 516)
BUILTIN_ARGTYPE(INT1, INT1)
BUILTIN_OPR(BITOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t arg1 = FMGR_ARG(1).GetInt8();
    int8_t res = (arg0 | arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(INT1_xor, 517)
BUILTIN_ARGTYPE(INT1, INT1)
BUILTIN_OPR(BITXOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t arg1 = FMGR_ARG(1).GetInt8();
    int8_t res = (arg0 ^ arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(INT1_lsh, 518)
BUILTIN_ARGTYPE(INT1, UINT1)
BUILTIN_OPR(LSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t arg1 = FMGR_ARG(1).GetInt8();
    int8_t res = (arg0 << arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(INT1_rsh, 519)
BUILTIN_ARGTYPE(INT1, UINT1)
BUILTIN_OPR(RSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int8_t arg0 = FMGR_ARG(0).GetInt8();
    int8_t arg1 = FMGR_ARG(1).GetUInt8();
    int8_t res = (arg0 >> arg1);
    return Datum::From(res);
}

}   // namespace taco

#endif      // UTILS_TYPSUPP_INT1_H
