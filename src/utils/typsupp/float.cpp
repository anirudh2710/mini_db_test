#ifndef UTILS_TYPSUPP_FLOAT_H
#define UTILS_TYPSUPP_FLOAT_H


#include "tdb.h"

#include <cinttypes>

#include <absl/strings/str_cat.h>

#include "utils/builtin_funcs.h"
#include "utils/numbers.h"

namespace taco {

BUILTIN_RETTYPE(FLOAT)
BUILTIN_FUNC(FLOAT_in, 800)
BUILTIN_ARGTYPE(__STRING)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    float val;
    if (!absl::SimpleAtof(str, &val)) {
        LOG(kError, "cannot parse \"%s\" as a FLOAT", str);
    }
    return Datum::From(val);
}

BUILTIN_RETTYPE(__STRING)
BUILTIN_FUNC(FLOAT_out, 801)
BUILTIN_ARGTYPE(FLOAT)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    float val = FMGR_ARG(0).GetFloat();
    std::string s = absl::StrCat(val);
    auto buffer = unique_malloc(s.size());
    memcpy((char*) buffer.get(), s.data(), s.size());
    return Datum::FromVarlenBytes(std::move(buffer), s.size());
}

BUILTIN_RETTYPE(FLOAT)
BUILTIN_FUNC(FLOAT_add, 802)
BUILTIN_ARGTYPE(FLOAT)
BUILTIN_OPR(ADD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    float arg0 = FMGR_ARG(0).GetFloat();
    float arg1 = FMGR_ARG(1).GetFloat();
    float res = arg0 + arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(FLOAT)
BUILTIN_FUNC(FLOAT_sub, 803)
BUILTIN_ARGTYPE(FLOAT)
BUILTIN_OPR(SUB)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    float arg0 = FMGR_ARG(0).GetFloat();
    float arg1 = FMGR_ARG(1).GetFloat();
    float res = arg0 - arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(FLOAT)
BUILTIN_FUNC(FLOAT_mul, 804)
BUILTIN_ARGTYPE(FLOAT)
BUILTIN_OPR(MUL)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    float arg0 = FMGR_ARG(0).GetFloat();
    float arg1 = FMGR_ARG(1).GetFloat();
    float res = arg0 * arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(FLOAT)
BUILTIN_FUNC(FLOAT_div, 805)
BUILTIN_ARGTYPE(FLOAT)
BUILTIN_OPR(DIV)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    float arg0 = FMGR_ARG(0).GetFloat();
    float arg1 = FMGR_ARG(1).GetFloat();
    float res = arg0 / arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(FLOAT)
BUILTIN_FUNC(FLOAT_neg, 806)
BUILTIN_ARGTYPE(FLOAT)
BUILTIN_OPR(NEG)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    float arg0 = FMGR_ARG(0).GetFloat();
    float res = -arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(FLOAT_eq, 807)
BUILTIN_ARGTYPE(FLOAT, FLOAT)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    float arg0 = FMGR_ARG(0).GetFloat();
    float arg1 = FMGR_ARG(1).GetFloat();
    bool res = arg0 == arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(FLOAT_ne, 808)
BUILTIN_ARGTYPE(FLOAT, FLOAT)
BUILTIN_OPR(NE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    float arg0 = FMGR_ARG(0).GetFloat();
    float arg1 = FMGR_ARG(1).GetFloat();
    bool res = arg0 != arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(FLOAT_lt, 809)
BUILTIN_ARGTYPE(FLOAT, FLOAT)
BUILTIN_OPR(LT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    float arg0 = FMGR_ARG(0).GetFloat();
    float arg1 = FMGR_ARG(1).GetFloat();
    bool res = arg0 < arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(FLOAT_le, 810)
BUILTIN_ARGTYPE(FLOAT, FLOAT)
BUILTIN_OPR(LE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    float arg0 = FMGR_ARG(0).GetFloat();
    float arg1 = FMGR_ARG(1).GetFloat();
    bool res = arg0 <= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(FLOAT_gt, 811)
BUILTIN_ARGTYPE(FLOAT, FLOAT)
BUILTIN_OPR(GT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    float arg0 = FMGR_ARG(0).GetFloat();
    float arg1 = FMGR_ARG(1).GetFloat();
    bool res = arg0 > arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(FLOAT_ge, 812)
BUILTIN_ARGTYPE(FLOAT, FLOAT)
BUILTIN_OPR(GE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    float arg0 = FMGR_ARG(0).GetFloat();
    float arg1 = FMGR_ARG(1).GetFloat();
    bool res = arg0 >= arg1;
    return Datum::From(res);
}


}   // namespace taco

#endif      // UTILS_TYPSUPP_FLOAT_H
