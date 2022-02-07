#ifndef UTILS_TYPSUPP_DOUBLE_H
#define UTILS_TYPSUPP_DOUBLE_H


#include "tdb.h"

#include <cinttypes>

#include <absl/strings/str_cat.h>

#include "utils/builtin_funcs.h"
#include "utils/numbers.h"

namespace taco {

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(DOUBLE_in, 830)
BUILTIN_ARGTYPE(__STRING)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    double val;
    if (!absl::SimpleAtod(str, &val)) {
        LOG(kError, "cannot parse \"%s\" as a DOUBLE", str);
    }
    return Datum::From(val);
}

BUILTIN_RETTYPE(__STRING)
BUILTIN_FUNC(DOUBLE_out, 831)
BUILTIN_ARGTYPE(DOUBLE)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    double val = FMGR_ARG(0).GetDouble();
    std::string s = absl::StrCat(val);
    auto buffer = unique_malloc(s.size());
    memcpy((char*) buffer.get(), s.data(), s.size());
    return Datum::FromVarlenBytes(std::move(buffer), s.size());
}

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(DOUBLE_add, 832)
BUILTIN_ARGTYPE(DOUBLE)
BUILTIN_OPR(ADD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    double arg0 = FMGR_ARG(0).GetDouble();
    double arg1 = FMGR_ARG(1).GetDouble();
    double res = arg0 + arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(DOUBLE_sub, 833)
BUILTIN_ARGTYPE(DOUBLE)
BUILTIN_OPR(SUB)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    double arg0 = FMGR_ARG(0).GetDouble();
    double arg1 = FMGR_ARG(1).GetDouble();
    double res = arg0 - arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(DOUBLE_mul, 834)
BUILTIN_ARGTYPE(DOUBLE)
BUILTIN_OPR(MUL)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    double arg0 = FMGR_ARG(0).GetDouble();
    double arg1 = FMGR_ARG(1).GetDouble();
    double res = arg0 * arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(DOUBLE_div, 835)
BUILTIN_ARGTYPE(DOUBLE)
BUILTIN_OPR(DIV)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    double arg0 = FMGR_ARG(0).GetDouble();
    double arg1 = FMGR_ARG(1).GetDouble();
    double res = arg0 / arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(DOUBLE_neg, 836)
BUILTIN_ARGTYPE(DOUBLE)
BUILTIN_OPR(NEG)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    double arg0 = FMGR_ARG(0).GetDouble();
    double res = -arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(DOUBLE_eq, 837)
BUILTIN_ARGTYPE(DOUBLE, DOUBLE)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    double arg0 = FMGR_ARG(0).GetDouble();
    double arg1 = FMGR_ARG(1).GetDouble();
    bool res = arg0 == arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(DOUBLE_ne, 838)
BUILTIN_ARGTYPE(DOUBLE, DOUBLE)
BUILTIN_OPR(NE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    double arg0 = FMGR_ARG(0).GetDouble();
    double arg1 = FMGR_ARG(1).GetDouble();
    bool res = arg0 != arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(DOUBLE_lt, 839)
BUILTIN_ARGTYPE(DOUBLE, DOUBLE)
BUILTIN_OPR(LT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    double arg0 = FMGR_ARG(0).GetDouble();
    double arg1 = FMGR_ARG(1).GetDouble();
    bool res = arg0 < arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(DOUBLE_le, 840)
BUILTIN_ARGTYPE(DOUBLE, DOUBLE)
BUILTIN_OPR(LE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    double arg0 = FMGR_ARG(0).GetDouble();
    double arg1 = FMGR_ARG(1).GetDouble();
    bool res = arg0 <= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(DOUBLE_gt, 841)
BUILTIN_ARGTYPE(DOUBLE, DOUBLE)
BUILTIN_OPR(GT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    double arg0 = FMGR_ARG(0).GetDouble();
    double arg1 = FMGR_ARG(1).GetDouble();
    bool res = arg0 > arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(DOUBLE_ge, 842)
BUILTIN_ARGTYPE(DOUBLE, DOUBLE)
BUILTIN_OPR(GE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    double arg0 = FMGR_ARG(0).GetDouble();
    double arg1 = FMGR_ARG(1).GetDouble();
    bool res = arg0 >= arg1;
    return Datum::From(res);
}


}   // namespace taco

#endif      // UTILS_TYPSUPP_DOUBLE_H
