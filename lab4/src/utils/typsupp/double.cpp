#include "tdb.h"

#include <cinttypes>

#include <absl/strings/str_cat.h>

#include "utils/builtin_funcs.h"
#include "utils/numbers.h"
#include "utils/typsupp/aggregation.h"
#include "utils/typsupp/varchar.h"

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
    // XXX force 2 digits after decimal points for TPC-H
    std::string s = absl::StrFormat("%.2f", val);
    auto buffer = unique_malloc(s.size());
    memcpy((char*) buffer.get(), s.data(), s.size());
    return Datum::FromVarlenBytes(std::move(buffer), s.size());
}

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(DOUBLE_add, 832)
BUILTIN_ARGTYPE(DOUBLE, DOUBLE)
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
BUILTIN_ARGTYPE(DOUBLE, DOUBLE)
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
BUILTIN_ARGTYPE(DOUBLE, DOUBLE)
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
BUILTIN_ARGTYPE(DOUBLE, DOUBLE)
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

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(DOUBLE_SUM_acc, 843)
BUILTIN_ARGTYPE(__INTERNAL, DOUBLE)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    SumState *s = (SumState *) FMGR_ARG(0).GetVarlenBytes();
    s->m_empty = false;

    typedef typename SumStateAggType<double>::A A;
    ((A&) s->m_agg) += FMGR_ARG(1).GetDouble();
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(DOUBLE_AVG_acc, 844)
BUILTIN_ARGTYPE(__INTERNAL, DOUBLE)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    AvgState *s = (AvgState *) FMGR_ARG(0).GetVarlenBytes();
    s->m_sum += FMGR_ARG(1).GetDouble();
    s->m_cnt += 1;
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(DOUBLE_MIN_acc, 845)
BUILTIN_ARGTYPE(__INTERNAL, DOUBLE)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    double val = FMGR_ARG(1).GetDouble();
    if (s->m_empty) {
        s->m_empty = false;
        ((double&) s->m_value) = val;
    } else {
        if (val < ((double&) s->m_value)) {
            ((double&) s->m_value) = val;
        }
    }
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(DOUBLE_MAX_acc, 846)
BUILTIN_ARGTYPE(__INTERNAL, DOUBLE)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    double val = FMGR_ARG(1).GetDouble();
    if (s->m_empty) {
        s->m_empty = false;
        ((double&) s->m_value) = val;
    } else {
        if (val > ((double&) s->m_value)) {
            ((double&) s->m_value) = val;
        }
    }
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(DOUBLE_MINMAX_finalize, 847)
BUILTIN_ARGTYPE(__INTERNAL)
{
    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    if (s->m_empty) {
        return Datum::FromNull();
    }
    return Datum::From((double&)s->m_value);
}

BUILTIN_RETTYPE(VARCHAR)
BUILTIN_FUNC(DOUBLE_to_VARCHAR, 848)
BUILTIN_ARGTYPE(DOUBLE)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    double val = FMGR_ARG(0).GetDouble();
    std::string s = absl::StrCat(val);
    auto buffer = unique_malloc(s.size());
    memcpy((char*) buffer.get(), s.data(), s.length());
    return Datum::FromVarlenBytes(std::move(buffer), (uint32_t) s.length());
}

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(VARCHAR_to_DOUBLE, 849)
BUILTIN_ARGTYPE(VARCHAR)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view str = varchar_to_string_view(FMGR_ARG(0));
    double val;
    if (!absl::SimpleAtod(str, &val)) {
        LOG(kError, "cannot cast string \"%s\" as a @SQLTYPE", str);
    }
    return Datum::From(val);
}

}   // namespace taco

