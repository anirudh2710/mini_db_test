#include "tdb.h"

#include <cinttypes>

#include <absl/strings/str_cat.h>

#include "utils/builtin_funcs.h"
#include "utils/numbers.h"
#include "utils/typsupp/aggregation.h"
#include "utils/typsupp/varchar.h"

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
BUILTIN_ARGTYPE(FLOAT, FLOAT)
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
BUILTIN_ARGTYPE(FLOAT, FLOAT)
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
BUILTIN_ARGTYPE(FLOAT, FLOAT)
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
BUILTIN_ARGTYPE(FLOAT, FLOAT)
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

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(FLOAT_SUM_acc, 813)
BUILTIN_ARGTYPE(__INTERNAL, FLOAT)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    SumState *s = (SumState *) FMGR_ARG(0).GetVarlenBytes();
    s->m_empty = false;

    typedef typename SumStateAggType<float>::A A;
    ((A&) s->m_agg) += FMGR_ARG(1).GetFloat();
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(FLOAT_AVG_acc, 814)
BUILTIN_ARGTYPE(__INTERNAL, FLOAT)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    AvgState *s = (AvgState *) FMGR_ARG(0).GetVarlenBytes();
    s->m_sum += FMGR_ARG(1).GetFloat();
    s->m_cnt += 1;
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(FLOAT_MIN_acc, 815)
BUILTIN_ARGTYPE(__INTERNAL, FLOAT)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    float val = FMGR_ARG(1).GetFloat();
    if (s->m_empty) {
        s->m_empty = false;
        ((float&) s->m_value) = val;
    } else {
        if (val < ((float&) s->m_value)) {
            ((float&) s->m_value) = val;
        }
    }
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(FLOAT_MAX_acc, 816)
BUILTIN_ARGTYPE(__INTERNAL, FLOAT)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    float val = FMGR_ARG(1).GetFloat();
    if (s->m_empty) {
        s->m_empty = false;
        ((float&) s->m_value) = val;
    } else {
        if (val > ((float&) s->m_value)) {
            ((float&) s->m_value) = val;
        }
    }
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(FLOAT_MINMAX_finalize, 817)
BUILTIN_ARGTYPE(__INTERNAL)
{
    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    if (s->m_empty) {
        return Datum::FromNull();
    }
    return Datum::From((float&)s->m_value);
}

BUILTIN_RETTYPE(VARCHAR)
BUILTIN_FUNC(FLOAT_to_VARCHAR, 818)
BUILTIN_ARGTYPE(FLOAT)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    float val = FMGR_ARG(0).GetFloat();
    std::string s = absl::StrCat(val);
    auto buffer = unique_malloc(s.size());
    memcpy((char*) buffer.get(), s.data(), s.length());
    return Datum::FromVarlenBytes(std::move(buffer), (uint32_t) s.length());
}

BUILTIN_RETTYPE(FLOAT)
BUILTIN_FUNC(VARCHAR_to_FLOAT, 819)
BUILTIN_ARGTYPE(VARCHAR)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view str = varchar_to_string_view(FMGR_ARG(0));
    float val;
    if (!absl::SimpleAtof(str, &val)) {
        LOG(kError, "cannot cast string \"%s\" as a @SQLTYPE", str);
    }
    return Datum::From(val);
}

}   // namespace taco

