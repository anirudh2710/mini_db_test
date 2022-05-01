#include "tdb.h"

#include <cinttypes>

#include <absl/strings/numbers.h>

#include "utils/builtin_funcs.h"
#include "utils/numbers.h"
#include "utils/typsupp/aggregation.h"
#include "utils/typsupp/varchar.h"

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

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(INT8_SUM_acc, 610)
BUILTIN_ARGTYPE(__INTERNAL, INT8)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    SumState *s = (SumState *) FMGR_ARG(0).GetVarlenBytes();
    s->m_empty = false;

    typedef typename SumStateAggType<int64_t>::A A;
    ((A&) s->m_agg) += FMGR_ARG(1).GetInt64();
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(INT8_AVG_acc, 611)
BUILTIN_ARGTYPE(__INTERNAL, INT8)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    AvgState *s = (AvgState *) FMGR_ARG(0).GetVarlenBytes();
    s->m_sum += FMGR_ARG(1).GetInt64();
    s->m_cnt += 1;
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(INT8_MIN_acc, 612)
BUILTIN_ARGTYPE(__INTERNAL, INT8)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    int64_t val = FMGR_ARG(1).GetInt64();
    if (s->m_empty) {
        s->m_empty = false;
        ((int64_t&) s->m_value) = val;
    } else {
        if (val < ((int64_t&) s->m_value)) {
            ((int64_t&) s->m_value) = val;
        }
    }
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(INT8_MAX_acc, 613)
BUILTIN_ARGTYPE(__INTERNAL, INT8)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    int64_t val = FMGR_ARG(1).GetInt64();
    if (s->m_empty) {
        s->m_empty = false;
        ((int64_t&) s->m_value) = val;
    } else {
        if (val > ((int64_t&) s->m_value)) {
            ((int64_t&) s->m_value) = val;
        }
    }
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(INT8_MINMAX_finalize, 614)
BUILTIN_ARGTYPE(__INTERNAL)
{
    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    if (s->m_empty) {
        return Datum::FromNull();
    }
    return Datum::From((int64_t&)s->m_value);
}

BUILTIN_RETTYPE(VARCHAR)
BUILTIN_FUNC(INT8_to_VARCHAR, 615)
BUILTIN_ARGTYPE(INT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    int64_t val = FMGR_ARG(0).GetInt64();
    auto buffer = unique_malloc(STRING_OUTPUT_BUFLEN);
    auto len = absl::SNPrintF(
        (char*) buffer.get(), STRING_OUTPUT_BUFLEN, "%" PRId64, val);
    // It's safe to pass a buffer that is larger than the declared length to
    // Datum, which will free the buffer using `free()` without the length.
    return Datum::FromVarlenBytes(std::move(buffer), (uint32_t) len);
}

BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(VARCHAR_to_INT8, 616)
BUILTIN_ARGTYPE(VARCHAR)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view str = varchar_to_string_view(FMGR_ARG(0));
    int64_t val;
    if (!SimpleAtoiWrapper(str, &val)) {
        LOG(kError, "cannot cast string \"%s\" as a @SQLTYPE", str);
    }
    return Datum::From(val);
}

}   // namespace taco

