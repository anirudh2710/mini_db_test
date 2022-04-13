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

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(INT2_SUM_acc, 550)
BUILTIN_ARGTYPE(__INTERNAL, INT2)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    SumState *s = (SumState *) FMGR_ARG(0).GetVarlenBytes();
    s->m_empty = false;

    typedef typename SumStateAggType<int16_t>::A A;
    ((A&) s->m_agg) += FMGR_ARG(1).GetInt16();
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(INT2_AVG_acc, 551)
BUILTIN_ARGTYPE(__INTERNAL, INT2)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    AvgState *s = (AvgState *) FMGR_ARG(0).GetVarlenBytes();
    s->m_sum += FMGR_ARG(1).GetInt16();
    s->m_cnt += 1;
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(INT2_MIN_acc, 552)
BUILTIN_ARGTYPE(__INTERNAL, INT2)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    int16_t val = FMGR_ARG(1).GetInt16();
    if (s->m_empty) {
        s->m_empty = false;
        ((int16_t&) s->m_value) = val;
    } else {
        if (val < ((int16_t&) s->m_value)) {
            ((int16_t&) s->m_value) = val;
        }
    }
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(INT2_MAX_acc, 553)
BUILTIN_ARGTYPE(__INTERNAL, INT2)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    int16_t val = FMGR_ARG(1).GetInt16();
    if (s->m_empty) {
        s->m_empty = false;
        ((int16_t&) s->m_value) = val;
    } else {
        if (val > ((int16_t&) s->m_value)) {
            ((int16_t&) s->m_value) = val;
        }
    }
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(INT2_MINMAX_finalize, 554)
BUILTIN_ARGTYPE(__INTERNAL)
{
    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    if (s->m_empty) {
        return Datum::FromNull();
    }
    return Datum::From((int16_t&)s->m_value);
}

BUILTIN_RETTYPE(VARCHAR)
BUILTIN_FUNC(INT2_to_VARCHAR, 555)
BUILTIN_ARGTYPE(INT2)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    int16_t val = FMGR_ARG(0).GetInt16();
    auto buffer = unique_malloc(STRING_OUTPUT_BUFLEN);
    auto len = absl::SNPrintF(
        (char*) buffer.get(), STRING_OUTPUT_BUFLEN, "%" PRId16, val);
    // It's safe to pass a buffer that is larger than the declared length to
    // Datum, which will free the buffer using `free()` without the length.
    return Datum::FromVarlenBytes(std::move(buffer), (uint32_t) len);
}

BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(VARCHAR_to_INT2, 556)
BUILTIN_ARGTYPE(VARCHAR)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view str = varchar_to_string_view(FMGR_ARG(0));
    int16_t val;
    if (!SimpleAtoiWrapper(str, &val)) {
        LOG(kError, "cannot cast string \"%s\" as a @SQLTYPE", str);
    }
    return Datum::From(val);
}

}   // namespace taco

