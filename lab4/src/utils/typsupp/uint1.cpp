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

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_in, 620)
BUILTIN_ARGTYPE(__STRING)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    uint8_t val;
    if (!SimpleAtoiWrapper(str, &val)) {
        LOG(kError, "cannot parse \"%s\" as a UINT1", str);
    }
    return Datum::From(val);
}

BUILTIN_RETTYPE(__STRING)
BUILTIN_FUNC(UINT1_out, 621)
BUILTIN_ARGTYPE(UINT1)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint8_t val = FMGR_ARG(0).GetUInt8();
    auto buffer = unique_malloc(STRING_OUTPUT_BUFLEN);
    absl::SNPrintF((char*) buffer.get(), STRING_OUTPUT_BUFLEN, "%" PRIu8, val);
    return Datum::FromVarlenBytes(std::move(buffer),
                                  strlen((char*) buffer.get()));
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_add, 622)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(ADD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    uint8_t res = arg0 + arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_sub, 623)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(SUB)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    uint8_t res = arg0 - arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_mul, 624)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(MUL)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    uint8_t res = arg0 * arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_div, 625)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(DIV)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t res = arg0 / arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_mod, 626)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(MOD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t res = arg0 % arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_neg, 627)
BUILTIN_ARGTYPE(UINT1)
BUILTIN_OPR(NEG)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t res = -arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT1_eq, 628)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    bool res = arg0 == arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT1_ne, 629)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(NE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    bool res = arg0 != arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT1_lt, 630)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(LT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    bool res = arg0 < arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT1_le, 631)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(LE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    bool res = arg0 <= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT1_gt, 632)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(GT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    bool res = arg0 > arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT1_ge, 633)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(GE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    bool res = arg0 >= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_not, 634)
BUILTIN_ARGTYPE(UINT1)
BUILTIN_OPR(BITNOT)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t res = ~arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_and, 635)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(BITAND)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    uint8_t res = (arg0 & arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_or, 636)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(BITOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    uint8_t res = (arg0 | arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_xor, 637)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(BITXOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    uint8_t res = (arg0 ^ arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_lsh, 638)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(LSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    uint8_t res = (arg0 << arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT1_rsh, 639)
BUILTIN_ARGTYPE(UINT1, UINT1)
BUILTIN_OPR(RSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint8_t arg0 = FMGR_ARG(0).GetUInt8();
    uint8_t arg1 = FMGR_ARG(1).GetUInt8();
    uint8_t res = (arg0 >> arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(UINT1_SUM_acc, 640)
BUILTIN_ARGTYPE(__INTERNAL, UINT1)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    SumState *s = (SumState *) FMGR_ARG(0).GetVarlenBytes();
    s->m_empty = false;

    typedef typename SumStateAggType<uint8_t>::A A;
    ((A&) s->m_agg) += FMGR_ARG(1).GetUInt8();
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(UINT1_AVG_acc, 641)
BUILTIN_ARGTYPE(__INTERNAL, UINT1)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    AvgState *s = (AvgState *) FMGR_ARG(0).GetVarlenBytes();
    s->m_sum += FMGR_ARG(1).GetUInt8();
    s->m_cnt += 1;
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(UINT1_MIN_acc, 642)
BUILTIN_ARGTYPE(__INTERNAL, UINT1)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    uint8_t val = FMGR_ARG(1).GetUInt8();
    if (s->m_empty) {
        s->m_empty = false;
        ((uint8_t&) s->m_value) = val;
    } else {
        if (val < ((uint8_t&) s->m_value)) {
            ((uint8_t&) s->m_value) = val;
        }
    }
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(UINT1_MAX_acc, 643)
BUILTIN_ARGTYPE(__INTERNAL, UINT1)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    uint8_t val = FMGR_ARG(1).GetUInt8();
    if (s->m_empty) {
        s->m_empty = false;
        ((uint8_t&) s->m_value) = val;
    } else {
        if (val > ((uint8_t&) s->m_value)) {
            ((uint8_t&) s->m_value) = val;
        }
    }
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(UINT1_MINMAX_finalize, 644)
BUILTIN_ARGTYPE(__INTERNAL)
{
    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    if (s->m_empty) {
        return Datum::FromNull();
    }
    return Datum::From((uint8_t&)s->m_value);
}

BUILTIN_RETTYPE(VARCHAR)
BUILTIN_FUNC(UINT1_to_VARCHAR, 645)
BUILTIN_ARGTYPE(UINT1)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint8_t val = FMGR_ARG(0).GetUInt8();
    auto buffer = unique_malloc(STRING_OUTPUT_BUFLEN);
    auto len = absl::SNPrintF(
        (char*) buffer.get(), STRING_OUTPUT_BUFLEN, "%" PRIu8, val);
    // It's safe to pass a buffer that is larger than the declared length to
    // Datum, which will free the buffer using `free()` without the length.
    return Datum::FromVarlenBytes(std::move(buffer), (uint32_t) len);
}

BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(VARCHAR_to_UINT1, 646)
BUILTIN_ARGTYPE(VARCHAR)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view str = varchar_to_string_view(FMGR_ARG(0));
    uint8_t val;
    if (!SimpleAtoiWrapper(str, &val)) {
        LOG(kError, "cannot cast string \"%s\" as a @SQLTYPE", str);
    }
    return Datum::From(val);
}

}   // namespace taco

