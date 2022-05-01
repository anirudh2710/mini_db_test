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

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_in, 710)
BUILTIN_ARGTYPE(__STRING)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    uint64_t val;
    if (!SimpleAtoiWrapper(str, &val)) {
        LOG(kError, "cannot parse \"%s\" as a UINT8", str);
    }
    return Datum::From(val);
}

BUILTIN_RETTYPE(__STRING)
BUILTIN_FUNC(UINT8_out, 711)
BUILTIN_ARGTYPE(UINT8)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint64_t val = FMGR_ARG(0).GetUInt64();
    auto buffer = unique_malloc(STRING_OUTPUT_BUFLEN);
    absl::SNPrintF((char*) buffer.get(), STRING_OUTPUT_BUFLEN, "%" PRIu64, val);
    return Datum::FromVarlenBytes(std::move(buffer),
                                  strlen((char*) buffer.get()));
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_add, 712)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(ADD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    uint64_t res = arg0 + arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_sub, 713)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(SUB)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    uint64_t res = arg0 - arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_mul, 714)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(MUL)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    uint64_t res = arg0 * arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_div, 715)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(DIV)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t res = arg0 / arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_mod, 716)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(MOD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    if (arg1 == 0) {
        LOG(kError, "division by zero");
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t res = arg0 % arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_neg, 717)
BUILTIN_ARGTYPE(UINT8)
BUILTIN_OPR(NEG)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t res = -arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT8_eq, 718)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    bool res = arg0 == arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT8_ne, 719)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(NE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    bool res = arg0 != arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT8_lt, 720)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(LT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    bool res = arg0 < arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT8_le, 721)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(LE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    bool res = arg0 <= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT8_gt, 722)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(GT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    bool res = arg0 > arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(UINT8_ge, 723)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(GE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    bool res = arg0 >= arg1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_not, 724)
BUILTIN_ARGTYPE(UINT8)
BUILTIN_OPR(BITNOT)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t res = ~arg0 ;
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_and, 725)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(BITAND)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    uint64_t res = (arg0 & arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_or, 726)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(BITOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    uint64_t res = (arg0 | arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_xor, 727)
BUILTIN_ARGTYPE(UINT8, UINT8)
BUILTIN_OPR(BITXOR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    uint64_t res = (arg0 ^ arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_lsh, 728)
BUILTIN_ARGTYPE(UINT8, UINT1)
BUILTIN_OPR(LSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt64();
    uint64_t res = (arg0 << arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT8_rsh, 729)
BUILTIN_ARGTYPE(UINT8, UINT1)
BUILTIN_OPR(RSH)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    uint64_t arg0 = FMGR_ARG(0).GetUInt64();
    uint64_t arg1 = FMGR_ARG(1).GetUInt8();
    uint64_t res = (arg0 >> arg1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(UINT8_SUM_acc, 730)
BUILTIN_ARGTYPE(__INTERNAL, UINT8)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    SumState *s = (SumState *) FMGR_ARG(0).GetVarlenBytes();
    s->m_empty = false;

    typedef typename SumStateAggType<uint64_t>::A A;
    ((A&) s->m_agg) += FMGR_ARG(1).GetUInt64();
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(UINT8_AVG_acc, 731)
BUILTIN_ARGTYPE(__INTERNAL, UINT8)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    AvgState *s = (AvgState *) FMGR_ARG(0).GetVarlenBytes();
    s->m_sum += FMGR_ARG(1).GetUInt64();
    s->m_cnt += 1;
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(UINT8_MIN_acc, 732)
BUILTIN_ARGTYPE(__INTERNAL, UINT8)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    uint64_t val = FMGR_ARG(1).GetUInt64();
    if (s->m_empty) {
        s->m_empty = false;
        ((uint64_t&) s->m_value) = val;
    } else {
        if (val < ((uint64_t&) s->m_value)) {
            ((uint64_t&) s->m_value) = val;
        }
    }
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(UINT8_MAX_acc, 733)
BUILTIN_ARGTYPE(__INTERNAL, UINT8)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    uint64_t val = FMGR_ARG(1).GetUInt64();
    if (s->m_empty) {
        s->m_empty = false;
        ((uint64_t&) s->m_value) = val;
    } else {
        if (val > ((uint64_t&) s->m_value)) {
            ((uint64_t&) s->m_value) = val;
        }
    }
    return Datum::FromNull();
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(UINT8_MINMAX_finalize, 734)
BUILTIN_ARGTYPE(__INTERNAL)
{
    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) FMGR_ARG(0).GetVarlenBytes();
    if (s->m_empty) {
        return Datum::FromNull();
    }
    return Datum::From((uint64_t&)s->m_value);
}

BUILTIN_RETTYPE(VARCHAR)
BUILTIN_FUNC(UINT8_to_VARCHAR, 735)
BUILTIN_ARGTYPE(UINT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint64_t val = FMGR_ARG(0).GetUInt64();
    auto buffer = unique_malloc(STRING_OUTPUT_BUFLEN);
    auto len = absl::SNPrintF(
        (char*) buffer.get(), STRING_OUTPUT_BUFLEN, "%" PRIu64, val);
    // It's safe to pass a buffer that is larger than the declared length to
    // Datum, which will free the buffer using `free()` without the length.
    return Datum::FromVarlenBytes(std::move(buffer), (uint32_t) len);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(VARCHAR_to_UINT8, 736)
BUILTIN_ARGTYPE(VARCHAR)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view str = varchar_to_string_view(FMGR_ARG(0));
    uint64_t val;
    if (!SimpleAtoiWrapper(str, &val)) {
        LOG(kError, "cannot cast string \"%s\" as a @SQLTYPE", str);
    }
    return Datum::From(val);
}

}   // namespace taco

