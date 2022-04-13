#include "tdb.h"

#include "utils/builtin_funcs.h"
#include "utils/typsupp/aggregation.h"

namespace taco {

BUILTIN_RETTYPE(__INTERNAL)
BUILTIN_FUNC(COUNT_init, 930)
BUILTIN_ARGTYPE()
{
    unique_malloced_ptr c = unique_aligned_alloc(8, sizeof(uint64_t));
    *((uint64_t*) c.get()) = 0;
    return Datum::FromVarlenBytes(std::move(c), sizeof(uint64_t));
}

BUILTIN_RETTYPE(VOID)
BUILTIN_FUNC(COUNT_acc, 931)
BUILTIN_ARGTYPE(__INTERNAL, ANY)
{
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }
    uint64_t *c = (uint64_t*) FMGR_ARG(0).GetVarlenBytes();
    *c += 1;
    return Datum::FromNull();
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(COUNT_finalize, 932)
BUILTIN_ARGTYPE(__INTERNAL)
{
    uint64_t *c = (uint64_t*) FMGR_ARG(0).GetVarlenBytes();
    return Datum::From(*c);
}

BUILTIN_RETTYPE(__INTERNAL)
BUILTIN_FUNC(primitive_SUM_init, 933)
BUILTIN_ARGTYPE()
{
    SumState *s = (SumState*) aligned_alloc(8, sizeof(SumState));
    memset(s, 0, sizeof(SumState));
    s->m_empty = true;
    return Datum::FromVarlenBytes(wrap_malloc(s), sizeof(SumState));
}

BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(int_SUM_finalize, 934)
BUILTIN_ARGTYPE(__INTERNAL)
{
    SumState *c = (SumState *) FMGR_ARG(0).GetVarlenBytes();
    if (c->m_empty) {
        return Datum::FromNull();
    }
    return Datum::From(c->m_agg.m_signed);
}

BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(uint_SUM_finalize, 935)
BUILTIN_ARGTYPE(__INTERNAL)
{
    SumState *c = (SumState *) FMGR_ARG(0).GetVarlenBytes();
    if (c->m_empty) {
        return Datum::FromNull();
    }
    return Datum::From(c->m_agg.m_unsigned);
}

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(floating_SUM_finalize, 936)
BUILTIN_ARGTYPE(__INTERNAL)
{
    SumState *c = (SumState *) FMGR_ARG(0).GetVarlenBytes();
    if (c->m_empty) {
        return Datum::FromNull();
    }
    return Datum::From(c->m_agg.m_float);
}

BUILTIN_RETTYPE(__INTERNAL)
BUILTIN_FUNC(AVG_init, 937)
BUILTIN_ARGTYPE()
{
    AvgState *s = (AvgState *) aligned_alloc(8, sizeof(AvgState));
    memset(s, 0, sizeof(AvgState));
    return Datum::FromVarlenBytes(wrap_malloc(s), sizeof(AvgState));
}

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(AVG_finalize, 938)
BUILTIN_ARGTYPE(__INTERNAL)
{
    AvgState *s = (AvgState *) FMGR_ARG(0).GetVarlenBytes();
    if (s->m_cnt == 0) {
        return Datum::FromNull();
    }
    return Datum::From(s->m_sum / s->m_cnt);
}

BUILTIN_RETTYPE(__INTERNAL)
BUILTIN_FUNC(MINMAX_init, 939)
BUILTIN_ARGTYPE()
{
    PrimitiveMinMaxState *s =
        (PrimitiveMinMaxState *) aligned_alloc(8, sizeof(PrimitiveMinMaxState));
    memset(s, 0, sizeof(PrimitiveMinMaxState));
    s->m_empty = true;
    return Datum::FromVarlenBytes(wrap_malloc(s), sizeof(PrimitiveMinMaxState));
}

}   // namespace taco

