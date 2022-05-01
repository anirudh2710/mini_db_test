DEFINE_SYSTABLE(Aggregation, 10,
    "stores all aggregations known to TDB. "
    "An aggregation is a set of 3 functions: __INTERNAL initfunc(), "
    "VOID accfunc(__INTERNAL, SOME_DATUM_TYPE), "
    "SOME_RET_TYPE finalizefunc(__INTERNAL), "
    "where the __INTERNAL typed datum is the state maintained by the "
    "aggregation functions.")
DEFINE_SYSTABLE_FIELD(OID, aggid, "the aggregation ID")
DEFINE_SYSTABLE_FIELD(UINT1, aggtid, "the type of the aggregation")
DEFINE_SYSTABLE_FIELD(OID, aggoprtypid,
    "the type of the datum that the aggregation accumulate function works on. "
    "This is InvalidOid (0) if the aggregation may be applied to any type.")
DEFINE_SYSTABLE_FIELD(OID, aggrettypid,
    "the return type of the aggregation finalize function")
DEFINE_SYSTABLE_FIELD(OID, agginitfuncid, "the initialize function ID")
DEFINE_SYSTABLE_FIELD(OID, aggaccfuncid, "the accumulate function ID")
DEFINE_SYSTABLE_FIELD(OID, aggfinalizefuncid, "the finalize function ID")
DEFINE_SYSTABLE_FIELD(VARCHAR(NAMELEN), aggname, "the name of the aggregation")

DEFINE_SYSTABLE_INDEX(Aggregation, true, aggid)
DEFINE_SYSTABLE_INDEX(Aggregation, true, aggoprtypid, aggname)
DEFINE_SYSTABLE_INDEX(Aggregation, false, aggoprtypid, aggtid)

