DEFINE_SYSTABLE(Index, 7, "stores all the indexes in TDB")
DEFINE_SYSTABLE_FIELD(OID, idxid, "the index ID")
DEFINE_SYSTABLE_FIELD(OID, idxtabid, "the table id of that the index is built on (may be InvalidOid)")
DEFINE_SYSTABLE_FIELD(UINT1, idxtyp, "the type id of the index, see index/idxtyps.h")
DEFINE_SYSTABLE_FIELD(BOOL, idxunique, "whether this is a unique index")
DEFINE_SYSTABLE_FIELD(INT2, idxncols, "the number of key columns")
DEFINE_SYSTABLE_FIELD(UINT4, idxfid, "the file ID of the index if any")
DEFINE_SYSTABLE_FIELD(VARCHAR(NAMELEN), idxname, "the index name")

DEFINE_SYSTABLE_INDEX(Index, true, idxid)
DEFINE_SYSTABLE_INDEX(Index, true, idxname)
DEFINE_SYSTABLE_INDEX(Index, false, idxtabid)
DEFINE_SYSTABLE_INDEX(Index, false, idxtyp)

