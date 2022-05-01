DEFINE_SYSTABLE(Cast, 9, "stores all casts known to TDB")
DEFINE_SYSTABLE_FIELD(OID, castfuncid, "the function id of the cast")
DEFINE_SYSTABLE_FIELD(OID, castoprtypid, "the type id of the operand")
DEFINE_SYSTABLE_FIELD(OID, castrettypid, "the type id of the return value")
DEFINE_SYSTABLE_FIELD(BOOL, castisimplicit, "whether this cast can be implicit")

DEFINE_SYSTABLE_INDEX(Cast, true, castfuncid)
DEFINE_SYSTABLE_INDEX(Cast, true, castoprtypid, castrettypid)

