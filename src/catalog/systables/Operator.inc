DEFINE_SYSTABLE(Operator, 6, "stores all operators known to TDB")
DEFINE_SYSTABLE_FIELD(UINT1, optype, "the type of the operator, see expr/optypes.h")
DEFINE_SYSTABLE_FIELD(OID, opfuncid, "the function ID")
DEFINE_SYSTABLE_FIELD(OID, oparg0typid, "the type id of the operand 0")
DEFINE_SYSTABLE_FIELD(OID, oparg1typid, "the type id of the operand 1")

DEFINE_SYSTABLE_INDEX(Operator, true, opfuncid)
DEFINE_SYSTABLE_INDEX(Operator, true, optype, oparg0typid, oparg1typid)
