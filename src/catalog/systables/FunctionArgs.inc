DEFINE_SYSTABLE(FunctionArgs, 5, "stores the argument information of all functions")
DEFINE_SYSTABLE_FIELD(OID, funcid, "the function ID")
DEFINE_SYSTABLE_FIELD(INT2, funcargid, "the argument ID, starts from 0")
DEFINE_SYSTABLE_FIELD(OID, funcargtypid,
    "the argument's type id. If it is InvalidOid (0), this argument can be any type.")

DEFINE_SYSTABLE_INDEX(FunctionArgs, true, funcid, funcargid)

