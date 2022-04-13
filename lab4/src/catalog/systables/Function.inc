DEFINE_SYSTABLE(Function, 4, "stores all function known to TDB")
DEFINE_SYSTABLE_FIELD(OID, funcid, "the function ID")
DEFINE_SYSTABLE_FIELD(INT2, funcnargs, "the number of arguments")
DEFINE_SYSTABLE_FIELD(OID, funcrettypid,
    "the return type id. If it is InvalidOid (0), this function does not have a return value (i.e., the return value is always NULL).")
DEFINE_SYSTABLE_FIELD(VARCHAR(NAMELEN), funcname, "the function name")

DEFINE_SYSTABLE_INDEX(Function, true, funcid)
DEFINE_SYSTABLE_INDEX(Function, true, funcname)
