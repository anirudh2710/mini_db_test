DEFINE_SYSTABLE(Table, 1, "stores all tables in TDB")
DEFINE_SYSTABLE_FIELD(OID, tabid, "the table ID")
DEFINE_SYSTABLE_FIELD(BOOL, tabissys, "whether this table is a catalog table")
DEFINE_SYSTABLE_FIELD(BOOL, tabisvarlen,
    "whether this table is stored as a variable-length heap file")
DEFINE_SYSTABLE_FIELD(INT2, tabncols, "the number of columns")
DEFINE_SYSTABLE_FIELD(UINT4, tabfid, "the file ID of the heap file of the table")
DEFINE_SYSTABLE_FIELD(VARCHAR(NAMELEN), tabname, "the table name")

DEFINE_SYSTABLE_INDEX(Table, true, tabid)
DEFINE_SYSTABLE_INDEX(Table, true, tabname)
