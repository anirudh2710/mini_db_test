DEFINE_SYSTABLE(Type, 2, "stores all types known to TDB")
DEFINE_SYSTABLE_FIELD(OID, typid, "the type ID")
DEFINE_SYSTABLE_FIELD(INT2, typlen,
    "the length of a type if it is fixed-length, "
    "or -1 if it is a variable-length field")
DEFINE_SYSTABLE_FIELD(BOOL, typisvarlen, "whether it is variable-length")
DEFINE_SYSTABLE_FIELD(BOOL, typbyref, "whether the value should be passed by reference in memory")
DEFINE_SYSTABLE_FIELD(UINT1, typalign,
    "the alignment requirement of the type. It may only be a power of 2.")
DEFINE_SYSTABLE_FIELD(VARCHAR(NAMELEN), typname, "the type name")

// internal types do not need to have any of the I/O functions
DEFINE_SYSTABLE_FIELD_OPT(OID, typinfunc, 0, "the input function of this type")
DEFINE_SYSTABLE_FIELD_OPT(OID, typoutfunc, 0, "the output function of this type")

// typlenfunc is only needed for a fixed-length type that has a type parameter
DEFINE_SYSTABLE_FIELD_OPT(OID, typlenfunc, 0,
    "the function for calculating the type length for a fixed-length type that "
    "has a type parameter")

DEFINE_SYSTABLE_INDEX(Type, true, typid)
DEFINE_SYSTABLE_INDEX(Type, true, typname)

