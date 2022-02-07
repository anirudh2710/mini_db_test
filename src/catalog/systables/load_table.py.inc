// Load the table definition
fields = []
#define DEFINE_SYSTABLE(_1, _2, ...) \
    tabname_, tabid_ = STRINGIFY(_1), _2
#define DEFINE_SYSTABLE_FIELD(coltyp, colname, ...) \
    fields.append((STRINGIFY(colname), coltyp, CONCAT(X_, coltyp), None))
#define DEFINE_SYSTABLE_FIELD_OPT(coltyp, colname, defaultval, ...) \
    fields.append((STRINGIFY(colname), coltyp, CONCAT(X_, coltyp), defaultval))
#define DEFINE_SYSTABLE_INDEX(...)
#include TABLEDEF
#undef DEFINE_SYSTABLE
#undef DEFINE_SYSTABLE_FIELD
#undef DEFINE_SYSTABLE_FIELD_OPT
#undef DEFINE_SYSTABLE_INDEX


// populate the column information, and also add a python variable
// with the column name to help data loading
optional_fields = {}
field_typids = []
field_isarray = []
field_typparams = []
field_defaultvals = []
for i in range(len(fields)):
    t = fields[i]
    fields[i] = t[0]
    if t[1] < 0:
        field_isarray.append(True)
        field_typids.append(-t[1])
    else:
        field_isarray.append(False)
        field_typids.append(t[1])
    field_typparams.append(t[2])
    field_defaultvals.append(t[3])
    if t[3] is not None:
        optional_fields[t[0]] = t[3]
    exec("{} = '{}'".format(t[0], t[0]))

#ifdef TABLEDATA
datalist = [
#include TABLEDATA
]
#undef TABLEDATA
#endif

// Complete the default values
for t in datalist:
    for col in optional_fields:
        if col not in t:
            t[col] = optional_fields[col]

#undef TABLEDEF

