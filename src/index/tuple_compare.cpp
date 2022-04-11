#include "index/tuple_compare.h"

namespace taco {

int
TupleCompare(const IndexKey *key,
             const char *tuplebuf,
             const Schema *schema,
             const FunctionInfo *lt_funcs,
             const FunctionInfo *eq_funcs) {
    FieldId nkeys = schema->GetNumFields();

    if (key->GetNumKeys() > nkeys) {
        LOG(kFatal, "key must not have more fields than the schema does: "
                    "key has " FIELDID_FORMAT ", schema has " FIELDID_FORMAT,
                    key->GetNumKeys(), nkeys);
    }

    if (key->GetNumKeys() < nkeys) {
        nkeys = key->GetNumKeys();
    }

    for (FieldId keyid = 0; keyid < nkeys; ++keyid) {
        bool key_field_is_null = key->IsNull(keyid);
        bool tuple_field_is_null = schema->FieldIsNull(keyid, tuplebuf);
        if (key_field_is_null) {
            if (tuple_field_is_null) {
                continue;
            } else {
                return -1;
            }
        } else {
            if (tuple_field_is_null) {
                return 1;
            } else {
                DatumRef key_field = key->GetKey(keyid);
                Datum tuple_field = schema->GetField(keyid, tuplebuf);
                Datum res =
                    FunctionCall(eq_funcs[keyid], key_field, tuple_field);
                ASSERT(!res.isnull());
                if (res.GetBool()) {
                    continue;
                }

                res = FunctionCall(lt_funcs[keyid], key_field, tuple_field);
                ASSERT(!res.isnull());
                if (res.GetBool()) {
                    return -1;
                } else {
                    return 1;
                }
            }
        }
    }

    // compares equal for all keys
    return 0;
}

bool
TupleEqual(const IndexKey *key,
           const char *tuplebuf,
           const Schema *schema,
           const FunctionInfo *eq_funcs) {
    FieldId nkeys = schema->GetNumFields();

    if (key->GetNumKeys() > nkeys) {
        LOG(kFatal, "key must not have more fields than the schema does: "
                    "key has " FIELDID_FORMAT ", schema has " FIELDID_FORMAT,
                    key->GetNumKeys(), nkeys);
    }

    if (key->GetNumKeys() < nkeys) {
        nkeys = key->GetNumKeys();
    }

    for (FieldId keyid = 0; keyid < nkeys; ++keyid) {
        bool key_field_is_null = key->IsNull(keyid);
        bool tuple_field_is_null = schema->FieldIsNull(keyid, tuplebuf);

        if (key_field_is_null) {
            if (tuple_field_is_null) {
                continue;
            } else {
                return false;
            }
        } else {
            if (tuple_field_is_null) {
                return false;
            } else {
                DatumRef key_field = key->GetKey(keyid);
                Datum tuple_field = schema->GetField(keyid, tuplebuf);
                Datum res =
                    FunctionCall(eq_funcs[keyid], key_field, tuple_field);
                ASSERT(!res.isnull());
                if (res.GetBool()) {
                    continue;
                } else {
                    return false;
                }
            }
        }
    }

    return true;
}

}   // namespace taco
