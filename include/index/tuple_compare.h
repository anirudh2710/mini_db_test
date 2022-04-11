#ifndef INDEX_TUPLECOMPARE_H
#define INDEX_TUPLECOMPARE_H

#include "tdb.h"

#include "catalog/Schema.h"
#include "index/IndexKey.h"

namespace taco {

/*!
 * Compares the \p key with the tuple serialized in the buffer \p tuplebuf with
 * the \p schema, using the "<" functions in \p lt_funcs, and ">" functions in
 * \p eq_funcs, in lexicographical order. However, null values are considered
 * to be smaller than any non-null and two null values compare equal.
 *
 * When \p key has fewer keys fields than the schema, only a prefix of the
 * fields are compared. Hence, \p key which is a prefix of a key record is
 * considered as equal rather than smaller than that. If the caller wants
 * to interpret that a prefix of a key record as smaller than the key record,
 * it should do further check if the number of fields in the key matches
 * the number of fields in the key record.
 *
 * Returns a -1 if \p key is smaller than the tuple stored in \p
 * tuplebuf; returns 0 if they are equal; or returns +1 if \p
 * key is larger than the tuple stored in \p tuplebuf.
 *
 * It is a fatal error if the \p key has more fields than the schema does.  It
 * is undefined if `!schema->IsLayoutComputed()`; or lt_funcs/eq_funcs do not
 * have as many functions as the number of fields.
 */
int TupleCompare(const IndexKey *key,
                 const char *tuplebuf,
                 const Schema *schema,
                 const FunctionInfo *lt_funcs,
                 const FunctionInfo *eq_funcs);

bool TupleEqual(const IndexKey *key,
                const char *tuplebuf,
                const Schema *schema,
                const FunctionInfo *eq_funcs);

}   // namespace taco

#endif  // INDEX_TUPLECOMPARE_H
