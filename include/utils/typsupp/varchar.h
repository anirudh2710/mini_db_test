#ifndef UTILS_TYPSUPP_VARCHAR_H
#define UTILS_TYPSUPP_VARCHAR_H

#include "tdb.h"

namespace taco {

/*!
 * Converts a VARCHAR-typed datum into a internal string. Different from the
 * output function VARCHAR_out, this does not copy the content and thus is
 * useful for avoiding an unncessary copy if the caller can make sure the datum
 * is still alive when the returned string is accessed.
 */
absl::string_view varchar_to_string_view(DatumRef datum);

}

#endif  // UTILS_TYPSUPP_VARCHAR_H
