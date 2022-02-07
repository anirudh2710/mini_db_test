#ifndef INDEX_IDXTYPS_H
#define INDEX_IDXTYPS_H

#define IDXTYP_INVALID 0
#define IDXTYP_VOLATILETREE 1
#define IDXTYP_BTREE 2

#define NUM_IDXTYPS 2

#ifdef IDXTYP_CONSTANT_ONLY
#define IDXTYP(indextype) CONCAT(IDXTYP_, indextype)

#else
#include "tdb.h"

namespace taco {

// typedef IdxType moved to base/tdb_base.h

#define IDXTYP(indextype) ((IdxType) CONCAT(IDXTYP_, indextype))

inline static constexpr bool
IdxTypeIsValid(IdxType idxtyp) {
    return idxtyp != IDXTYP(INVALID) && idxtyp <= NUM_IDXTYPS;
}

absl::string_view IdxTypeGetName(IdxType idx);
bool IdxTypeIsVolatile(IdxType idx);
bool IdxTypeNeedsEqualOperator(IdxType idx);
bool IdxTypeNeedsLessOperator(IdxType idx);

}   // namespace taco

#endif  // IDXTYP_CONSTANT_ONLY

#endif      // INDEX_IDXTYPS_H
