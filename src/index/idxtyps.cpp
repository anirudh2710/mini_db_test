#include "index/idxtyps.h"

namespace taco {

absl::string_view IdxTypeGetName(IdxType idx) {
    switch (idx) {
    case IDXTYP(VOLATILETREE):
        return "volatile tree";
    case IDXTYP(BTREE):
        return "b-tree";
    }

    LOG(kFatal, "unknown index type: %d", (int) idx);
    return "unknown";

}

bool
IdxTypeIsVolatile(IdxType idx) {
    return idx == IDXTYP(VOLATILETREE);
}

bool
IdxTypeNeedsEqualOperator(IdxType idx) {
    switch (idx) {
    case IDXTYP(VOLATILETREE):
    case IDXTYP(BTREE):
        return true;
    }

    LOG(kFatal, "unknown index type: %d", (int) idx);
    return false;
}

bool
IdxTypeNeedsLessOperator(IdxType idx) {
    switch (idx) {
    case IDXTYP(VOLATILETREE):
    case IDXTYP(BTREE):
        return true;
    }

    LOG(kFatal, "unknown index type: %d", (int) idx);
    return false;
}

}   // namespace taco
