#include "index/btree/BTreeInternal.h"

namespace taco {

void
BTree::BulkLoad(BulkLoadIterator &iter) {
    if (!IsEmpty()) {
        LOG(kError, "can't bulk load a non-empty B-tree");
    }

    // TODO switch to external sorting in project 4
    // nothing to do in B-tree project
    Index::BulkLoad(iter);
}

}   // namespace taco
