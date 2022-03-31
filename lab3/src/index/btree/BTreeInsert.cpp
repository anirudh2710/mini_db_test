#include "index/btree/BTreeInternal.h"

namespace taco {

bool
BTree::InsertKey(const IndexKey *key, RecordId recid) {
    // find the leaf page to insert into
    std::vector<PathItem> search_path;
    bool idxunique = GetIndexDesc()->GetIndexEntry()->idxunique();

    // If this is a unique index, use the +inf record id instead. If there is a
    // duplicate key (but not duplicate record id) that happens to be the
    // separator key of two sibling internal pages, we will descend into right
    // instead of left and will end up with a page that contains the duplicat
    // key. If there's not, we will end up with a page that also covers the
    // pair to be inserted.
    ScopedBufferId leaf_bufid = FindLeafPage(key,
                                idxunique ? INFINITY_RECORDID : recid,
                                &search_path);

    // find the slot to insert into and check whether there's a duplicate if
    // this index is a unique index
    SlotId sid = FindInsertionSlotIdOnLeaf(key, recid, leaf_bufid);
    if (sid == INVALID_SID) {
        // duplicate key
        return false;
    }

    // construct the index record
    maxaligned_char_buf leaf_recbuf;
    CreateLeafRecord(key, recid, leaf_recbuf);
    if (leaf_recbuf.size() > BTREE_MAX_RECORD_SIZE) {
        LOG(kError, "key is too long for B-tree index: expecting <= "
                    FIELDOFFSET_FORMAT, ", but got " FIELDOFFSET_FORMAT,
                    BTREE_MAX_RECORD_SIZE, leaf_recbuf.size());
    }

    // do the actual insertion
    InsertRecordOnPage(leaf_bufid.Release(),
                       sid,
                       std::move(leaf_recbuf),
                       std::move(search_path));
    return true;
}

SlotId
BTree::FindInsertionSlotIdOnLeaf(const IndexKey *key,
                                 const RecordId &recid,
                                 BufferId bufid) {
    // TODO implement it
    // Hint: you may use GetIndexDesc()->GetIndexEntry()->idxunique() to
    // find out if the index is declared as a unique index

    return INVALID_SID;
}

void
BTree::InsertRecordOnPage(BufferId bufid,
                          SlotId insertion_sid,
                          maxaligned_char_buf &&recbuf,
                          std::vector<PathItem> &&search_path) {
    // TODO implement it
}

void
BTree::CreateNewRoot(BufferId bufid,
                     maxaligned_char_buf &&recbuf) {
    // TODO implement it
}

maxaligned_char_buf
BTree::SplitPage(BufferId bufid,
                 SlotId insertion_sid,
                 maxaligned_char_buf &&recbuf) {
    // TODO implement it
    return {};
}

}   // namespace taco
