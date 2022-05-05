#include "index/btree/BTreeInternal.h"

#include "index/tuple_compare.h"

namespace taco {

int
BTree::BTreeTupleCompare(const IndexKey *key,
                         const RecordId &recid,
                         const char *recbuf,
                         bool isleaf) const {
    // compare the actual keys
    const char *rec_payload = BTreeRecordGetPayload(recbuf, isleaf);
    int cmpres = TupleCompare(key, rec_payload, GetKeySchema(),
                              m_lt_funcs.data(), m_eq_funcs.data());
    if (cmpres != 0) {
        return cmpres;
    }

    // TupleCompare() may return 0 when key is a prefix of the fields in
    // recbuf.  A key with fewer fields than the key schema does is smaller
    // than any key that is a prefix of it. In this case, the caller should not
    // have specified a valid record id (because all items on the leaf page
    // should have complete keys).
    const FieldId nkeys = key->GetNumKeys();
    const FieldId idxncols = GetKeySchema()->GetNumFields();
    if (nkeys < idxncols) {
        ASSERT(!recid.IsValid());
        return -1;
    }

    // Number of keys matches. An invalid recid is always considered as smaller
    // than all valid recid. Otherwise, we may need to compare the recid.
    if (!recid.IsValid()) {
        return -1;
    }
    const RecordId &heap_recid =
        BTreeRecordGetHeapRecordId(recbuf, isleaf);
    if (recid == heap_recid) {
        return 0;
    }
    return (recid < heap_recid) ? -1 : 1;
}

SlotId
BTree::BinarySearchOnPage(char *buf,
                          const IndexKey *key,
                          const RecordId &recid) {
    VarlenDataPage pg(buf);
    auto hdr = (BTreePageHeaderData *) pg.GetUserData();
    bool isleaf = hdr->IsLeafPage();

    // find the first key > (key, recid)
    // invariant: for i < low: (k_i, recid_i) <= (key, recid);
    // for i >= high: (k_i, recid_i) > (key, recid).
    SlotId low = pg.GetMinSlotId();
    SlotId high = pg.GetMaxSlotId() + 1;
    while (low != high) {
        SlotId mid = (low + high) >> 1;

        int res;
        if (!isleaf && mid == pg.GetMinSlotId()) {
            // the first index record on an internal page does not store a key
            // and is always assumed to be -infinity
            res = 1;
        } else {
            char *recbuf = pg.GetRecordBuffer(mid, nullptr);
            res = BTreeTupleCompare(key, recid, recbuf, isleaf);
        }

        if (res >= 0) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    // we're asking for the last one <= (key, recid), hence -1
    return low - 1;
}

BufferId
BTree::FindLeafPage(const IndexKey *key,
                    const RecordId &recid,
                    std::vector<PathItem> *p_search_path) {
    ScopedBufferId meta_bufid = GetBTreeMetaPage();
    BTreeMetaPageData *meta =
        ((BTreeMetaPageData *) g_bufman->GetBuffer(meta_bufid));
    PageNumber pid = meta->m_root_pid;
    g_bufman->UnpinPage(meta_bufid);

    if (p_search_path)
        p_search_path->clear();

    char *buf;
    BufferId bufid = g_bufman->PinPage(pid, &buf);
    return FindLeafPage(bufid, key, recid, p_search_path);
}

BufferId
BTree::FindLeafPage(BufferId bufid,
                    const IndexKey *key,
                    const RecordId &recid,
                    std::vector<PathItem> *p_search_path) {
    ScopedBufferId bufid_(bufid);

    char *buf = g_bufman->GetBuffer(bufid_);
    VarlenDataPage pg(buf);
    auto hdr = (BTreePageHeaderData *) pg.GetUserData();

    // this is a leaf page, just return the buffer
    if (hdr->IsLeafPage()) {
        return bufid_.Release();
    }

    SlotId sid;
    if (!key) {
        // the caller is asking for the left-most page
        sid = pg.GetMinSlotId();
    } else {
        // binary search for the insertion point
        sid = BinarySearchOnPage(buf, key, recid);
    }
    ASSERT(pg.IsOccupied(sid));

    // read and follow the child page pointer at slot sid
    auto recbuf =
        (BTreeInternalRecordHeaderData *) pg.GetRecordBuffer(sid, nullptr);
    PageNumber cpid = recbuf->m_child_pid;

    // push the parent page info to serach_path before we search the next level
    PageNumber pid = g_bufman->GetPageNumber(bufid_);
    if (p_search_path)
        p_search_path->emplace_back(RecordId{pid, sid});

    g_bufman->UnpinPage(bufid_);
    bufid_ = g_bufman->PinPage(cpid, &buf);
    return FindLeafPage(bufid_.Release(), key, recid, p_search_path);
}


}   // namespace taco

