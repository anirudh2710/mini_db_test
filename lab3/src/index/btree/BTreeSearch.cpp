#include "index/btree/BTreeInternal.h"

#include "index/tuple_compare.h"

namespace taco {

int
BTree::BTreeTupleCompare(const IndexKey *key,
                         const RecordId &recid,
                         const char *recbuf,
                         bool isleaf) const {
    // TODO implement it
    return 0;
}

SlotId
BTree::BinarySearchOnPage(char *buf,
                          const IndexKey *key,
                          const RecordId &recid) {
    // TODO implement it
    return INVALID_SID;
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
    // TODO implement it
    return INVALID_BUFID;
}


}   // namespace taco

