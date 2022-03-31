#include "index/btree/BTreeInternal.h"

#include "storage/VarlenDataPage.h"

namespace taco {

void
BTreeMetaPageData::Initialize(char *btmeta_buf, PageNumber root_pid) {
    auto btmeta = (BTreeMetaPageData *) btmeta_buf;
    btmeta->m_root_pid = root_pid;
}

void
BTreePageHeaderData::Initialize(char *pagebuf,
                                uint16_t flags,
                                PageNumber prev_pid,
                                PageNumber next_pid) {
    VarlenDataPage::InitializePage(pagebuf, BTreePageHeaderSize);
    VarlenDataPage pg(pagebuf);
    BTreePageHeaderData *btpghdr = (BTreePageHeaderData *) pg.GetUserData();
    btpghdr->m_flags = flags;
    btpghdr->m_prev_pid = prev_pid;
    btpghdr->m_next_pid = next_pid;
    btpghdr->m_totrlen = 0;
}

BufferId
BTree::CreateNewBTreePage(bool isroot,
                          bool isleaf,
                          PageNumber prev_pid,
                          PageNumber next_pid) {
    // TODO implement it
    return INVALID_BUFID;
}

BufferId
BTree::GetBTreeMetaPage() {
    // TODO implement it
    return INVALID_BUFID;
}

void
BTree::CreateLeafRecord(const IndexKey *key,
                        const RecordId &recid,
                        maxaligned_char_buf &recbuf) {
    // TODO implement it
}

void
BTree::CreateInternalRecord(const Record &child_recbuf,
                            PageNumber child_pid,
                            bool child_isleaf,
                            maxaligned_char_buf &recbuf) {
    // TODO implement it
}

}   // namespace taco
