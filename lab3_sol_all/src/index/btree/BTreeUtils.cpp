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
    PageNumber newpg_pid = m_file->AllocatePage();
    char *newpg_buf;
    BufferId newpg_bufid = g_bufman->PinPage(newpg_pid, &newpg_buf);
    uint16_t flags = 0;
    if (isroot)
        flags |= BTREE_PAGE_ISROOT;
    if (isleaf)
        flags |= BTREE_PAGE_ISLEAF;
    BTreePageHeaderData::Initialize(newpg_buf, flags,
                                    prev_pid, next_pid);
    return newpg_bufid;
}

BufferId
BTree::GetBTreeMetaPage() {
    PageNumber metapg_pid = m_file->GetFirstPageNumber();
    char *meta_pgbuf;
    return g_bufman->PinPage(metapg_pid, &meta_pgbuf);
}

void
BTree::CreateLeafRecord(const IndexKey *key,
                        const RecordId &recid,
                        maxaligned_char_buf &recbuf) {
    recbuf.resize(BTreeLeafRecordHeaderSize, 0);
    auto rechdr = (BTreeLeafRecordHeaderData *) recbuf.data();
    rechdr->m_recid = recid;
    if (-1 ==
        GetKeySchema()->WritePayloadToBuffer(key->ToNullableDatumVector(),
                                             recbuf)) {
        LOG(kError, "index key is too long");
    }
}

void
BTree::CreateInternalRecord(const Record &child_recbuf,
                            PageNumber child_pid,
                            bool child_isleaf,
                            maxaligned_char_buf &recbuf) {
    FieldOffset reclen = child_isleaf ?
        (child_recbuf.GetLength() - BTreeLeafRecordHeaderSize +
         BTreeInternalRecordHeaderSize) :
        child_recbuf.GetLength();
    recbuf.resize(reclen);
    auto rechdr = (BTreeInternalRecordHeaderData*) recbuf.data();
    rechdr->m_child_pid = child_pid;

    const char *payload;
    FieldOffset payload_len;
    if (child_isleaf) {
        auto child_rechdr = (BTreeLeafRecordHeaderData*) child_recbuf.GetData();
        rechdr->m_heap_recid = child_rechdr->m_recid;
        payload = child_rechdr->GetPayload();
        payload_len = child_recbuf.GetLength()
            - BTreeLeafRecordHeaderSize;
    } else {
        auto child_rechdr =
            (BTreeInternalRecordHeaderData*) child_recbuf.GetData();
        rechdr->m_heap_recid = child_rechdr->m_heap_recid;
        payload = child_rechdr->GetPayload();
        payload_len = child_recbuf.GetLength()
            - BTreeInternalRecordHeaderSize;
    }
    memcpy(rechdr->GetPayload(), payload, payload_len);
}

}   // namespace taco
