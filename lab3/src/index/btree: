#include "index/btree/BTreeInternal.h"
#include "storage/VarlenDataPage.h"
#include "iostream"
namespace taco {

void
BTreeMetaPageData::Initialize(char *btmeta_buf, PageNumber root_pid) {
    auto btmeta = (BTreeMetaPageData *) btmeta_buf;
    btmeta->m_root_pid=root_pid;
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
    char* buf_frame;
    PageNumber page_no;
    BufferId bufid;

    page_no = m_file->AllocatePage();
    
    bufid = g_bufman->PinPage(page_no,&buf_frame);                                    
    
    if(isroot && isleaf)  //when both are true:
    {
        BTreePageHeaderData::Initialize(buf_frame, 
                                        BTREE_PAGE_ISROOT|BTREE_PAGE_ISLEAF, 
                                        prev_pid, 
                                        next_pid);
    }
    else if(isroot && !isleaf)  //when isleaf if false:
    {
        BTreePageHeaderData::Initialize(buf_frame, 
                                    BTREE_PAGE_ISROOT, 
                                    prev_pid, 
                                    next_pid);


    }
    else if(!isroot && isleaf)    //when isroot is false but isleaf is true
    { 
        BTreePageHeaderData::Initialize(buf_frame, 
                                        BTREE_PAGE_ISLEAF, 
                                        prev_pid, 
                                        next_pid);
    }
    else //when both are false
    {
        BTreePageHeaderData::Initialize(buf_frame, 
                                        BTREE_PAGE_ISROOT, 
                                        prev_pid, 
                                        next_pid);
    }
     
    return bufid;
}


BufferId
BTree::GetBTreeMetaPage() {
    PageNumber page_no; 
    char *buf_frame;   
    page_no = m_file->GetFirstPageNumber(); 
    BufferId bufid =g_bufman->PinPage(page_no, &buf_frame);
    return bufid;
}

void
BTree::CreateLeafRecord(const IndexKey *key,
                        const RecordId &recid,
                        maxaligned_char_buf &recbuf) {

    //resizing the recbuf                        
    recbuf.resize(sizeof(BTreeLeafRecordHeaderData));

    BTreeLeafRecordHeaderData* bheader = (BTreeLeafRecordHeaderData*)recbuf.data();
    
    bheader->m_recid = recid;

    this->GetKeySchema()->WritePayloadToBuffer(key->ToNullableDatumVector(), recbuf);
}

void
BTree::CreateInternalRecord(const Record &child_recbuf,
                            PageNumber child_pid,
                            bool child_isleaf,
                            maxaligned_char_buf &recbuf) {
    //resizing the recbuf                              
    recbuf.resize(sizeof(BTreeInternalRecordHeaderData));

    BTreeInternalRecordHeaderData* bheader = (BTreeInternalRecordHeaderData*)recbuf.data();
    
    bheader->m_child_pid = child_pid;

    const char *buffer = child_recbuf.GetData();
    if(child_isleaf){
        BTreeLeafRecordHeaderData* bleafhead = (BTreeLeafRecordHeaderData*)buffer;
        bheader->m_heap_recid = bleafhead->m_recid;
        std::memcpy(recbuf.data()+sizeof(BTreeInternalRecordHeaderData),bleafhead->GetPayload(),sizeof(bleafhead->GetPayload()));
        recbuf.resize(sizeof(BTreeInternalRecordHeaderData)+child_recbuf.GetLength()-sizeof(BTreeLeafRecordHeaderData));
    }else{
        BTreeInternalRecordHeaderData* inthead = (BTreeInternalRecordHeaderData*)buffer;
        bheader->m_heap_recid = inthead->m_heap_recid;
        std::memmove(recbuf.data()+sizeof(BTreeInternalRecordHeaderData),inthead->GetPayload(),sizeof(inthead->GetPayload()));
        recbuf.resize(child_recbuf.GetLength());
    }
}

}   // namespace taco
