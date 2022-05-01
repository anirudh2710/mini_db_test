#include "index/btree/BTreeInternal.h"

#include "index/tuple_compare.h"

namespace taco {

int
BTree::BTreeTupleCompare(const IndexKey *key,
                         const RecordId &recid,
                         const char *recbuf,
                         bool isleaf) const {
    RecordId rec_id;
    char* payload;
    int temp;
    
    if(!isleaf)
    {
        BTreeInternalRecordHeaderData* head = (BTreeInternalRecordHeaderData*)recbuf;       
        payload = head->GetPayload();
        rec_id = head->m_heap_recid;
        
    }
    else{
        BTreeLeafRecordHeaderData* lhead = (BTreeLeafRecordHeaderData*)recbuf;

        payload = lhead->GetPayload();
        rec_id = lhead->m_recid; 
    }
    temp = TupleCompare(key,payload, this->GetKeySchema(), m_lt_funcs.data(), m_eq_funcs.data());
    if(temp==0){
        if(this->GetKeySchema()->GetNumFields() > key->GetNumKeys()) 
        {
            return -1;
        }
        if(!recid.IsValid())
        {
            return -1;
        } 
        
        if(rec_id==recid)  
        {
            return 0;}
        else if (rec_id>recid) 
        {
            return -1;}
        else
        {
            return 1;
        }
    }
    return temp;
}


BufferId
BTree::FindLeafPage(const IndexKey *key,
                    const RecordId &recid,
                    std::vector<PathItem> *p_search_path) {

    ScopedBufferId metabuffer;
    char *bufer;
    PageNumber page_no; 

    metabuffer = GetBTreeMetaPage();
    BTreeMetaPageData *leaf =
        ((BTreeMetaPageData *) g_bufman->GetBuffer(metabuffer));
    page_no = leaf->m_root_pid;
    g_bufman->UnpinPage(metabuffer);

    if (p_search_path)
    {
        p_search_path->clear();
    }

    BufferId buf_id = g_bufman->PinPage(page_no, &bufer);

    return FindLeafPage(buf_id, key, recid, p_search_path);
}


SlotId
BTree::BinarySearchOnPage(char *buf,
                          const IndexKey *key,
                          const RecordId &recid) {
    VarlenDataPage varlen(buf);
    BTreePageHeaderData *head = (BTreePageHeaderData *) varlen.GetUserData();

    
    //return Search(key,recid, head-> IsLeafPage(), &varlen, varlen.GetMinSlotId(), varlen.GetMaxSlotId())
    
}

BufferId
BTree::FindLeafPage(BufferId bufid,
                    const IndexKey *key,
                    const RecordId &recid,
                    std::vector<PathItem> *p_search_path) {

    VarlenDataPage pg(g_bufman->GetBuffer(bufid));
    BTreePageHeaderData *head = (BTreePageHeaderData *) pg.GetUserData();
    if(head->IsLeafPage()) 
    {
        return bufid;
    }

    else
    {
        BTreeInternalRecordHeaderData *rec_head;
        char* buffer = g_bufman->GetBuffer(bufid);
        SlotId sid;
        if(key==nullptr)
        {
            sid = MinSlotId;
        } 
        else
        {
            sid = BinarySearchOnPage(buffer, key, recid);
        }

        Record rec = pg.GetRecord(sid);
        rec_head = (BTreeInternalRecordHeaderData*)(rec.GetData());
        rec.GetRecordId().pid=g_bufman->GetPageNumber(bufid);

        if(p_search_path!=NULL)
        {
            p_search_path->push_back(rec.GetRecordId());
        }

        g_bufman->UnpinPage(bufid);
        BufferId bufid = g_bufman->PinPage(rec_head->m_child_pid, &buffer);

        return FindLeafPage(bufid, key, recid, p_search_path);
    }
    return INVALID_BUFID;
}


}   // namespace taco

