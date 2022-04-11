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

SlotId
BTree::Search(const IndexKey *key,const RecordId &recid,bool isleaf,VarlenDataPage* varlen, SlotId startid, SlotId endid) 
{
    if (startid>endid)
    {
        if(isleaf){
            return INVALID_SID;
        }
        else{
            return 1;
        }
    }

    if( startid == endid ){
        Record rec_mid = varlen->GetRecord(startid);
        //we will check the condition of temp_mid
        int temp_mid = BTreeTupleCompare(key, recid, rec_mid.GetData(), isleaf);
        if(temp_mid==1 || temp_mid==0) 
        {
            return startid;
        }
        if(startid>1){
            Record rec_bef = varlen->GetRecord(startid-1);

            int temp_bef = BTreeTupleCompare(key, recid, rec_bef.GetData(), isleaf);
            //we will check the condition of temp_bef
            return temp_bef==1 || temp_bef==0 ? startid-1 : isleaf ? INVALID_SID : 1;
        }
        if (isleaf) 
        {
            return INVALID_SID;
        } 
        else{
            return 1;
        } 
    }
        
    Record rec_mid = varlen->GetRecord(startid + (endid - startid) / 2);

    int temp = BTreeTupleCompare(key, recid, rec_mid.GetData(), isleaf);

    if(temp==0) 
    {
        return startid + (endid - startid) / 2;}
    if( temp==-1 )
    {
        return Search( key, recid, isleaf,varlen, startid, startid + (endid - startid) / 2 );}

    int value= Search( key, recid, isleaf, varlen, (startid + (endid - startid) / 2) +1, endid );

    if (value == -1)
    {
        return (startid + (endid - startid) / 2);
    }
    else {
        return value;
    }
}

SlotId
BTree::BinarySearchOnPage(char *buf,
                          const IndexKey *key,
                          const RecordId &recid) {
    VarlenDataPage varlen(buf);

    BTreePageHeaderData *head = (BTreePageHeaderData *) varlen.GetUserData();

    return Search(key, recid, head->IsLeafPage(), &varlen, varlen.GetMinSlotId(), varlen.GetMaxSlotId());
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

