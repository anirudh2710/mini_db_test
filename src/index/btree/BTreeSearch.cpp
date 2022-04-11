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
    
    std::function<SlotId(bool, const IndexKey*, const RecordId &,  VarlenDataPage*, SlotId, SlotId)> BinarySearch;
    auto f = [this, &BinarySearch](bool isleaf, const IndexKey *key, const RecordId &recid, VarlenDataPage *varlen, SlotId start_idx, SlotId end_idx) ->SlotId{
	    if(start_idx>end_idx) return isleaf ? INVALID_SID : 1;
	    if( start_idx == end_idx ){
		Record midRecord = varlen->GetRecord(start_idx);
		int val1 = BTreeTupleCompare(key, recid, midRecord.GetData(), isleaf);
		//Binary Search makes sure that if you reach here and val1==1 that means (key,recid) id greater than all the elements before it
		//Hence returning the start_idx from below.
		if(val1==1 || val1==0) return start_idx;
		//If you reach here that means val1==-1 i.e. (key, recid) is smaller and if its the first element i.e. start_idx==1,
		//that means no element smaller than (key,recid) exists.
		if(start_idx>1){
		    //If val1==-1 and if the element just before it i.e. val2==1,
		    //this confirms that (key,recid) is smaller than record at val1 and greater than record at val2
		    //hence we can return the record at val2 from here
		    Record oneRecordBefore = varlen->GetRecord(start_idx-1);
		    int val2 = BTreeTupleCompare(key, recid, oneRecordBefore.GetData(), isleaf);
		    return val2==1 || val2==0 ? start_idx-1 : isleaf ? INVALID_SID : 1;
		}
		return isleaf ? INVALID_SID : 1;
	    }
		

	    int mid_idx = start_idx + (end_idx - start_idx) / 2;
	    Record midRecord = varlen->GetRecord(mid_idx);
	    int val1 = BTreeTupleCompare(key, recid, midRecord.GetData(), isleaf);
	    if(val1==0) return mid_idx;
	    if( val1==-1 )
		return BinarySearch( isleaf, key, recid, varlen, start_idx, mid_idx );

	    int ret = BinarySearch( isleaf, key, recid, varlen, mid_idx+1, end_idx );
	    
	    return ret == -1 ? mid_idx : ret;

    };
    BinarySearch = f;
    return BinarySearch(head-> IsLeafPage(), key,recid, &varlen, varlen.GetMinSlotId(), varlen.GetMaxSlotId());
    
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
