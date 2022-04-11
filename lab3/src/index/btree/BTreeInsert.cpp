#include "index/btree/BTreeInternal.h"
#include "index/tuple_compare.h"
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
    //TODO
    char *bufer;
    int temp;
    char* payload;
    bufer = g_bufman->GetBuffer(bufid);

    SlotId slot_id = BinarySearchOnPage(bufer, key, recid);
    VarlenDataPage page_no(bufer);
    
    
    if(GetIndexDesc()->GetIndexEntry()->idxunique() && slot_id >= page_no.GetMinSlotId())
    {
        BTreeLeafRecordHeaderData* head = (BTreeLeafRecordHeaderData*)page_no.GetRecord(slot_id).GetData();
        payload = head->GetPayload();
        temp = TupleCompare(key, payload, this->GetKeySchema(), m_lt_funcs.data(), m_eq_funcs.data());
        if(temp==0) 
        {
            return INVALID_SID;
        }
    }
    ret_val = slot_id + 1;


    return ret_val;
}

void
BTree::InsertRecordOnPage(BufferId bufid,
                          SlotId insertion_sid,
                          maxaligned_char_buf &&recbuf,
                          std::vector<PathItem> &&search_path) {
    //TODOO
    char* bufer;
    Record recd(recbuf);

    bufer = g_bufman->GetBuffer(bufid);
    VarlenDataPage page_no(bufer);
    
    page_no.InsertRecordAt(insertion_sid, recd);

    if(recd.GetRecordId()==INVALID_SID)
    {
        maxaligned_char_buf int_rec = SplitPage(bufid, insertion_sid, std::move(recbuf));
        if(search_path.size()==0)
        {
            BTreePageHeaderData* lhead = (BTreePageHeaderData*)page_no.GetUserData();
            PageNumber rpg_no = lhead->m_next_pid;
            CreateNewRoot(bufid, std::move(int_rec));
            char* rightPageBuf;
            bufid = g_bufman->PinPage(rightPgNumber, &rightPageBuf);
        }
        
        //BufferId new_bufid = g_bufman->UnpinPage(pageHeaderData->m_next_pid);
        //InsertRecordOnPage()
    }
    g_bufman->UnpinPage(bufid);
}

void
BTree::CreateNewRoot(BufferId bufid,
                     maxaligned_char_buf &&recbuf) {
    //TODO
    BufferId rootPageBufId = CreateNewBTreePage(true,false,InvalidOid,InvalidOid);
    char* rootPageBuf = g_bufman->GetBuffer(rootPageBufId);
    PageNumber rootPgNumber = g_bufman->GetPageNumber(rootPageBufId);
    VarlenDataPage rootPg(rootPageBuf);

    char* leftPageBuf = g_bufman->GetBuffer(bufid);
    PageNumber leftPgNumber = g_bufman->GetPageNumber(bufid);
    VarlenDataPage leftPg(leftPageBuf);
    BTreePageHeaderData* lhead = (BTreePageHeaderData*)leftPg.GetUserData();
    lhead->m_flags = BTREE_PAGE_ISLEAF;

    char* rightPageBuf;
    BufferId rightPageBufId = g_bufman->PinPage(lhead->m_next_pid, &rightPageBuf);
    VarlenDataPage rightPg(rightPageBuf);
    Record r(recbuf);
    char* leftbuf;
    BTreeInternalRecordHeaderData* firstRecord = (BTreeInternalRecordHeaderData*)leftbuf;
    firstRecord->m_child_pid = leftPgNumber;
    Record temp(leftbuf, sizeof(leftbuf));
    rootPg.InsertRecordAt(rootPg.GetMinSlotId(), temp);
    rootPg.InsertRecordAt(rootPg.GetMinSlotId()+1, r);

    BufferId bid = GetBTreeMetaPage(); 
    char* metaPage = g_bufman->GetBuffer(bid);
    BTreeMetaPageData::Initialize(metaPage,rootPgNumber);
    g_bufman->UnpinPage(rootPageBufId); //new root page
    g_bufman->UnpinPage(bid);//meta page
    g_bufman->UnpinPage(rightPageBufId);//right leaf page
    g_bufman->UnpinPage(bufid);//Left leaf or old root                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
}

maxaligned_char_buf
BTree::SplitPage(BufferId bufid,
                 SlotId insertion_sid,
                 maxaligned_char_buf &&recbuf) {
    //TODOO
    char* buf = g_bufman->GetBuffer(bufid);
    VarlenDataPage pg(buf);
    PageNumber currPage = g_bufman->GetPageNumber(bufid);
    BTreePageHeaderData* pageHeaderData = (BTreePageHeaderData*)pg.GetUserData();
    BufferId new_bufid =
        CreateNewBTreePage(pageHeaderData->IsRootPage(), pageHeaderData->IsLeafPage(), currPage, pageHeaderData->m_next_pid);
    PageNumber nextPage = g_bufman->GetPageNumber(new_bufid);
    pageHeaderData->m_next_pid = nextPage;
    char* new_buf = g_bufman->GetBuffer(new_bufid);
    VarlenDataPage new_page(new_buf);
    SlotId mid = pg.GetMaxSlotId()/2;
    //Moving one extra record if insertion_sid is less than mid
    mid = insertion_sid<=mid ? mid : mid+1;
    for(SlotId start=mid;start<=pg.GetMaxSlotId();start++){
        Record r = pg.GetRecord(start);
        new_page.InsertRecord(r);
        r.GetRecordId().pid = nextPage;
    }
    for(SlotId end=pg.GetMaxSlotId();end>=mid;end--){
        pg.RemoveSlot(end);
    }

    Record new_record(recbuf);
    if(insertion_sid<=mid){
        pg.InsertRecordAt(insertion_sid, new_record);
        new_record.GetRecordId().pid = currPage;
    } 
    else{
        new_page.InsertRecordAt(insertion_sid, new_record);
        new_record.GetRecordId().pid = nextPage;
    }
    maxaligned_char_buf firstRecord;
    CreateInternalRecord(new_page.GetRecord(new_page.GetMinSlotId()), nextPage, true, firstRecord);
    g_bufman->UnpinPage(new_bufid);
    return firstRecord;
}

}   // namespace taco
