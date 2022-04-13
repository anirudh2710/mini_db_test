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
    BTreeLeafRecordHeaderData* head;
    bufer = g_bufman->GetBuffer(bufid);

    SlotId slot_id = BinarySearchOnPage(bufer, key, recid);
    VarlenDataPage page_no(bufer);
    
    
    if(GetIndexDesc()->GetIndexEntry()->idxunique() && slot_id >= page_no.GetMinSlotId())
    {
        head = (BTreeLeafRecordHeaderData*)page_no.GetRecord(slot_id).GetData();
        payload = head->GetPayload();
        temp = TupleCompare(key, payload, this->GetKeySchema(), m_lt_funcs.data(), m_eq_funcs.data());
        if(temp==0) 
        {
            return INVALID_SID;
        }
    }
    int ret_val = slot_id + 1;


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
    BTreePageHeaderData* lhead;
    char* r_buf;
    bufer = g_bufman->GetBuffer(bufid);
    VarlenDataPage page_no(bufer);
    
    page_no.InsertRecordAt(insertion_sid, recd);

    if(recd.GetRecordId()==INVALID_SID)
    {
        maxaligned_char_buf int_rec = SplitPage(bufid, insertion_sid, std::move(recbuf));
        if(search_path.size()==0)
        {
            lhead = (BTreePageHeaderData*)page_no.GetUserData();
            PageNumber rpg_no = lhead->m_next_pid;
            CreateNewRoot(bufid, std::move(int_rec));
            bufid = g_bufman->PinPage(rpg_no, &r_buf);
        }
    }
    g_bufman->UnpinPage(bufid);
}

void
BTree::CreateNewRoot(BufferId bufid,
                     maxaligned_char_buf &&recbuf) {
    //TODO
    char* rpage_buffer;
    char* lpage_bufer;
    char* r_buf;
    char* l_buf;
    char* meta_pg;
    BTreeInternalRecordHeaderData* recd_val;
    BufferId root_bufid;
    
    root_bufid = CreateNewBTreePage(true,false,InvalidOid,InvalidOid);
    rpage_buffer = g_bufman->GetBuffer(root_bufid);
    VarlenDataPage root_pg(rpage_buffer);

    PageNumber root_pgno = g_bufman->GetPageNumber(root_bufid);
    

    lpage_bufer = g_bufman->GetBuffer(bufid);
    PageNumber left_pgno = g_bufman->GetPageNumber(bufid);
    VarlenDataPage left_pg(lpage_bufer);

    BTreePageHeaderData* lhead = (BTreePageHeaderData*)left_pg.GetUserData();
    lhead->m_flags = BTREE_PAGE_ISLEAF;

    
    BufferId r_bufid = g_bufman->PinPage(lhead->m_next_pid, &r_buf);
    VarlenDataPage rightPg(r_buf);

    Record recd(recbuf);
    
    recd_val = (BTreeInternalRecordHeaderData*)l_buf;
    recd_val->m_child_pid = left_pgno;
    Record temp(l_buf, sizeof(l_buf));
    root_pg.InsertRecordAt(root_pg.GetMinSlotId(), temp);
    root_pg.InsertRecordAt(root_pg.GetMinSlotId()+1, recd);

    BufferId buffid = GetBTreeMetaPage(); 
    meta_pg = g_bufman->GetBuffer(buffid);

    BTreeMetaPageData::Initialize(meta_pg,root_pgno);

    g_bufman->UnpinPage(root_bufid); 
    g_bufman->UnpinPage(buffid);
    g_bufman->UnpinPage(r_bufid);
    g_bufman->UnpinPage(bufid);                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
}

maxaligned_char_buf
BTree::SplitPage(BufferId bufid,
                 SlotId insertion_sid,
                 maxaligned_char_buf &&recbuf) {
    //TODOO
    char* buffer;
    char* next_buffer;
    buffer = g_bufman->GetBuffer(bufid);
    BTreePageHeaderData* pg_head;
    maxaligned_char_buf recd_val;

    VarlenDataPage pg_no(buffer);
    PageNumber cur_page = g_bufman->GetPageNumber(bufid);
    pg_head = (BTreePageHeaderData*)pg_no.GetUserData();

    BufferId next_bufid =CreateNewBTreePage(pg_head->IsRootPage(), pg_head->IsLeafPage(), cur_page, pg_head->m_next_pid);

    PageNumber next_pg = g_bufman->GetPageNumber(next_bufid);
    pg_head->m_next_pid = next_pg;
    
    next_buffer = g_bufman->GetBuffer(next_bufid);
    VarlenDataPage new_page(next_buffer);

    SlotId slotid = pg_no.GetMaxSlotId()/2;
    if (insertion_sid>slotid)
    {
        slotid = slotid+1;
    }
    SlotId start=slotid;
    while(start<pg_no.GetMaxSlotId())
    {
        Record r = pg_no.GetRecord(start);
        new_page.InsertRecord(r);
        r.GetRecordId().pid = next_pg;
        start++;
    }
    SlotId end=pg_no.GetMaxSlotId();
    while(end>=slotid)
    {
        pg_no.RemoveSlot(end);
        end--;
    }

    Record next_recd(recbuf);
    if(insertion_sid<=slotid)
    {
        pg_no.InsertRecordAt(insertion_sid, next_recd);
        next_recd.GetRecordId().pid = cur_page;
    } 
    else{
        new_page.InsertRecordAt(insertion_sid, next_recd);
        next_recd.GetRecordId().pid = next_pg;
    }

    
    CreateInternalRecord(new_page.GetRecord(new_page.GetMinSlotId()), next_pg, true, recd_val);
    g_bufman->UnpinPage(next_bufid);
    return recd_val;
}

}   // namespace taco
