#define DEFAULT_BUFPOOL_SIZE 4
#include "index/btree/TestBTree.h"

namespace taco {

using BasicTestBTreeMergePages = TestBTree;

/*!
 * Removes the first n slots from the given B-tree page. This is a meant to be
 * a replacement of VarlenDataPage(pg).ShiftSlots(n, true) for this
 * specific test suite only, which makes a few assumptions that might not hold
 * elsewhere. Do not use it anywhere else!
 *
 */
static void PurgeFirstNSlotsOnPage(char *pg, SlotId n);

// this test also assumes page size is 4096
static_assert(PAGE_SIZE == 4096);

TEST_F(BasicTestBTreeMergePages, TestMergeLeafPageWithUniformReclenSmallKey) {
    TDB_TEST_BEGIN

    // the leaf page in m_idx_1 will fit 202 leaf tuples but not 203
    ASSERT_EQ(BTreeComputePageUsage(202, 16 * 202), 4080);
    ASSERT_GT(BTreeComputePageUsage(203, 16 * 203), 4096);

    ScopedBufferId bufid0 =
        CreateNewBTreePage(m_idx_1, false, true, INVALID_PID, INVALID_PID);
    PageNumber pid0 = g_bufman->GetPageNumber(bufid0);
    // we can unpin page 0 for now
    g_bufman->UnpinPage(bufid0);

    // creates a parent page
    ScopedBufferId pbufid =
        CreateNewBTreePage(m_idx_1, false, false, INVALID_PID, INVALID_PID);
    PageNumber ppid = g_bufman->GetPageNumber(pbufid);
    char *pbuf = g_bufman->GetBuffer(pbufid);
    VarlenDataPage ppg(pbuf);
    // inserts two dummy records into the parent page, we will fill their
    // values a bit later
    //
    // We intentionally do not insert a third record here pointing to pid0,
    // because it may be on a right sibling of the parent page instead. The
    // MergePages() implementation should not use the slot id to find the right
    // sibling of the right page. Instead, it should use the m_next_pid in the
    // header.
    BTreeInternalRecordHeaderData ppg_rec1_data;
    Record ppg_rec1((char*) &ppg_rec1_data, BTreeInternalRecordHeaderSize);
    ASSERT_NO_ERROR(ppg.InsertRecordAt(ppg.GetMaxSlotId() + 1, ppg_rec1));
    ASSERT_NE(ppg_rec1.GetRecordId().sid, INVALID_SID);
    IndexKey *ppg_rec2_key = get_key_f1(203 ^ m_reorder_mask);
    RecordId ppg_rec2_recid{
        (PageNumber)(100 + 203), (SlotId)((203 ^ m_reorder_mask) + 1)};
    maxaligned_char_buf ppg_rec2_buf_tmp;
    maxaligned_char_buf ppg_rec2_buf;
    CreateLeafRecord(m_idx_1, ppg_rec2_key, ppg_rec2_recid, ppg_rec2_buf_tmp);
    CreateInternalRecord(m_idx_1, Record(ppg_rec2_buf_tmp),
                         /* child_pid = */INVALID_PID,
                         true, ppg_rec2_buf);
    Record ppg_rec2(ppg_rec2_buf);
    ASSERT_NO_ERROR(ppg.InsertRecordAt(ppg.GetMaxSlotId() + 1, ppg_rec2));
    ASSERT_NE(ppg_rec2.GetRecordId().sid, INVALID_SID);

    auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
    phdr->m_totrlen = ppg_rec1.GetLength() + ppg_rec2.GetLength();

    // make a copy of the parent page for later overwrites
    alignas(CACHELINE_SIZE) char ppg_copy[PAGE_SIZE];
    memcpy(ppg_copy, pbuf, PAGE_SIZE);

    g_bufman->MarkDirty(pbufid);
    g_bufman->UnpinPage(pbufid);

    // creates two new leaf pages followed by pid0
    ScopedBufferId lbufid =
        CreateNewBTreePage(m_idx_1, false, true, INVALID_PID, INVALID_PID);
    PageNumber lpid = g_bufman->GetPageNumber(lbufid);
    char *lbuf = g_bufman->GetBuffer(lbufid);
    VarlenDataPage lpg(lbuf);
    auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();

    // inserts 1 to 202 into the left page, inserting an additional record will
    // fail
    ASSERT_EQ(lhdr->m_totrlen, 0);
    maxaligned_char_buf recbuf;
    for (int64_t i = 1; i <= 203; ++i) {
        IndexKey *key = get_key_f1(i ^ m_reorder_mask);
        RecordId recid{(PageNumber)(100 + i), (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_1, key, recid, recbuf);
        Record rec(recbuf);
        ASSERT_EQ(recbuf.size(), 16);
        ASSERT_NO_ERROR(lpg.InsertRecordAt(lpg.GetMaxSlotId() + 1, rec));
        ASSERT_EQ(rec.GetRecordId().sid != INVALID_SID, i < 203)
            << "the " << i << "th insertion on the leaf page is expected to "
            << ((i < 203) ? "succeed" : "fail");
        if (i < 203) {
            lhdr->m_totrlen += rec.GetLength();
        }
    }

    // make a copy of the original left page so we can its non fm header
    // portion with the loaded data
    alignas(CACHELINE_SIZE) char lpg_copy[PAGE_SIZE];
    memcpy(lpg_copy, lbuf, PAGE_SIZE);

    // drop the pin for now
    g_bufman->MarkDirty(lbufid);
    g_bufman->UnpinPage(lbufid);

    // Instead of creating a new right page here, we directly initializes
    // in our local buffer since the right page will be deleted in every
    // successful merge test. We can use this to overwrite the non fm header
    // portion of the page.
    alignas(CACHELINE_SIZE) char rpg_copy[PAGE_SIZE];
    std::memset(rpg_copy, 0, PAGE_SIZE);
    BTreePageHeaderData::Initialize(rpg_copy,
                                    BTREE_PAGE_ISLEAF,
                                    /*prev_pid =*/lpid,
                                    /*next_pid =*/pid0);
    VarlenDataPage rpg(rpg_copy);
    auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
    // insert keys 203 - 404, 405 should fail
    for (int64_t i = 203; i <= 405; ++i) {
        IndexKey *key = get_key_f1(i ^ m_reorder_mask);
        RecordId recid{(PageNumber)(100 + i), (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_1, key, recid, recbuf);
        Record rec(recbuf);
        ASSERT_EQ(recbuf.size(), 16);
        ASSERT_NO_ERROR(rpg.InsertRecordAt(rpg.GetMaxSlotId() + 1, rec));
        ASSERT_EQ(rec.GetRecordId().sid != INVALID_SID, i < 405)
            << "the " << i << "th insertion on the leaf page is expected to "
            << ((i < 405) ? "succeed" : "fail");
        if (i < 405) {
            rhdr->m_totrlen += rec.GetLength();
        }
    }

    for (SlotId lpg_nslots: {1, 2, 60, 101, 102, 160, 201, 202})
    for (SlotId rpg_nslots: {203 - lpg_nslots, 202 - lpg_nslots}) {
        // skip the test case where there's no tuple on the leaf page, which
        // can't happen in a valid B-tree
        if (rpg_nslots == 0)
            continue;

        // prepare the pages

        // prepare the right page first
        ScopedBufferId rbufid =
            CreateNewBTreePage(m_idx_1, false, true, INVALID_PID, INVALID_PID);
        PageNumber rpid = g_bufman->GetPageNumber(rbufid);
        char *rbuf = g_bufman->GetBuffer(rbufid);
        // MUST NOT copy the page header data portion as we're allocating
        // and deallocating pages across the loops!
        memcpy(rbuf + sizeof(PageHeaderData), rpg_copy + sizeof(PageHeaderData),
               (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage rpg(rbuf);
        auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
        ASSERT_LE(rpg_nslots, rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1);
        SlotId n_to_truncate_on_right = rpg.GetMaxSlotId() - rpg.GetMinSlotId()
            + 1 - rpg_nslots;
        for (SlotId sid = rpg.GetMinSlotId();
                    sid < rpg.GetMinSlotId() + n_to_truncate_on_right; ++sid) {
            rhdr->m_totrlen -= rpg.GetRecord(sid).GetLength();
        }
        if (n_to_truncate_on_right > 0) {
            ASSERT_NO_ERROR(
                PurgeFirstNSlotsOnPage(rbuf, n_to_truncate_on_right));
            //ASSERT_NO_ERROR(rpg.ShiftSlots(n_to_truncate_on_right, true));
        }
        rhdr->m_prev_pid = lpid;
        rhdr->m_next_pid = pid0;
        g_bufman->MarkDirty(rbufid);
        g_bufman->UnpinPage(rbufid);

        // prepare the page 0 (right most)
        char *buf0;
        ScopedBufferId bufid0 = g_bufman->PinPage(pid0, &buf0);
        VarlenDataPage pg0(buf0);
        auto hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
        hdr0->m_prev_pid = rpid;
        g_bufman->MarkDirty(bufid0);
        g_bufman->UnpinPage(bufid0);

        // prepare the left page
        char *lbuf;
        ScopedBufferId lbufid = g_bufman->PinPage(lpid, &lbuf);
        // MUST NOT copy the page header data portion as we're allocating
        // and deallocating pages across the loops!
        memcpy(lbuf + sizeof(PageHeaderData), lpg_copy + sizeof(PageHeaderData),
               (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage lpg(lbuf);
        ASSERT_LE(lpg_nslots, lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1);
        auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();
        for (SlotId sid = lpg.GetMaxSlotId(); sid > lpg_nslots; --sid) {
            lhdr->m_totrlen -= lpg.GetRecord(sid).GetLength();
            ASSERT_NO_ERROR(lpg.RemoveSlot(sid));
        }
        lhdr->m_next_pid = rpid;
        g_bufman->MarkDirty(lbufid);
        g_bufman->UnpinPage(lbufid);

        // prepare the parent page
        char *pbuf;
        ScopedBufferId pbufid = g_bufman->PinPage(ppid, &pbuf);
        memcpy(pbuf + sizeof(PageHeaderData), ppg_copy + sizeof(PageHeaderData),
              (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage ppg(pbuf);
        ASSERT_EQ(ppg.GetMaxSlotId(), 2);
        auto prechdr1 = (BTreeInternalRecordHeaderData*) ppg.GetRecordBuffer(1, nullptr);
        prechdr1->m_child_pid = lpid;
        auto prechdr2 = (BTreeInternalRecordHeaderData*) ppg.GetRecordBuffer(2, nullptr);
        prechdr2->m_child_pid = rpid;
        g_bufman->MarkDirty(pbufid);
        g_bufman->UnpinPage(pbufid);

        // now force a buffer flush
        ASSERT_NO_ERROR(ForceFlushBuffer());

        // pin the three pages back in the buffer pool
        BufferId lbufid_ = g_bufman->PinPage(lpid, &lbuf);
        BufferId rbufid_ = g_bufman->PinPage(rpid, &rbuf);
        pbufid = g_bufman->PinPage(ppid, &pbuf);

        // and try to merge the left and right page
        bool merge_succeeded;
        ASSERT_NO_ERROR(merge_succeeded =
            MergePages(m_idx_1, lbufid_, rbufid_, pbufid, 1));

        // merge will only succeed if there're no more than 202 records in total
        bool expected_to_succeed = (lpg_nslots + rpg_nslots) <= 202;
        ASSERT_EQ(merge_succeeded, expected_to_succeed);

        // unpin all these pages
        ASSERT_NO_ERROR(g_bufman->UnpinPage(pbufid));
        if (!merge_succeeded) {
            g_bufman->UnpinPage(lbufid_);
            g_bufman->UnpinPage(rbufid_);
        }

        // If the merge is successful, we flush the buffer pool in order to
        // check if the dirty bits are set correctly. Otherwise, we should not
        // flush the buffer pool, in case the pages are modified but the
        // dirty bits were not set.
        if (merge_succeeded) {
            ASSERT_NO_ERROR(ForceFlushBuffer());
        }

        if (merge_succeeded) {
            // check if the right page is freed
            rbufid = g_bufman->PinPage(rpid, &rbuf);
            PageHeaderData *rph = (PageHeaderData*) rbuf;
            ASSERT_FALSE(rph->IsAllocated())
                << "merge should free the right page upon succesful return";
            g_bufman->UnpinPage(rbufid);

            // check if the pid 0 page has the correct left sibling pointer
            bufid0 = g_bufman->PinPage(pid0, &buf0);
            pg0 = buf0;
            hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
            ASSERT_EQ(hdr0->m_prev_pid, lpid);
            ASSERT_EQ(hdr0->m_next_pid, INVALID_PID);
            ASSERT_FALSE(hdr0->IsRootPage());
            ASSERT_TRUE(hdr0->IsLeafPage());
            g_bufman->UnpinPage(bufid0);

            // check if the internal record pointing to the right page is
            // deleted from the parent page
            pbufid = g_bufman->PinPage(ppid, &pbuf);
            ppg = pbuf;
            // should just have one internal record left on the parent page
            ASSERT_EQ(ppg.GetMaxSlotId(), 1);
            ASSERT_EQ(ppg.GetRecord(1).GetLength(), BTreeInternalRecordHeaderSize);
            auto prechdr1 =
                (BTreeInternalRecordHeaderData*) ppg.GetRecordBuffer(1, nullptr);
            ASSERT_EQ(prechdr1->m_child_pid, lpid);
            auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
            ASSERT_EQ(phdr->m_totrlen, BTreeInternalRecordHeaderSize);
            ASSERT_FALSE(phdr->IsRootPage());
            ASSERT_FALSE(phdr->IsLeafPage());
            g_bufman->UnpinPage(pbufid);

            // check if the left page looks good
            lbufid = g_bufman->PinPage(lpid, &lbuf);
            lpg = lbuf;
            lhdr = (BTreePageHeaderData*) lpg.GetUserData();
            ASSERT_EQ(lhdr->m_prev_pid, INVALID_PID);
            ASSERT_EQ(lhdr->m_next_pid, pid0);
            ASSERT_TRUE(lhdr->IsLeafPage());
            ASSERT_FALSE(lhdr->IsRootPage());

            ASSERT_EQ(lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1,
                      lpg_nslots + rpg_nslots);
            VarlenDataPage lpg_old(lpg_copy);
            VarlenDataPage rpg_old(rpg_copy);
            FieldOffset totrlen = 0;
            for (SlotId sid = lpg.GetMinSlotId(); sid <= lpg.GetMaxSlotId();
                    ++sid) {
                Record oldrec;
                if (sid <= lpg_nslots) {
                    oldrec = lpg_old.GetRecord(sid);
                } else {
                    oldrec = rpg_old.GetRecord(sid - lpg_nslots
                                               + n_to_truncate_on_right);
                }
                Record newrec = lpg.GetRecord(sid);
                ASSERT_EQ(newrec.GetLength(), oldrec.GetLength()) << sid;

                auto oldrechdr = (BTreeLeafRecordHeaderData*) oldrec.GetData();
                auto newrechdr = (BTreeLeafRecordHeaderData*) newrec.GetData();
                ASSERT_EQ(newrechdr->m_recid, oldrechdr->m_recid) << sid;

                int64_t oldkey = read_f1_payload(oldrechdr->GetPayload());
                int64_t newkey = read_f1_payload(newrechdr->GetPayload());
                ASSERT_EQ(newkey, oldkey);

                totrlen += newrec.GetLength();
            }
            ASSERT_EQ(lhdr->m_totrlen, totrlen);
            g_bufman->UnpinPage(lbufid);
        } else {
            // check if the right page looks good
            rbufid = g_bufman->PinPage(rpid, &rbuf);
            PageHeaderData *rph = (PageHeaderData*) rbuf;
            ASSERT_TRUE(rph->IsAllocated())
                << "merge should not free the right page upon unsuccessful "
                   "return";
            rpg = rbuf;
            rhdr = (BTreePageHeaderData*) rpg.GetUserData();
            ASSERT_EQ(rhdr->m_prev_pid, lpid);
            ASSERT_EQ(rhdr->m_next_pid, pid0);
            ASSERT_TRUE(rhdr->IsLeafPage());
            ASSERT_FALSE(rhdr->IsRootPage());

            ASSERT_EQ(rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1,
                      rpg_nslots);
            VarlenDataPage rpg_old(rpg_copy);
            FieldOffset totrlen_right = 0;
            for (SlotId sid = rpg.GetMinSlotId(); sid <= rpg.GetMaxSlotId();
                    ++sid) {
                Record newrec = rpg.GetRecord(sid);
                Record oldrec = rpg_old.GetRecord(sid + n_to_truncate_on_right);
                ASSERT_EQ(newrec.GetLength(), oldrec.GetLength());

                auto oldrechdr = (BTreeLeafRecordHeaderData*) oldrec.GetData();
                auto newrechdr = (BTreeLeafRecordHeaderData*) newrec.GetData();
                ASSERT_EQ(newrechdr->m_recid, oldrechdr->m_recid) << sid;

                int64_t oldkey = read_f1_payload(oldrechdr->GetPayload());
                int64_t newkey = read_f1_payload(newrechdr->GetPayload());
                ASSERT_EQ(newkey, oldkey);

                totrlen_right += newrec.GetLength();
            }
            ASSERT_EQ(rhdr->m_totrlen, totrlen_right);
            g_bufman->UnpinPage(rbufid);

            // check if the pid 0 page has the correct left sibling pointer
            bufid0 = g_bufman->PinPage(pid0, &buf0);
            pg0 = buf0;
            hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
            ASSERT_EQ(hdr0->m_prev_pid, rpid);
            ASSERT_EQ(hdr0->m_next_pid, INVALID_PID);
            ASSERT_FALSE(hdr0->IsRootPage());
            ASSERT_TRUE(hdr0->IsLeafPage());
            g_bufman->UnpinPage(bufid0);

            // check if the parent page still has two internal records
            pbufid = g_bufman->PinPage(ppid, &pbuf);
            ppg = pbuf;
            // should just have one internal record left on the parent page
            ASSERT_EQ(ppg.GetMaxSlotId(), 2);
            ASSERT_EQ(ppg.GetRecord(1).GetLength(), BTreeInternalRecordHeaderSize);
            auto prechdr1 =
                (BTreeInternalRecordHeaderData*) ppg.GetRecordBuffer(1, nullptr);
            ASSERT_EQ(prechdr1->m_child_pid, lpid);

            ASSERT_EQ(ppg.GetRecord(2).GetLength(), BTreeInternalRecordHeaderSize + 8);
            auto prechdr2 =
                (BTreeInternalRecordHeaderData*) ppg.GetRecordBuffer(2, nullptr);
            ASSERT_EQ(prechdr2->m_child_pid, rpid);
            ASSERT_EQ(prechdr2->m_heap_recid, ppg_rec2_recid);
            int64_t prec2_key = read_f1_payload(prechdr2->GetPayload());
            ASSERT_EQ(prec2_key, 203);

            auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
            ASSERT_EQ(phdr->m_totrlen, BTreeInternalRecordHeaderSize * 2 + 8);
            ASSERT_FALSE(phdr->IsRootPage());
            ASSERT_FALSE(phdr->IsLeafPage());
            g_bufman->UnpinPage(pbufid);

            // check if the left page looks good
            lbufid = g_bufman->PinPage(lpid, &lbuf);
            lpg = lbuf;
            lhdr = (BTreePageHeaderData*) lpg.GetUserData();
            ASSERT_EQ(lhdr->m_prev_pid, INVALID_PID);
            ASSERT_EQ(lhdr->m_next_pid, rpid);
            ASSERT_TRUE(lhdr->IsLeafPage());
            ASSERT_FALSE(lhdr->IsRootPage());

            ASSERT_EQ(lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1,
                      lpg_nslots);
            VarlenDataPage lpg_old(lpg_copy);
            FieldOffset totrlen_left = 0;
            for (SlotId sid = lpg.GetMinSlotId(); sid <= lpg.GetMaxSlotId();
                    ++sid) {
                Record oldrec = lpg_old.GetRecord(sid);
                Record newrec = lpg.GetRecord(sid);
                ASSERT_EQ(newrec.GetLength(), oldrec.GetLength()) << sid;

                auto oldrechdr = (BTreeLeafRecordHeaderData*) oldrec.GetData();
                auto newrechdr = (BTreeLeafRecordHeaderData*) newrec.GetData();
                ASSERT_EQ(newrechdr->m_recid, oldrechdr->m_recid) << sid;

                int64_t oldkey = read_f1_payload(oldrechdr->GetPayload());
                int64_t newkey = read_f1_payload(newrechdr->GetPayload());
                ASSERT_EQ(newkey, oldkey);

                totrlen_left += newrec.GetLength();
            }
            ASSERT_EQ(lhdr->m_totrlen, totrlen_left);
            g_bufman->UnpinPage(lbufid);
        }
    }

    TDB_TEST_END
}

TEST_F(BasicTestBTreeMergePages, TestMergeLeafPageWithUniformReclenLargeKey) {
    TDB_TEST_BEGIN

    // the leaf page in m_idx_2 will fit 10 leaf tuples but not 11
    ASSERT_EQ(BTreeComputePageUsage(10, 10 * 392), 4000);
    ASSERT_GT(BTreeComputePageUsage(11, 11 * 392), 4096);

    ScopedBufferId bufid0 =
        CreateNewBTreePage(m_idx_2, false, true, INVALID_PID, INVALID_PID);
    PageNumber pid0 = g_bufman->GetPageNumber(bufid0);
    // we can unpin page 0 for now
    g_bufman->UnpinPage(bufid0);

    // creates a parent page
    ScopedBufferId pbufid =
        CreateNewBTreePage(m_idx_2, false, false, INVALID_PID, INVALID_PID);
    PageNumber ppid = g_bufman->GetPageNumber(pbufid);
    char *pbuf = g_bufman->GetBuffer(pbufid);
    VarlenDataPage ppg(pbuf);
    // inserts two dummy records into the parent page, we will fill their
    // values a bit later
    //
    // We intentionally do not insert a third record here pointing to pid0,
    // because it may be on a right sibling of the parent page instead. The
    // MergePages() implementation should not use the slot id to find the right
    // sibling of the right page. Instead, it should use the m_next_pid in the
    // header.
    BTreeInternalRecordHeaderData ppg_rec1_data;
    Record ppg_rec1((char*) &ppg_rec1_data, BTreeInternalRecordHeaderSize);
    ASSERT_NO_ERROR(ppg.InsertRecordAt(ppg.GetMaxSlotId() + 1, ppg_rec1));
    ASSERT_NE(ppg_rec1.GetRecordId().sid, INVALID_SID);
    IndexKey *ppg_rec2_key = get_key_f2(11 ^ m_reorder_mask);
    RecordId ppg_rec2_recid{
        (PageNumber)(100 + 11), (SlotId)((11 ^ m_reorder_mask) + 1)};
    maxaligned_char_buf ppg_rec2_buf_tmp;
    maxaligned_char_buf ppg_rec2_buf;
    CreateLeafRecord(m_idx_2, ppg_rec2_key, ppg_rec2_recid, ppg_rec2_buf_tmp);
    CreateInternalRecord(m_idx_2, Record(ppg_rec2_buf_tmp),
                         /* child_pid = */INVALID_PID,
                         true, ppg_rec2_buf);
    Record ppg_rec2(ppg_rec2_buf);
    ASSERT_NO_ERROR(ppg.InsertRecordAt(ppg.GetMaxSlotId() + 1, ppg_rec2));
    ASSERT_NE(ppg_rec2.GetRecordId().sid, INVALID_SID);

    auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
    phdr->m_totrlen = ppg_rec1.GetLength() + ppg_rec2.GetLength();

    // make a copy of the parent page for later overwrites
    alignas(CACHELINE_SIZE) char ppg_copy[PAGE_SIZE];
    memcpy(ppg_copy, pbuf, PAGE_SIZE);

    g_bufman->MarkDirty(pbufid);
    g_bufman->UnpinPage(pbufid);

    // creates two new leaf pages followed by pid0
    ScopedBufferId lbufid =
        CreateNewBTreePage(m_idx_2, false, true, INVALID_PID, INVALID_PID);
    PageNumber lpid = g_bufman->GetPageNumber(lbufid);
    char *lbuf = g_bufman->GetBuffer(lbufid);
    VarlenDataPage lpg(lbuf);
    auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();

    // inserts 1 to 10 into the left page, inserting an additional record will
    // fail
    ASSERT_EQ(lhdr->m_totrlen, 0);
    maxaligned_char_buf recbuf;
    for (int64_t i = 1; i <= 11; ++i) {
        IndexKey *key = get_key_f2(i ^ m_reorder_mask);
        RecordId recid{(PageNumber)(100 + i), (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_2, key, recid, recbuf);
        Record rec(recbuf);
        ASSERT_EQ(recbuf.size(), 392);
        ASSERT_NO_ERROR(lpg.InsertRecordAt(lpg.GetMaxSlotId() + 1, rec));
        ASSERT_EQ(rec.GetRecordId().sid != INVALID_SID, i < 11)
            << "the " << i << "th insertion on the leaf page is expected to "
            << ((i < 11) ? "succeed" : "fail");
        if (i < 11) {
            lhdr->m_totrlen += rec.GetLength();
        }
    }

    // make a copy of the original left page so we can its non fm header
    // portion with the loaded data
    alignas(CACHELINE_SIZE) char lpg_copy[PAGE_SIZE];
    memcpy(lpg_copy, lbuf, PAGE_SIZE);

    // drop the pin for now
    g_bufman->MarkDirty(lbufid);
    g_bufman->UnpinPage(lbufid);

    // Instead of creating a new right page here, we directly initializes
    // in our local buffer since the right page will be deleted in every
    // successful merge test. We can use this to overwrite the non fm header
    // portion of the page.
    alignas(CACHELINE_SIZE) char rpg_copy[PAGE_SIZE];
    std::memset(rpg_copy, 0, PAGE_SIZE);
    BTreePageHeaderData::Initialize(rpg_copy,
                                    BTREE_PAGE_ISLEAF,
                                    /*prev_pid =*/lpid,
                                    /*next_pid =*/pid0);
    VarlenDataPage rpg(rpg_copy);
    auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
    // insert keys 11 - 20, 21 should fail
    for (int64_t i = 11; i <= 21; ++i) {
        IndexKey *key = get_key_f2(i ^ m_reorder_mask);
        RecordId recid{(PageNumber)(100 + i), (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_2, key, recid, recbuf);
        Record rec(recbuf);
        ASSERT_EQ(recbuf.size(), 392);
        ASSERT_NO_ERROR(rpg.InsertRecordAt(rpg.GetMaxSlotId() + 1, rec));
        ASSERT_EQ(rec.GetRecordId().sid != INVALID_SID, i < 21)
            << "the " << i << "th insertion on the leaf page is expected to "
            << ((i < 21) ? "succeed" : "fail");
        if (i < 21) {
            rhdr->m_totrlen += rec.GetLength();
        }
    }

    for (SlotId lpg_nslots: {1, 2, 4, 5, 6, 7, 9, 10})
    for (SlotId rpg_nslots: {11 - lpg_nslots, 10 - lpg_nslots}) {
        // skip the test case where there's no tuple on the leaf page, which
        // can't happen in a valid B-tree
        if (rpg_nslots == 0)
            continue;

        // prepare the pages

        // prepare the right page first
        ScopedBufferId rbufid =
            CreateNewBTreePage(m_idx_2, false, true, INVALID_PID, INVALID_PID);
        PageNumber rpid = g_bufman->GetPageNumber(rbufid);
        char *rbuf = g_bufman->GetBuffer(rbufid);
        // MUST NOT copy the page header data portion as we're allocating
        // and deallocating pages across the loops!
        memcpy(rbuf + sizeof(PageHeaderData), rpg_copy + sizeof(PageHeaderData),
               (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage rpg(rbuf);
        auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
        ASSERT_LE(rpg_nslots, rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1);
        SlotId n_to_truncate_on_right = rpg.GetMaxSlotId() - rpg.GetMinSlotId()
            + 1 - rpg_nslots;
        for (SlotId sid = rpg.GetMinSlotId();
                    sid < rpg.GetMinSlotId() + n_to_truncate_on_right; ++sid) {
            rhdr->m_totrlen -= rpg.GetRecord(sid).GetLength();
        }
        if (n_to_truncate_on_right > 0) {
            ASSERT_NO_ERROR(
                PurgeFirstNSlotsOnPage(rbuf, n_to_truncate_on_right));
            //ASSERT_NO_ERROR(rpg.ShiftSlots(n_to_truncate_on_right, true));
        }
        rhdr->m_prev_pid = lpid;
        rhdr->m_next_pid = pid0;
        g_bufman->MarkDirty(rbufid);
        g_bufman->UnpinPage(rbufid);

        // prepare the page 0 (right most)
        char *buf0;
        ScopedBufferId bufid0 = g_bufman->PinPage(pid0, &buf0);
        VarlenDataPage pg0(buf0);
        auto hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
        hdr0->m_prev_pid = rpid;
        g_bufman->MarkDirty(bufid0);
        g_bufman->UnpinPage(bufid0);

        // prepare the left page
        char *lbuf;
        ScopedBufferId lbufid = g_bufman->PinPage(lpid, &lbuf);
        // MUST NOT copy the page header data portion as we're allocating
        // and deallocating pages across the loops!
        memcpy(lbuf + sizeof(PageHeaderData), lpg_copy + sizeof(PageHeaderData),
               (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage lpg(lbuf);
        ASSERT_LE(lpg_nslots, lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1);
        auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();
        for (SlotId sid = lpg.GetMaxSlotId(); sid > lpg_nslots; --sid) {
            lhdr->m_totrlen -= lpg.GetRecord(sid).GetLength();
            ASSERT_NO_ERROR(lpg.RemoveSlot(sid));
        }
        lhdr->m_next_pid = rpid;
        g_bufman->MarkDirty(lbufid);
        g_bufman->UnpinPage(lbufid);

        // prepare the parent page
        char *pbuf;
        ScopedBufferId pbufid = g_bufman->PinPage(ppid, &pbuf);
        memcpy(pbuf + sizeof(PageHeaderData), ppg_copy + sizeof(PageHeaderData),
              (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage ppg(pbuf);
        ASSERT_EQ(ppg.GetMaxSlotId(), 2);
        auto prechdr1 = (BTreeInternalRecordHeaderData*) ppg.GetRecordBuffer(1, nullptr);
        prechdr1->m_child_pid = lpid;
        auto prechdr2 = (BTreeInternalRecordHeaderData*) ppg.GetRecordBuffer(2, nullptr);
        prechdr2->m_child_pid = rpid;
        g_bufman->MarkDirty(pbufid);
        g_bufman->UnpinPage(pbufid);

        // now force a buffer flush
        ASSERT_NO_ERROR(ForceFlushBuffer());

        // pin the three pages back in the buffer pool
        BufferId lbufid_ = g_bufman->PinPage(lpid, &lbuf);
        BufferId rbufid_ = g_bufman->PinPage(rpid, &rbuf);
        pbufid = g_bufman->PinPage(ppid, &pbuf);

        // and try to merge the left and right page
        bool merge_succeeded;
        ASSERT_NO_ERROR(merge_succeeded =
            MergePages(m_idx_2, lbufid_, rbufid_, pbufid, 1));

        // merge will only succeed if there're no more than 10 records in total
        bool expected_to_succeed = (lpg_nslots + rpg_nslots) <= 10;
        ASSERT_EQ(merge_succeeded, expected_to_succeed);

        // unpin all these pages
        ASSERT_NO_ERROR(g_bufman->UnpinPage(pbufid));
        if (!merge_succeeded) {
            g_bufman->UnpinPage(lbufid_);
            g_bufman->UnpinPage(rbufid_);
        }

        // If the merge is successful, we flush the buffer pool in order to
        // check if the dirty bits are set correctly. Otherwise, we should not
        // flush the buffer pool, in case the pages are modified but the
        // dirty bits were not set.
        if (merge_succeeded) {
            ASSERT_NO_ERROR(ForceFlushBuffer());
        }

        if (merge_succeeded) {
            // check if the right page is freed
            rbufid = g_bufman->PinPage(rpid, &rbuf);
            PageHeaderData *rph = (PageHeaderData*) rbuf;
            ASSERT_FALSE(rph->IsAllocated())
                << "merge should free the right page upon succesful return";
            g_bufman->UnpinPage(rbufid);

            // check if the pid 0 page has the correct left sibling pointer
            bufid0 = g_bufman->PinPage(pid0, &buf0);
            pg0 = buf0;
            hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
            ASSERT_EQ(hdr0->m_prev_pid, lpid);
            ASSERT_EQ(hdr0->m_next_pid, INVALID_PID);
            ASSERT_FALSE(hdr0->IsRootPage());
            ASSERT_TRUE(hdr0->IsLeafPage());
            g_bufman->UnpinPage(bufid0);

            // check if the internal record pointing to the right page is
            // deleted from the parent page
            pbufid = g_bufman->PinPage(ppid, &pbuf);
            ppg = pbuf;
            // should just have one internal record left on the parent page
            ASSERT_EQ(ppg.GetMaxSlotId(), 1);
            ASSERT_EQ(ppg.GetRecord(1).GetLength(), BTreeInternalRecordHeaderSize);
            auto prechdr1 =
                (BTreeInternalRecordHeaderData*) ppg.GetRecordBuffer(1, nullptr);
            ASSERT_EQ(prechdr1->m_child_pid, lpid);
            auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
            ASSERT_EQ(phdr->m_totrlen, BTreeInternalRecordHeaderSize);
            ASSERT_FALSE(phdr->IsRootPage());
            ASSERT_FALSE(phdr->IsLeafPage());
            g_bufman->UnpinPage(pbufid);

            // check if the left page looks good
            lbufid = g_bufman->PinPage(lpid, &lbuf);
            lpg = lbuf;
            lhdr = (BTreePageHeaderData*) lpg.GetUserData();
            ASSERT_EQ(lhdr->m_prev_pid, INVALID_PID);
            ASSERT_EQ(lhdr->m_next_pid, pid0);
            ASSERT_TRUE(lhdr->IsLeafPage());
            ASSERT_FALSE(lhdr->IsRootPage());

            ASSERT_EQ(lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1,
                      lpg_nslots + rpg_nslots);
            VarlenDataPage lpg_old(lpg_copy);
            VarlenDataPage rpg_old(rpg_copy);
            FieldOffset totrlen = 0;
            for (SlotId sid = lpg.GetMinSlotId(); sid <= lpg.GetMaxSlotId();
                    ++sid) {
                Record oldrec;
                if (sid <= lpg_nslots) {
                    oldrec = lpg_old.GetRecord(sid);
                } else {
                    oldrec = rpg_old.GetRecord(sid - lpg_nslots
                                               + n_to_truncate_on_right);
                }
                Record newrec = lpg.GetRecord(sid);
                ASSERT_EQ(newrec.GetLength(), oldrec.GetLength()) << sid;

                auto oldrechdr = (BTreeLeafRecordHeaderData*) oldrec.GetData();
                auto newrechdr = (BTreeLeafRecordHeaderData*) newrec.GetData();
                ASSERT_EQ(newrechdr->m_recid, oldrechdr->m_recid) << sid;

                absl::string_view oldkey =
                    read_f2_payload(oldrechdr->GetPayload());
                absl::string_view newkey =
                    read_f2_payload(newrechdr->GetPayload());
                ASSERT_EQ(newkey, oldkey);

                totrlen += newrec.GetLength();
            }
            ASSERT_EQ(lhdr->m_totrlen, totrlen);
            g_bufman->UnpinPage(lbufid);
        } else {
            // check if the right page looks good
            rbufid = g_bufman->PinPage(rpid, &rbuf);
            PageHeaderData *rph = (PageHeaderData*) rbuf;
            ASSERT_TRUE(rph->IsAllocated())
                << "merge should not free the right page upon unsuccessful "
                   "return";
            rpg = rbuf;
            rhdr = (BTreePageHeaderData*) rpg.GetUserData();
            ASSERT_EQ(rhdr->m_prev_pid, lpid);
            ASSERT_EQ(rhdr->m_next_pid, pid0);
            ASSERT_TRUE(rhdr->IsLeafPage());
            ASSERT_FALSE(rhdr->IsRootPage());

            ASSERT_EQ(rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1,
                      rpg_nslots);
            VarlenDataPage rpg_old(rpg_copy);
            FieldOffset totrlen_right = 0;
            for (SlotId sid = rpg.GetMinSlotId(); sid <= rpg.GetMaxSlotId();
                    ++sid) {
                Record newrec = rpg.GetRecord(sid);
                Record oldrec = rpg_old.GetRecord(sid + n_to_truncate_on_right);
                ASSERT_EQ(newrec.GetLength(), oldrec.GetLength());

                auto oldrechdr = (BTreeLeafRecordHeaderData*) oldrec.GetData();
                auto newrechdr = (BTreeLeafRecordHeaderData*) newrec.GetData();
                ASSERT_EQ(newrechdr->m_recid, oldrechdr->m_recid) << sid;

                absl::string_view oldkey =
                    read_f2_payload(oldrechdr->GetPayload());
                absl::string_view newkey =
                    read_f2_payload(newrechdr->GetPayload());
                ASSERT_EQ(newkey, oldkey);

                totrlen_right += newrec.GetLength();
            }
            ASSERT_EQ(rhdr->m_totrlen, totrlen_right);
            g_bufman->UnpinPage(rbufid);

            // check if the pid 0 page has the correct left sibling pointer
            bufid0 = g_bufman->PinPage(pid0, &buf0);
            pg0 = buf0;
            hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
            ASSERT_EQ(hdr0->m_prev_pid, rpid);
            ASSERT_EQ(hdr0->m_next_pid, INVALID_PID);
            ASSERT_FALSE(hdr0->IsRootPage());
            ASSERT_TRUE(hdr0->IsLeafPage());
            g_bufman->UnpinPage(bufid0);

            // check if the parent page still has two internal records
            pbufid = g_bufman->PinPage(ppid, &pbuf);
            ppg = pbuf;
            // should just have one internal record left on the parent page
            ASSERT_EQ(ppg.GetMaxSlotId(), 2);
            ASSERT_EQ(ppg.GetRecord(1).GetLength(),
                      BTreeInternalRecordHeaderSize);
            auto prechdr1 = (BTreeInternalRecordHeaderData*)
                ppg.GetRecordBuffer(1, nullptr);
            ASSERT_EQ(prechdr1->m_child_pid, lpid);

            ASSERT_EQ(ppg.GetRecord(2).GetLength(),
                      BTreeInternalRecordHeaderSize + 384);
            auto prechdr2 = (BTreeInternalRecordHeaderData*)
                ppg.GetRecordBuffer(2, nullptr);
            ASSERT_EQ(prechdr2->m_child_pid, rpid);
            ASSERT_EQ(prechdr2->m_heap_recid, ppg_rec2_recid);
            absl::string_view prec2_key =
                read_f2_payload(prechdr2->GetPayload());
            absl::string_view expected_prec2_key =
                get_skey(11 ^ m_reorder_mask);
            ASSERT_EQ(prec2_key, expected_prec2_key);

            auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
            ASSERT_EQ(phdr->m_totrlen, BTreeInternalRecordHeaderSize * 2 + 384);
            ASSERT_FALSE(phdr->IsRootPage());
            ASSERT_FALSE(phdr->IsLeafPage());
            g_bufman->UnpinPage(pbufid);

            // check if the left page looks good
            lbufid = g_bufman->PinPage(lpid, &lbuf);
            lpg = lbuf;
            lhdr = (BTreePageHeaderData*) lpg.GetUserData();
            ASSERT_EQ(lhdr->m_prev_pid, INVALID_PID);
            ASSERT_EQ(lhdr->m_next_pid, rpid);
            ASSERT_TRUE(lhdr->IsLeafPage());
            ASSERT_FALSE(lhdr->IsRootPage());

            ASSERT_EQ(lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1,
                      lpg_nslots);
            VarlenDataPage lpg_old(lpg_copy);
            FieldOffset totrlen_left = 0;
            for (SlotId sid = lpg.GetMinSlotId(); sid <= lpg.GetMaxSlotId();
                    ++sid) {
                Record oldrec = lpg_old.GetRecord(sid);
                Record newrec = lpg.GetRecord(sid);
                ASSERT_EQ(newrec.GetLength(), oldrec.GetLength()) << sid;

                auto oldrechdr = (BTreeLeafRecordHeaderData*) oldrec.GetData();
                auto newrechdr = (BTreeLeafRecordHeaderData*) newrec.GetData();
                ASSERT_EQ(newrechdr->m_recid, oldrechdr->m_recid) << sid;

                absl::string_view oldkey =
                    read_f2_payload(oldrechdr->GetPayload());
                absl::string_view newkey =
                    read_f2_payload(newrechdr->GetPayload());
                ASSERT_EQ(newkey, oldkey);

                totrlen_left += newrec.GetLength();
            }
            ASSERT_EQ(lhdr->m_totrlen, totrlen_left);
            g_bufman->UnpinPage(lbufid);
        }
    }

    TDB_TEST_END
}

TEST_F(BasicTestBTreeMergePages, TestMergeInternalPageWithUniformReclenSmallKey) {
    TDB_TEST_BEGIN

    // the internal page in m_idx_1 will fit 145 internal tuples but not 146
    ASSERT_EQ(BTreeComputePageUsage(145, 144 * 24 + 16), 4092);
    ASSERT_GT(BTreeComputePageUsage(146, 145 * 24 + 16), 4096);

    ScopedBufferId bufid0 =
        CreateNewBTreePage(m_idx_1, false, false, INVALID_PID, INVALID_PID);
    PageNumber pid0 = g_bufman->GetPageNumber(bufid0);
    // we can unpin page 0 for now
    g_bufman->UnpinPage(bufid0);

    // creates a parent page
    ScopedBufferId pbufid =
        CreateNewBTreePage(m_idx_1, false, false, INVALID_PID, INVALID_PID);
    PageNumber ppid = g_bufman->GetPageNumber(pbufid);
    char *pbuf = g_bufman->GetBuffer(pbufid);
    VarlenDataPage ppg(pbuf);
    // inserts two dummy records into the parent page, we will fill their
    // values a bit later
    //
    // We intentionally do not insert a third record here pointing to pid0,
    // because it may be on a right sibling of the parent page instead. The
    // MergePages() implementation should not use the slot id to find the right
    // sibling of the right page. Instead, it should use the m_next_pid in the
    // header.
    BTreeInternalRecordHeaderData ppg_rec1_data;
    Record ppg_rec1((char*) &ppg_rec1_data, BTreeInternalRecordHeaderSize);
    ASSERT_NO_ERROR(ppg.InsertRecordAt(ppg.GetMaxSlotId() + 1, ppg_rec1));
    ASSERT_NE(ppg_rec1.GetRecordId().sid, INVALID_SID);
    IndexKey *ppg_rec2_key = get_key_f1(146 ^ m_reorder_mask);
    RecordId ppg_rec2_recid{
        (PageNumber)(100 + 146), (SlotId)((146 ^ m_reorder_mask) + 1)};
    maxaligned_char_buf ppg_rec2_buf_tmp;
    maxaligned_char_buf ppg_rec2_buf;
    CreateLeafRecord(m_idx_1, ppg_rec2_key, ppg_rec2_recid, ppg_rec2_buf_tmp);
    CreateInternalRecord(m_idx_1, Record(ppg_rec2_buf_tmp),
                         /* child_pid = */INVALID_PID,
                         true, ppg_rec2_buf);
    Record ppg_rec2(ppg_rec2_buf);
    ASSERT_NO_ERROR(ppg.InsertRecordAt(ppg.GetMaxSlotId() + 1, ppg_rec2));
    ASSERT_NE(ppg_rec2.GetRecordId().sid, INVALID_SID);

    auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
    phdr->m_totrlen = ppg_rec1.GetLength() + ppg_rec2.GetLength();

    // make a copy of the parent page for later overwrites
    alignas(CACHELINE_SIZE) char ppg_copy[PAGE_SIZE];
    memcpy(ppg_copy, pbuf, PAGE_SIZE);

    g_bufman->MarkDirty(pbufid);
    g_bufman->UnpinPage(pbufid);

    // creates two new leaf pages followed by pid0
    ScopedBufferId lbufid =
        CreateNewBTreePage(m_idx_1, false, false, INVALID_PID, INVALID_PID);
    PageNumber lpid = g_bufman->GetPageNumber(lbufid);
    char *lbuf = g_bufman->GetBuffer(lbufid);
    VarlenDataPage lpg(lbuf);
    auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();

    // inserts 1 to 145 into the left page, inserting an additional record will
    // fail
    ASSERT_EQ(lhdr->m_totrlen, 0);
    maxaligned_char_buf recbuf;
    maxaligned_char_buf recbuf_tmp;
    for (int64_t i = 1; i <= 146 ; ++i) {
        IndexKey *key = get_key_f1(i ^ m_reorder_mask);
        RecordId recid{(PageNumber)(100 + i), (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_1, key, recid, recbuf_tmp);
        CreateInternalRecord(m_idx_1, Record(recbuf_tmp),
                             /*child_pid =*/(PageNumber)(10000 + i),
                             true, recbuf);
        ASSERT_EQ(recbuf.size(), 24);
        Record rec(recbuf);
        if (i == 1) {
            rec.GetLength() = BTreeInternalRecordHeaderSize;
            ASSERT(rec.GetLength(), 16);
        }
        ASSERT_NO_ERROR(lpg.InsertRecordAt(lpg.GetMaxSlotId() + 1, rec));
        ASSERT_EQ(rec.GetRecordId().sid != INVALID_SID, i < 146)
            << "the " << i << "th insertion on the leaf page is expected to "
            << ((i < 146) ? "succeed" : "fail");
        if (i < 146) {
            lhdr->m_totrlen += rec.GetLength();
        }
    }

    // make a copy of the original left page so we can its non fm header
    // portion with the loaded data
    alignas(CACHELINE_SIZE) char lpg_copy[PAGE_SIZE];
    memcpy(lpg_copy, lbuf, PAGE_SIZE);

    // drop the pin for now
    g_bufman->MarkDirty(lbufid);
    g_bufman->UnpinPage(lbufid);

    // Instead of creating a new right page here, we directly initializes
    // in our local buffer since the right page will be deleted in every
    // successful merge test. We can use this to overwrite the non fm header
    // portion of the page.
    alignas(CACHELINE_SIZE) char rpg_copy[PAGE_SIZE];
    std::memset(rpg_copy, 0, PAGE_SIZE);
    BTreePageHeaderData::Initialize(rpg_copy,
                                    0,
                                    /*prev_pid =*/lpid,
                                    /*next_pid =*/pid0);
    VarlenDataPage rpg(rpg_copy);
    auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
    // insert keys 146 - 290, 291 should fail
    for (int64_t i = 146; i <= 291; ++i) {
        IndexKey *key = get_key_f1(i ^ m_reorder_mask);
        RecordId recid{(PageNumber)(100 + i), (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_1, key, recid, recbuf_tmp);
        CreateInternalRecord(m_idx_2, Record(recbuf_tmp),
                             /*child_pid =*/(PageNumber)(10000 + i),
                             true, recbuf);
        ASSERT_EQ(recbuf.size(), 24);
        Record rec(recbuf);
        if (i == 146) {
            rec.GetLength() = BTreeInternalRecordHeaderSize;
            ASSERT(rec.GetLength(), 16);
        }
        ASSERT_NO_ERROR(rpg.InsertRecordAt(rpg.GetMaxSlotId() + 1, rec));
        ASSERT_EQ(rec.GetRecordId().sid != INVALID_SID, i < 291)
            << "the " << i << "th insertion on the leaf page is expected to "
            << ((i < 291) ? "succeed" : "fail");
        if (i < 291) {
            rhdr->m_totrlen += rec.GetLength();
        }
    }

    for (SlotId lpg_nslots: {1, 2, 40, 72, 73, 74, 75, 110, 144, 145})
    for (SlotId rpg_nslots: {146 - lpg_nslots, 145 - lpg_nslots}) {
        // skip the test case where there's no tuple on the leaf page, which
        // can't happen in a valid B-tree
        if (rpg_nslots == 0)
            continue;

        // prepare the pages

        // prepare the right page first
        ScopedBufferId rbufid =
            CreateNewBTreePage(m_idx_1, false, false, INVALID_PID, INVALID_PID);
        PageNumber rpid = g_bufman->GetPageNumber(rbufid);
        char *rbuf = g_bufman->GetBuffer(rbufid);
        // MUST NOT copy the page header data portion as we're allocating
        // and deallocating pages across the loops!
        memcpy(rbuf + sizeof(PageHeaderData), rpg_copy + sizeof(PageHeaderData),
               (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage rpg(rbuf);
        auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
        ASSERT_LE(rpg_nslots, rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1);
        SlotId n_to_truncate_on_right = rpg.GetMaxSlotId() - rpg.GetMinSlotId()
            + 1 - rpg_nslots;
        for (SlotId sid = rpg.GetMinSlotId();
                    sid < rpg.GetMinSlotId() + n_to_truncate_on_right; ++sid) {
            rhdr->m_totrlen -= rpg.GetRecord(sid).GetLength();
        }
        if (n_to_truncate_on_right > 0) {
            ASSERT_NO_ERROR(
                PurgeFirstNSlotsOnPage(rbuf, n_to_truncate_on_right));
            //ASSERT_NO_ERROR(rpg.ShiftSlots(n_to_truncate_on_right, true));
        }
        // truncate the (new) first record on the right page
        Record first_rec_on_right = rpg.GetRecord(1);
        rhdr->m_totrlen -= first_rec_on_right.GetLength();
        rhdr->m_totrlen += BTreeInternalRecordHeaderSize;
        first_rec_on_right.GetLength() = BTreeInternalRecordHeaderSize;
        ASSERT_TRUE(rpg.UpdateRecord(1, first_rec_on_right));
        ASSERT_NE(first_rec_on_right.GetRecordId().sid, INVALID_SID);
        rhdr->m_prev_pid = lpid;
        rhdr->m_next_pid = pid0;
        g_bufman->MarkDirty(rbufid);
        g_bufman->UnpinPage(rbufid);

        // prepare the page 0 (right most)
        char *buf0;
        ScopedBufferId bufid0 = g_bufman->PinPage(pid0, &buf0);
        VarlenDataPage pg0(buf0);
        auto hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
        hdr0->m_prev_pid = rpid;
        g_bufman->MarkDirty(bufid0);
        g_bufman->UnpinPage(bufid0);

        // prepare the left page
        char *lbuf;
        ScopedBufferId lbufid = g_bufman->PinPage(lpid, &lbuf);
        // MUST NOT copy the page header data portion as we're allocating
        // and deallocating pages across the loops!
        memcpy(lbuf + sizeof(PageHeaderData), lpg_copy + sizeof(PageHeaderData),
               (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage lpg(lbuf);
        ASSERT_LE(lpg_nslots, lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1);
        auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();
        for (SlotId sid = lpg.GetMaxSlotId(); sid > lpg_nslots; --sid) {
            lhdr->m_totrlen -= lpg.GetRecord(sid).GetLength();
            ASSERT_NO_ERROR(lpg.RemoveSlot(sid));
        }
        lhdr->m_next_pid = rpid;
        g_bufman->MarkDirty(lbufid);
        g_bufman->UnpinPage(lbufid);

        // prepare the parent page
        char *pbuf;
        ScopedBufferId pbufid = g_bufman->PinPage(ppid, &pbuf);
        memcpy(pbuf + sizeof(PageHeaderData), ppg_copy + sizeof(PageHeaderData),
              (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage ppg(pbuf);
        ASSERT_EQ(ppg.GetMaxSlotId(), 2);
        auto prechdr1 = (BTreeInternalRecordHeaderData*) ppg.GetRecordBuffer(1, nullptr);
        prechdr1->m_child_pid = lpid;
        auto prechdr2 = (BTreeInternalRecordHeaderData*) ppg.GetRecordBuffer(2, nullptr);
        prechdr2->m_child_pid = rpid;
        g_bufman->MarkDirty(pbufid);
        g_bufman->UnpinPage(pbufid);

        // now force a buffer flush
        ASSERT_NO_ERROR(ForceFlushBuffer());

        // pin the three pages back in the buffer pool
        BufferId lbufid_ = g_bufman->PinPage(lpid, &lbuf);
        BufferId rbufid_ = g_bufman->PinPage(rpid, &rbuf);
        pbufid = g_bufman->PinPage(ppid, &pbuf);

        // and try to merge the left and right page
        bool merge_succeeded;
        ASSERT_NO_ERROR(merge_succeeded =
            MergePages(m_idx_1, lbufid_, rbufid_, pbufid, 1));

        // merge will only succeed if there're no more than 145 records in total
        bool expected_to_succeed = (lpg_nslots + rpg_nslots) <= 145;
        ASSERT_EQ(merge_succeeded, expected_to_succeed);

        // unpin all these pages
        ASSERT_NO_ERROR(g_bufman->UnpinPage(pbufid));
        if (!merge_succeeded) {
            g_bufman->UnpinPage(lbufid_);
            g_bufman->UnpinPage(rbufid_);
        }

        // If the merge is successful, we flush the buffer pool in order to
        // check if the dirty bits are set correctly. Otherwise, we should not
        // flush the buffer pool, in case the pages are modified but the
        // dirty bits were not set.
        if (merge_succeeded) {
            ASSERT_NO_ERROR(ForceFlushBuffer());
        }

        if (merge_succeeded) {
            // check if the right page is freed
            rbufid = g_bufman->PinPage(rpid, &rbuf);
            PageHeaderData *rph = (PageHeaderData*) rbuf;
            ASSERT_FALSE(rph->IsAllocated())
                << "merge should free the right page upon succesful return";
            g_bufman->UnpinPage(rbufid);

            // check if the pid 0 page has the correct left sibling pointer
            bufid0 = g_bufman->PinPage(pid0, &buf0);
            pg0 = buf0;
            hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
            ASSERT_EQ(hdr0->m_prev_pid, lpid);
            ASSERT_EQ(hdr0->m_next_pid, INVALID_PID);
            ASSERT_FALSE(hdr0->IsRootPage());
            ASSERT_FALSE(hdr0->IsLeafPage());
            g_bufman->UnpinPage(bufid0);

            // check if the internal record pointing to the right page is
            // deleted from the parent page
            pbufid = g_bufman->PinPage(ppid, &pbuf);
            ppg = pbuf;
            // should just have one internal record left on the parent page
            ASSERT_EQ(ppg.GetMaxSlotId(), 1);
            ASSERT_EQ(ppg.GetRecord(1).GetLength(), BTreeInternalRecordHeaderSize);
            auto prechdr1 =
                (BTreeInternalRecordHeaderData*) ppg.GetRecordBuffer(1, nullptr);
            ASSERT_EQ(prechdr1->m_child_pid, lpid);
            auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
            ASSERT_EQ(phdr->m_totrlen, BTreeInternalRecordHeaderSize);
            ASSERT_FALSE(phdr->IsRootPage());
            ASSERT_FALSE(phdr->IsLeafPage());
            g_bufman->UnpinPage(pbufid);

            // check if the left page looks good
            lbufid = g_bufman->PinPage(lpid, &lbuf);
            lpg = lbuf;
            lhdr = (BTreePageHeaderData*) lpg.GetUserData();
            ASSERT_EQ(lhdr->m_prev_pid, INVALID_PID);
            ASSERT_EQ(lhdr->m_next_pid, pid0);
            ASSERT_FALSE(lhdr->IsLeafPage());
            ASSERT_FALSE(lhdr->IsRootPage());

            ASSERT_EQ(lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1,
                      lpg_nslots + rpg_nslots);
            VarlenDataPage lpg_old(lpg_copy);
            VarlenDataPage rpg_old(rpg_copy);
            VarlenDataPage ppg_old(ppg_copy);
            FieldOffset totrlen = 0;
            for (SlotId sid = lpg.GetMinSlotId(); sid <= lpg.GetMaxSlotId();
                    ++sid) {
                Record oldrec;
                if (sid <= lpg_nslots) {
                    oldrec = lpg_old.GetRecord(sid);
                } else if (sid == lpg_nslots + 1) {
                    oldrec = ppg_old.GetRecord(2);
                    auto oldrechdr =
                        (BTreeInternalRecordHeaderData*) oldrec.GetData();
                    oldrechdr->m_child_pid = (PageNumber)
                        (10000 + 145 + n_to_truncate_on_right + 1);
                    oldrechdr->m_heap_recid = RecordId{
                        (PageNumber)(100 + 146),
                        (SlotId)((146 ^ m_reorder_mask) + 1)
                    };
                } else {
                    oldrec = rpg_old.GetRecord(sid - lpg_nslots
                                               + n_to_truncate_on_right);
                }
                Record newrec = lpg.GetRecord(sid);
                ASSERT_EQ(newrec.GetLength(), oldrec.GetLength()) << sid;

                auto oldrechdr =
                    (BTreeInternalRecordHeaderData*) oldrec.GetData();
                auto newrechdr =
                    (BTreeInternalRecordHeaderData*) newrec.GetData();
                ASSERT_EQ(newrechdr->m_child_pid, oldrechdr->m_child_pid)
                    << sid;

                if (sid != lpg.GetMinSlotId()) {
                    ASSERT_EQ(oldrechdr->m_heap_recid, newrechdr->m_heap_recid)
                        << sid;

                    int64_t oldkey = read_f1_payload(oldrechdr->GetPayload());
                    int64_t newkey = read_f1_payload(newrechdr->GetPayload());
                    ASSERT_EQ(newkey, oldkey) << sid;
                }

                totrlen += newrec.GetLength();
            }
            ASSERT_EQ(lhdr->m_totrlen, totrlen);
            g_bufman->UnpinPage(lbufid);
        } else {
            // check if the right page looks good
            rbufid = g_bufman->PinPage(rpid, &rbuf);
            PageHeaderData *rph = (PageHeaderData*) rbuf;
            ASSERT_TRUE(rph->IsAllocated())
                << "merge should not free the right page upon unsuccessful "
                   "return";
            rpg = rbuf;
            rhdr = (BTreePageHeaderData*) rpg.GetUserData();
            ASSERT_EQ(rhdr->m_prev_pid, lpid);
            ASSERT_EQ(rhdr->m_next_pid, pid0);
            ASSERT_FALSE(rhdr->IsLeafPage());
            ASSERT_FALSE(rhdr->IsRootPage());

            ASSERT_EQ(rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1,
                      rpg_nslots);
            VarlenDataPage rpg_old(rpg_copy);
            FieldOffset totrlen_right = 0;
            for (SlotId sid = rpg.GetMinSlotId(); sid <= rpg.GetMaxSlotId();
                    ++sid) {
                Record newrec = rpg.GetRecord(sid);
                Record oldrec = rpg_old.GetRecord(sid + n_to_truncate_on_right);

                auto oldrechdr =
                    (BTreeInternalRecordHeaderData*) oldrec.GetData();
                auto newrechdr =
                    (BTreeInternalRecordHeaderData*) newrec.GetData();
                ASSERT_EQ(newrechdr->m_child_pid, oldrechdr->m_child_pid)
                    << sid;

                if (sid != rpg.GetMinSlotId()) {
                    ASSERT_EQ(newrec.GetLength(), oldrec.GetLength());
                    ASSERT_EQ(oldrechdr->m_heap_recid, newrechdr->m_heap_recid)
                        << sid;

                    int64_t oldkey = read_f1_payload(oldrechdr->GetPayload());
                    int64_t newkey = read_f1_payload(newrechdr->GetPayload());
                    ASSERT_EQ(newkey, oldkey) << sid;
                } else {
                    ASSERT_EQ(newrec.GetLength(),
                              BTreeInternalRecordHeaderSize);
                }

                totrlen_right += newrec.GetLength();
            }
            ASSERT_EQ(rhdr->m_totrlen, totrlen_right);
            g_bufman->UnpinPage(rbufid);

            // check if the pid 0 page has the correct left sibling pointer
            bufid0 = g_bufman->PinPage(pid0, &buf0);
            pg0 = buf0;
            hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
            ASSERT_EQ(hdr0->m_prev_pid, rpid);
            ASSERT_EQ(hdr0->m_next_pid, INVALID_PID);
            ASSERT_FALSE(hdr0->IsRootPage());
            ASSERT_FALSE(hdr0->IsLeafPage());
            g_bufman->UnpinPage(bufid0);

            // check if the parent page still has two internal records
            pbufid = g_bufman->PinPage(ppid, &pbuf);
            ppg = pbuf;
            // should just have one internal record left on the parent page
            ASSERT_EQ(ppg.GetMaxSlotId(), 2);
            ASSERT_EQ(ppg.GetRecord(1).GetLength(),
                      BTreeInternalRecordHeaderSize);
            auto prechdr1 = (BTreeInternalRecordHeaderData*)
                ppg.GetRecordBuffer(1, nullptr);
            ASSERT_EQ(prechdr1->m_child_pid, lpid);

            ASSERT_EQ(ppg.GetRecord(2).GetLength(),
                      BTreeInternalRecordHeaderSize + 8);
            auto prechdr2 = (BTreeInternalRecordHeaderData*)
                ppg.GetRecordBuffer(2, nullptr);
            ASSERT_EQ(prechdr2->m_child_pid, rpid);
            ASSERT_EQ(prechdr2->m_heap_recid, ppg_rec2_recid);
            int64_t prec2_key = read_f1_payload(prechdr2->GetPayload());
            ASSERT_EQ(prec2_key, 146);

            auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
            ASSERT_EQ(phdr->m_totrlen, BTreeInternalRecordHeaderSize * 2 + 8);
            ASSERT_FALSE(phdr->IsRootPage());
            ASSERT_FALSE(phdr->IsLeafPage());
            g_bufman->UnpinPage(pbufid);

            // check if the left page looks good
            lbufid = g_bufman->PinPage(lpid, &lbuf);
            lpg = lbuf;
            lhdr = (BTreePageHeaderData*) lpg.GetUserData();
            ASSERT_EQ(lhdr->m_prev_pid, INVALID_PID);
            ASSERT_EQ(lhdr->m_next_pid, rpid);
            ASSERT_FALSE(lhdr->IsLeafPage());
            ASSERT_FALSE(lhdr->IsRootPage());

            ASSERT_EQ(lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1,
                      lpg_nslots);
            VarlenDataPage lpg_old(lpg_copy);
            FieldOffset totrlen_left = 0;
            for (SlotId sid = lpg.GetMinSlotId(); sid <= lpg.GetMaxSlotId();
                    ++sid) {
                Record oldrec = lpg_old.GetRecord(sid);
                Record newrec = lpg.GetRecord(sid);
                ASSERT_EQ(newrec.GetLength(), oldrec.GetLength()) << sid;

                auto oldrechdr = (BTreeInternalRecordHeaderData*)
                    oldrec.GetData();
                auto newrechdr = (BTreeInternalRecordHeaderData*)
                    newrec.GetData();
                ASSERT_EQ(newrechdr->m_child_pid, oldrechdr->m_child_pid)
                    << sid;

                if (sid != lpg.GetMinSlotId()) {
                    ASSERT_EQ(oldrechdr->m_heap_recid, newrechdr->m_heap_recid)
                        << sid;

                    int64_t oldkey = read_f1_payload(oldrechdr->GetPayload());
                    int64_t newkey = read_f1_payload(newrechdr->GetPayload());
                    ASSERT_EQ(newkey, oldkey) << sid;
                }

                totrlen_left += newrec.GetLength();
            }
            ASSERT_EQ(lhdr->m_totrlen, totrlen_left);
            g_bufman->UnpinPage(lbufid);
        }
    }

    TDB_TEST_END
}

TEST_F(BasicTestBTreeMergePages, TestMergeInternalPageWithUniformReclenLargeKey) {
    TDB_TEST_BEGIN

    // the internal page in m_idx_2 will fit 10 internal tuples but not 11
    ASSERT_EQ(BTreeComputePageUsage(10, 9 * 400 + 16), 3696);
    ASSERT_GT(BTreeComputePageUsage(11, 10 * 400 + 16), 4096);

    ScopedBufferId bufid0 =
        CreateNewBTreePage(m_idx_2, false, false, INVALID_PID, INVALID_PID);
    PageNumber pid0 = g_bufman->GetPageNumber(bufid0);
    // we can unpin page 0 for now
    g_bufman->UnpinPage(bufid0);

    // creates a parent page
    ScopedBufferId pbufid =
        CreateNewBTreePage(m_idx_2, false, false, INVALID_PID, INVALID_PID);
    PageNumber ppid = g_bufman->GetPageNumber(pbufid);
    char *pbuf = g_bufman->GetBuffer(pbufid);
    VarlenDataPage ppg(pbuf);
    // inserts two dummy records into the parent page, we will fill their
    // values a bit later
    //
    // We intentionally do not insert a third record here pointing to pid0,
    // because it may be on a right sibling of the parent page instead. The
    // MergePages() implementation should not use the slot id to find the right
    // sibling of the right page. Instead, it should use the m_next_pid in the
    // header.
    BTreeInternalRecordHeaderData ppg_rec1_data;
    Record ppg_rec1((char*) &ppg_rec1_data, BTreeInternalRecordHeaderSize);
    ASSERT_NO_ERROR(ppg.InsertRecordAt(ppg.GetMaxSlotId() + 1, ppg_rec1));
    ASSERT_NE(ppg_rec1.GetRecordId().sid, INVALID_SID);
    IndexKey *ppg_rec2_key = get_key_f2(11 ^ m_reorder_mask);
    RecordId ppg_rec2_recid{
        (PageNumber)(100 + 11), (SlotId)((11 ^ m_reorder_mask) + 1)};
    maxaligned_char_buf ppg_rec2_buf_tmp;
    maxaligned_char_buf ppg_rec2_buf;
    CreateLeafRecord(m_idx_2, ppg_rec2_key, ppg_rec2_recid, ppg_rec2_buf_tmp);
    CreateInternalRecord(m_idx_2, Record(ppg_rec2_buf_tmp),
                         /* child_pid = */INVALID_PID,
                         true, ppg_rec2_buf);
    Record ppg_rec2(ppg_rec2_buf);
    ASSERT_NO_ERROR(ppg.InsertRecordAt(ppg.GetMaxSlotId() + 1, ppg_rec2));
    ASSERT_NE(ppg_rec2.GetRecordId().sid, INVALID_SID);

    auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
    phdr->m_totrlen = ppg_rec1.GetLength() + ppg_rec2.GetLength();

    // make a copy of the parent page for later overwrites
    alignas(CACHELINE_SIZE) char ppg_copy[PAGE_SIZE];
    memcpy(ppg_copy, pbuf, PAGE_SIZE);

    g_bufman->MarkDirty(pbufid);
    g_bufman->UnpinPage(pbufid);

    // creates two new leaf pages followed by pid0
    ScopedBufferId lbufid =
        CreateNewBTreePage(m_idx_2, false, false, INVALID_PID, INVALID_PID);
    PageNumber lpid = g_bufman->GetPageNumber(lbufid);
    char *lbuf = g_bufman->GetBuffer(lbufid);
    VarlenDataPage lpg(lbuf);
    auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();

    // inserts 1 to 10 into the left page, inserting an additional record will
    // fail
    ASSERT_EQ(lhdr->m_totrlen, 0);
    maxaligned_char_buf recbuf;
    maxaligned_char_buf recbuf_tmp;
    for (int64_t i = 1; i <= 11; ++i) {
        IndexKey *key = get_key_f2(i ^ m_reorder_mask);
        RecordId recid{(PageNumber)(100 + i), (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_2, key, recid, recbuf_tmp);
        CreateInternalRecord(m_idx_2, Record(recbuf_tmp),
                             /*child_pid =*/(PageNumber)(10000 + i),
                             true, recbuf);
        ASSERT_EQ(recbuf.size(), 400);
        Record rec(recbuf);
        if (i == 1) {
            rec.GetLength() = BTreeInternalRecordHeaderSize;
            ASSERT(rec.GetLength(), 16);
        }
        ASSERT_NO_ERROR(lpg.InsertRecordAt(lpg.GetMaxSlotId() + 1, rec));
        ASSERT_EQ(rec.GetRecordId().sid != INVALID_SID, i < 11)
            << "the " << i << "th insertion on the leaf page is expected to "
            << ((i < 11) ? "succeed" : "fail");
        if (i < 11) {
            lhdr->m_totrlen += rec.GetLength();
        }
    }

    // make a copy of the original left page so we can its non fm header
    // portion with the loaded data
    alignas(CACHELINE_SIZE) char lpg_copy[PAGE_SIZE];
    memcpy(lpg_copy, lbuf, PAGE_SIZE);

    // drop the pin for now
    g_bufman->MarkDirty(lbufid);
    g_bufman->UnpinPage(lbufid);

    // Instead of creating a new right page here, we directly initializes
    // in our local buffer since the right page will be deleted in every
    // successful merge test. We can use this to overwrite the non fm header
    // portion of the page.
    alignas(CACHELINE_SIZE) char rpg_copy[PAGE_SIZE];
    std::memset(rpg_copy, 0, PAGE_SIZE);
    BTreePageHeaderData::Initialize(rpg_copy,
                                    0,
                                    /*prev_pid =*/lpid,
                                    /*next_pid =*/pid0);
    VarlenDataPage rpg(rpg_copy);
    auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
    // insert keys 11 - 20, 21 should fail
    for (int64_t i = 11; i <= 21; ++i) {
        IndexKey *key = get_key_f2(i ^ m_reorder_mask);
        RecordId recid{(PageNumber)(100 + i), (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_2, key, recid, recbuf_tmp);
        CreateInternalRecord(m_idx_2, Record(recbuf_tmp),
                             /*child_pid =*/(PageNumber)(10000 + i),
                             true, recbuf);
        ASSERT_EQ(recbuf.size(), 400);
        Record rec(recbuf);
        if (i == 11) {
            rec.GetLength() = BTreeInternalRecordHeaderSize;
            ASSERT(rec.GetLength(), 16);
        }
        ASSERT_NO_ERROR(rpg.InsertRecordAt(rpg.GetMaxSlotId() + 1, rec));
        ASSERT_EQ(rec.GetRecordId().sid != INVALID_SID, i < 21)
            << "the " << i << "th insertion on the leaf page is expected to "
            << ((i < 21) ? "succeed" : "fail");
        if (i < 21) {
            rhdr->m_totrlen += rec.GetLength();
        }
    }

    for (SlotId lpg_nslots: {1, 2, 4, 5, 6, 7, 9, 10})
    for (SlotId rpg_nslots: {11 - lpg_nslots, 10 - lpg_nslots}) {
        // skip the test case where there's no tuple on the leaf page, which
        // can't happen in a valid B-tree
        if (rpg_nslots == 0)
            continue;

        // prepare the pages

        // prepare the right page first
        ScopedBufferId rbufid =
            CreateNewBTreePage(m_idx_2, false, false, INVALID_PID, INVALID_PID);
        PageNumber rpid = g_bufman->GetPageNumber(rbufid);
        char *rbuf = g_bufman->GetBuffer(rbufid);
        // MUST NOT copy the page header data portion as we're allocating
        // and deallocating pages across the loops!
        memcpy(rbuf + sizeof(PageHeaderData), rpg_copy + sizeof(PageHeaderData),
               (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage rpg(rbuf);
        auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
        ASSERT_LE(rpg_nslots, rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1);
        SlotId n_to_truncate_on_right = rpg.GetMaxSlotId() - rpg.GetMinSlotId()
            + 1 - rpg_nslots;
        for (SlotId sid = rpg.GetMinSlotId();
                    sid < rpg.GetMinSlotId() + n_to_truncate_on_right; ++sid) {
            rhdr->m_totrlen -= rpg.GetRecord(sid).GetLength();
        }
        if (n_to_truncate_on_right > 0) {
            ASSERT_NO_ERROR(
                PurgeFirstNSlotsOnPage(rbuf, n_to_truncate_on_right));
            //ASSERT_NO_ERROR(rpg.ShiftSlots(n_to_truncate_on_right, true));
        }
        // truncate the (new) first record on the right page
        Record first_rec_on_right = rpg.GetRecord(1);
        rhdr->m_totrlen -= first_rec_on_right.GetLength();
        rhdr->m_totrlen += BTreeInternalRecordHeaderSize;
        first_rec_on_right.GetLength() = BTreeInternalRecordHeaderSize;
        ASSERT_TRUE(rpg.UpdateRecord(1, first_rec_on_right));
        ASSERT_NE(first_rec_on_right.GetRecordId().sid, INVALID_SID);
        rhdr->m_prev_pid = lpid;
        rhdr->m_next_pid = pid0;
        g_bufman->MarkDirty(rbufid);
        g_bufman->UnpinPage(rbufid);

        // prepare the page 0 (right most)
        char *buf0;
        ScopedBufferId bufid0 = g_bufman->PinPage(pid0, &buf0);
        VarlenDataPage pg0(buf0);
        auto hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
        hdr0->m_prev_pid = rpid;
        g_bufman->MarkDirty(bufid0);
        g_bufman->UnpinPage(bufid0);

        // prepare the left page
        char *lbuf;
        ScopedBufferId lbufid = g_bufman->PinPage(lpid, &lbuf);
        // MUST NOT copy the page header data portion as we're allocating
        // and deallocating pages across the loops!
        memcpy(lbuf + sizeof(PageHeaderData), lpg_copy + sizeof(PageHeaderData),
               (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage lpg(lbuf);
        ASSERT_LE(lpg_nslots, lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1);
        auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();
        for (SlotId sid = lpg.GetMaxSlotId(); sid > lpg_nslots; --sid) {
            lhdr->m_totrlen -= lpg.GetRecord(sid).GetLength();
            ASSERT_NO_ERROR(lpg.RemoveSlot(sid));
        }
        lhdr->m_next_pid = rpid;
        g_bufman->MarkDirty(lbufid);
        g_bufman->UnpinPage(lbufid);

        // prepare the parent page
        char *pbuf;
        ScopedBufferId pbufid = g_bufman->PinPage(ppid, &pbuf);
        memcpy(pbuf + sizeof(PageHeaderData), ppg_copy + sizeof(PageHeaderData),
              (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage ppg(pbuf);
        ASSERT_EQ(ppg.GetMaxSlotId(), 2);
        auto prechdr1 = (BTreeInternalRecordHeaderData*)
            ppg.GetRecordBuffer(1, nullptr);
        prechdr1->m_child_pid = lpid;
        auto prechdr2 = (BTreeInternalRecordHeaderData*)
            ppg.GetRecordBuffer(2, nullptr);
        prechdr2->m_child_pid = rpid;
        g_bufman->MarkDirty(pbufid);
        g_bufman->UnpinPage(pbufid);

        // now force a buffer flush
        ASSERT_NO_ERROR(ForceFlushBuffer());

        // pin the three pages back in the buffer pool
        BufferId lbufid_ = g_bufman->PinPage(lpid, &lbuf);
        BufferId rbufid_ = g_bufman->PinPage(rpid, &rbuf);
        pbufid = g_bufman->PinPage(ppid, &pbuf);

        // and try to merge the left and right page
        bool merge_succeeded;
        ASSERT_NO_ERROR(merge_succeeded =
            MergePages(m_idx_2, lbufid_, rbufid_, pbufid, 1));

        // merge will only succeed if there're no more than 145 records in total
        bool expected_to_succeed = (lpg_nslots + rpg_nslots) <= 10;
        ASSERT_EQ(merge_succeeded, expected_to_succeed);

        // unpin all these pages
        ASSERT_NO_ERROR(g_bufman->UnpinPage(pbufid));
        if (!merge_succeeded) {
            g_bufman->UnpinPage(lbufid_);
            g_bufman->UnpinPage(rbufid_);
        }

        // If the merge is successful, we flush the buffer pool in order to
        // check if the dirty bits are set correctly. Otherwise, we should not
        // flush the buffer pool, in case the pages are modified but the
        // dirty bits were not set.
        if (merge_succeeded) {
            ASSERT_NO_ERROR(ForceFlushBuffer());
        }

        if (merge_succeeded) {
            // check if the right page is freed
            rbufid = g_bufman->PinPage(rpid, &rbuf);
            PageHeaderData *rph = (PageHeaderData*) rbuf;
            ASSERT_FALSE(rph->IsAllocated())
                << "merge should free the right page upon succesful return";
            g_bufman->UnpinPage(rbufid);

            // check if the pid 0 page has the correct left sibling pointer
            bufid0 = g_bufman->PinPage(pid0, &buf0);
            pg0 = buf0;
            hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
            ASSERT_EQ(hdr0->m_prev_pid, lpid);
            ASSERT_EQ(hdr0->m_next_pid, INVALID_PID);
            ASSERT_FALSE(hdr0->IsRootPage());
            ASSERT_FALSE(hdr0->IsLeafPage());
            g_bufman->UnpinPage(bufid0);

            // check if the internal record pointing to the right page is
            // deleted from the parent page
            pbufid = g_bufman->PinPage(ppid, &pbuf);
            ppg = pbuf;
            // should just have one internal record left on the parent page
            ASSERT_EQ(ppg.GetMaxSlotId(), 1);
            ASSERT_EQ(ppg.GetRecord(1).GetLength(), BTreeInternalRecordHeaderSize);
            auto prechdr1 =
                (BTreeInternalRecordHeaderData*) ppg.GetRecordBuffer(1, nullptr);
            ASSERT_EQ(prechdr1->m_child_pid, lpid);
            auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
            ASSERT_EQ(phdr->m_totrlen, BTreeInternalRecordHeaderSize);
            ASSERT_FALSE(phdr->IsRootPage());
            ASSERT_FALSE(phdr->IsLeafPage());
            g_bufman->UnpinPage(pbufid);

            // check if the left page looks good
            lbufid = g_bufman->PinPage(lpid, &lbuf);
            lpg = lbuf;
            lhdr = (BTreePageHeaderData*) lpg.GetUserData();
            ASSERT_EQ(lhdr->m_prev_pid, INVALID_PID);
            ASSERT_EQ(lhdr->m_next_pid, pid0);
            ASSERT_FALSE(lhdr->IsLeafPage());
            ASSERT_FALSE(lhdr->IsRootPage());

            ASSERT_EQ(lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1,
                      lpg_nslots + rpg_nslots);
            VarlenDataPage lpg_old(lpg_copy);
            VarlenDataPage rpg_old(rpg_copy);
            VarlenDataPage ppg_old(ppg_copy);
            FieldOffset totrlen = 0;
            for (SlotId sid = lpg.GetMinSlotId(); sid <= lpg.GetMaxSlotId();
                    ++sid) {
                Record oldrec;
                if (sid <= lpg_nslots) {
                    oldrec = lpg_old.GetRecord(sid);
                } else if (sid == lpg_nslots + 1) {
                    oldrec = ppg_old.GetRecord(2);
                    auto oldrechdr =
                        (BTreeInternalRecordHeaderData*) oldrec.GetData();
                    oldrechdr->m_child_pid = (PageNumber)
                        (10000 + 10 + n_to_truncate_on_right + 1);
                    oldrechdr->m_heap_recid = RecordId{
                        (PageNumber)(100 + 11),
                        (SlotId)((11 ^ m_reorder_mask) + 1)
                    };
                } else {
                    oldrec = rpg_old.GetRecord(sid - lpg_nslots
                                               + n_to_truncate_on_right);
                }
                Record newrec = lpg.GetRecord(sid);
                ASSERT_EQ(newrec.GetLength(), oldrec.GetLength()) << sid;

                auto oldrechdr =
                    (BTreeInternalRecordHeaderData*) oldrec.GetData();
                auto newrechdr =
                    (BTreeInternalRecordHeaderData*) newrec.GetData();
                ASSERT_EQ(newrechdr->m_child_pid, oldrechdr->m_child_pid)
                    << sid;

                if (sid != lpg.GetMinSlotId()) {
                    ASSERT_EQ(oldrechdr->m_heap_recid, newrechdr->m_heap_recid)
                        << sid;

                    absl::string_view oldkey =
                        read_f2_payload(oldrechdr->GetPayload());
                    absl::string_view newkey =
                        read_f2_payload(newrechdr->GetPayload());
                    ASSERT_EQ(newkey, oldkey) << sid;
                }

                totrlen += newrec.GetLength();
            }
            ASSERT_EQ(lhdr->m_totrlen, totrlen);
            g_bufman->UnpinPage(lbufid);
        } else {
            // check if the right page looks good
            rbufid = g_bufman->PinPage(rpid, &rbuf);
            PageHeaderData *rph = (PageHeaderData*) rbuf;
            ASSERT_TRUE(rph->IsAllocated())
                << "merge should not free the right page upon unsuccessful "
                   "return";
            rpg = rbuf;
            rhdr = (BTreePageHeaderData*) rpg.GetUserData();
            ASSERT_EQ(rhdr->m_prev_pid, lpid);
            ASSERT_EQ(rhdr->m_next_pid, pid0);
            ASSERT_FALSE(rhdr->IsLeafPage());
            ASSERT_FALSE(rhdr->IsRootPage());

            ASSERT_EQ(rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1,
                      rpg_nslots);
            VarlenDataPage rpg_old(rpg_copy);
            FieldOffset totrlen_right = 0;
            for (SlotId sid = rpg.GetMinSlotId(); sid <= rpg.GetMaxSlotId();
                    ++sid) {
                Record newrec = rpg.GetRecord(sid);
                Record oldrec = rpg_old.GetRecord(sid + n_to_truncate_on_right);

                auto oldrechdr =
                    (BTreeInternalRecordHeaderData*) oldrec.GetData();
                auto newrechdr =
                    (BTreeInternalRecordHeaderData*) newrec.GetData();
                ASSERT_EQ(newrechdr->m_child_pid, oldrechdr->m_child_pid)
                    << sid;

                if (sid != rpg.GetMinSlotId()) {
                    ASSERT_EQ(newrec.GetLength(), oldrec.GetLength());
                    ASSERT_EQ(oldrechdr->m_heap_recid, newrechdr->m_heap_recid)
                        << sid;

                    absl::string_view oldkey =
                        read_f2_payload(oldrechdr->GetPayload());
                    absl::string_view newkey =
                        read_f2_payload(newrechdr->GetPayload());
                    ASSERT_EQ(newkey, oldkey) << sid;
                } else {
                    ASSERT_EQ(newrec.GetLength(),
                              BTreeInternalRecordHeaderSize);
                }

                totrlen_right += newrec.GetLength();
            }
            ASSERT_EQ(rhdr->m_totrlen, totrlen_right);
            g_bufman->UnpinPage(rbufid);

            // check if the pid 0 page has the correct left sibling pointer
            bufid0 = g_bufman->PinPage(pid0, &buf0);
            pg0 = buf0;
            hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
            ASSERT_EQ(hdr0->m_prev_pid, rpid);
            ASSERT_EQ(hdr0->m_next_pid, INVALID_PID);
            ASSERT_FALSE(hdr0->IsRootPage());
            ASSERT_FALSE(hdr0->IsLeafPage());
            g_bufman->UnpinPage(bufid0);

            // check if the parent page still has two internal records
            pbufid = g_bufman->PinPage(ppid, &pbuf);
            ppg = pbuf;
            // should just have one internal record left on the parent page
            ASSERT_EQ(ppg.GetMaxSlotId(), 2);
            ASSERT_EQ(ppg.GetRecord(1).GetLength(),
                      BTreeInternalRecordHeaderSize);
            auto prechdr1 = (BTreeInternalRecordHeaderData*)
                ppg.GetRecordBuffer(1, nullptr);
            ASSERT_EQ(prechdr1->m_child_pid, lpid);

            ASSERT_EQ(ppg.GetRecord(2).GetLength(),
                      BTreeInternalRecordHeaderSize + 384);
            auto prechdr2 = (BTreeInternalRecordHeaderData*)
                ppg.GetRecordBuffer(2, nullptr);
            ASSERT_EQ(prechdr2->m_child_pid, rpid);
            ASSERT_EQ(prechdr2->m_heap_recid, ppg_rec2_recid);
            absl::string_view prec2_key =
                read_f2_payload(prechdr2->GetPayload());
            absl::string_view expected_prec2_key =
                get_skey(11 ^ m_reorder_mask);
            ASSERT_EQ(prec2_key, expected_prec2_key);

            auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
            ASSERT_EQ(phdr->m_totrlen, BTreeInternalRecordHeaderSize * 2 + 384);
            ASSERT_FALSE(phdr->IsRootPage());
            ASSERT_FALSE(phdr->IsLeafPage());
            g_bufman->UnpinPage(pbufid);

            // check if the left page looks good
            lbufid = g_bufman->PinPage(lpid, &lbuf);
            lpg = lbuf;
            lhdr = (BTreePageHeaderData*) lpg.GetUserData();
            ASSERT_EQ(lhdr->m_prev_pid, INVALID_PID);
            ASSERT_EQ(lhdr->m_next_pid, rpid);
            ASSERT_FALSE(lhdr->IsLeafPage());
            ASSERT_FALSE(lhdr->IsRootPage());

            ASSERT_EQ(lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1,
                      lpg_nslots);
            VarlenDataPage lpg_old(lpg_copy);
            FieldOffset totrlen_left = 0;
            for (SlotId sid = lpg.GetMinSlotId(); sid <= lpg.GetMaxSlotId();
                    ++sid) {
                Record oldrec = lpg_old.GetRecord(sid);
                Record newrec = lpg.GetRecord(sid);
                ASSERT_EQ(newrec.GetLength(), oldrec.GetLength()) << sid;

                auto oldrechdr = (BTreeInternalRecordHeaderData*)
                    oldrec.GetData();
                auto newrechdr = (BTreeInternalRecordHeaderData*)
                    newrec.GetData();
                ASSERT_EQ(newrechdr->m_child_pid, oldrechdr->m_child_pid)
                    << sid;

                if (sid != lpg.GetMinSlotId()) {
                    ASSERT_EQ(oldrechdr->m_heap_recid, newrechdr->m_heap_recid)
                        << sid;

                    absl::string_view oldkey =
                        read_f2_payload(oldrechdr->GetPayload());
                    absl::string_view newkey =
                        read_f2_payload(newrechdr->GetPayload());
                    ASSERT_EQ(newkey, oldkey) << sid;
                }

                totrlen_left += newrec.GetLength();
            }
            ASSERT_EQ(lhdr->m_totrlen, totrlen_left);
            g_bufman->UnpinPage(lbufid);
        }
    }

    TDB_TEST_END
}

TEST_F(BasicTestBTreeMergePages, TestMergeLeafPageWithExactFill) {
    TDB_TEST_BEGIN

    // strlen: 380 * 10 + 28 + 36
    // payload len: 384 * 10 + 32 + 40
    // reclen: 392 * 10 + 40 + 48
    ASSERT_EQ(BTreeComputePageUsage(12, 10 * 392 + 40 + 48), 4096);

    ScopedBufferId bufid0 =
        CreateNewBTreePage(m_idx_2, false, true, INVALID_PID, INVALID_PID);
    PageNumber pid0 = g_bufman->GetPageNumber(bufid0);
    // we can unpin page 0 for now
    g_bufman->UnpinPage(bufid0);

    // creates a parent page
    ScopedBufferId pbufid =
        CreateNewBTreePage(m_idx_2, false, false, INVALID_PID, INVALID_PID);
    PageNumber ppid = g_bufman->GetPageNumber(pbufid);
    char *pbuf = g_bufman->GetBuffer(pbufid);
    VarlenDataPage ppg(pbuf);
    // inserts two dummy records into the parent page, we will fill their
    // values a bit later
    //
    // We intentionally do not insert a third record here pointing to pid0,
    // because it may be on a right sibling of the parent page instead. The
    // MergePages() implementation should not use the slot id to find the right
    // sibling of the right page. Instead, it should use the m_next_pid in the
    // header.
    BTreeInternalRecordHeaderData ppg_rec1_data;
    Record ppg_rec1((char*) &ppg_rec1_data, BTreeInternalRecordHeaderSize);
    ASSERT_NO_ERROR(ppg.InsertRecordAt(ppg.GetMaxSlotId() + 1, ppg_rec1));
    ASSERT_NE(ppg_rec1.GetRecordId().sid, INVALID_SID);
    IndexKey *ppg_rec2_key = get_key_f2(13 ^ m_reorder_mask);
    RecordId ppg_rec2_recid{
        (PageNumber)(100 + 13), (SlotId)((13 ^ m_reorder_mask) + 1)};
    maxaligned_char_buf ppg_rec2_buf_tmp;
    maxaligned_char_buf ppg_rec2_buf;
    CreateLeafRecord(m_idx_2, ppg_rec2_key, ppg_rec2_recid, ppg_rec2_buf_tmp);
    CreateInternalRecord(m_idx_2, Record(ppg_rec2_buf_tmp),
                         /* child_pid = */INVALID_PID,
                         true, ppg_rec2_buf);
    Record ppg_rec2(ppg_rec2_buf);
    ASSERT_NO_ERROR(ppg.InsertRecordAt(ppg.GetMaxSlotId() + 1, ppg_rec2));
    ASSERT_NE(ppg_rec2.GetRecordId().sid, INVALID_SID);

    auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
    phdr->m_totrlen = ppg_rec1.GetLength() + ppg_rec2.GetLength();

    // make a copy of the parent page for later overwrites
    alignas(CACHELINE_SIZE) char ppg_copy[PAGE_SIZE];
    memcpy(ppg_copy, pbuf, PAGE_SIZE);

    g_bufman->MarkDirty(pbufid);
    g_bufman->UnpinPage(pbufid);

    // creates two new leaf pages followed by pid0
    ScopedBufferId lbufid =
        CreateNewBTreePage(m_idx_2, false, true, INVALID_PID, INVALID_PID);
    PageNumber lpid = g_bufman->GetPageNumber(lbufid);
    char *lbuf = g_bufman->GetBuffer(lbufid);
    VarlenDataPage lpg(lbuf);
    auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();

    // inserts 1 to 12 into the left page, inserting a minimal record will
    // fail
    ASSERT_EQ(lhdr->m_totrlen, 0);
    maxaligned_char_buf recbuf;
    for (int64_t i = 1; i <= 13; ++i) {
        FieldOffset keylen;
        if (i <= 10) {
            keylen = 380;
        } else if (i == 11) {
            keylen = 36;
        } else if (i == 12) {
            keylen = 28;
        } else {
            keylen = 4;
        }
        IndexKey *key = get_key_f2(i ^ m_reorder_mask, keylen);
        RecordId recid{(PageNumber)(100 + i), (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_2, key, recid, recbuf);
        Record rec(recbuf);
        ASSERT_EQ(recbuf.size(), keylen + 12);
        ASSERT_NO_ERROR(lpg.InsertRecordAt(lpg.GetMaxSlotId() + 1, rec));
        ASSERT_EQ(rec.GetRecordId().sid != INVALID_SID, i < 13)
            << "the " << i << "th insertion on the leaf page is expected to "
            << ((i < 13) ? "succeed" : "fail");
        if (i < 13) {
            lhdr->m_totrlen += rec.GetLength();
        }
    }

    // make a copy of the original left page so we can its non fm header
    // portion with the loaded data
    alignas(CACHELINE_SIZE) char lpg_copy[PAGE_SIZE];
    memcpy(lpg_copy, lbuf, PAGE_SIZE);

    // drop the pin for now
    g_bufman->MarkDirty(lbufid);
    g_bufman->UnpinPage(lbufid);

    // Instead of creating a new right page here, we directly initializes
    // in our local buffer since the right page will be deleted in every
    // successful merge test. We can use this to overwrite the non fm header
    // portion of the page.
    alignas(CACHELINE_SIZE) char rpg_copy[PAGE_SIZE];
    std::memset(rpg_copy, 0, PAGE_SIZE);
    BTreePageHeaderData::Initialize(rpg_copy,
                                    BTREE_PAGE_ISLEAF,
                                    /*prev_pid =*/lpid,
                                    /*next_pid =*/pid0);
    VarlenDataPage rpg(rpg_copy);
    auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
    // insert keys 13 - 24, 25 should fail
    for (int64_t i = 13; i <= 25; ++i) {
        FieldOffset keylen;
        if (i <= 22) {
            keylen = 380;
        } else if (i == 23) {
            keylen = 28;
        } else if (i == 24) {
            keylen = 36;
        } else {
            keylen = 4;
        }
        IndexKey *key = get_key_f2(i ^ m_reorder_mask, keylen);
        RecordId recid{(PageNumber)(100 + i), (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_2, key, recid, recbuf);
        Record rec(recbuf);
        ASSERT_EQ(recbuf.size(), keylen + 12);
        ASSERT_NO_ERROR(rpg.InsertRecordAt(rpg.GetMaxSlotId() + 1, rec));
        ASSERT_EQ(rec.GetRecordId().sid != INVALID_SID, i < 25)
            << "the " << i << "th insertion on the leaf page is expected to "
            << ((i < 25) ? "succeed" : "fail");
        if (i < 25) {
            rhdr->m_totrlen += rec.GetLength();
        }
    }

    for (SlotId lpg_nslots: {1, 2, 3, 6, 9, 10, 11})
    for (SlotId rpg_nslots: {12 - lpg_nslots}) {
        // skip the test case where there's no tuple on the leaf page, which
        // can't happen in a valid B-tree
        if (rpg_nslots == 0)
            continue;

        // prepare the pages

        // prepare the right page first
        ScopedBufferId rbufid =
            CreateNewBTreePage(m_idx_2, false, true, INVALID_PID, INVALID_PID);
        PageNumber rpid = g_bufman->GetPageNumber(rbufid);
        char *rbuf = g_bufman->GetBuffer(rbufid);
        // MUST NOT copy the page header data portion as we're allocating
        // and deallocating pages across the loops!
        memcpy(rbuf + sizeof(PageHeaderData), rpg_copy + sizeof(PageHeaderData),
               (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage rpg(rbuf);
        auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
        ASSERT_LE(rpg_nslots, rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1);
        SlotId n_to_truncate_on_right = rpg.GetMaxSlotId() - rpg.GetMinSlotId()
            + 1 - rpg_nslots;
        for (SlotId sid = rpg.GetMinSlotId();
                    sid < rpg.GetMinSlotId() + n_to_truncate_on_right; ++sid) {
            rhdr->m_totrlen -= rpg.GetRecord(sid).GetLength();
        }
        if (n_to_truncate_on_right > 0) {
            ASSERT_NO_ERROR(
                PurgeFirstNSlotsOnPage(rbuf, n_to_truncate_on_right));
            //ASSERT_NO_ERROR(rpg.ShiftSlots(n_to_truncate_on_right, true));
        }
        rhdr->m_prev_pid = lpid;
        rhdr->m_next_pid = pid0;
        g_bufman->MarkDirty(rbufid);
        g_bufman->UnpinPage(rbufid);

        // prepare the page 0 (right most)
        char *buf0;
        ScopedBufferId bufid0 = g_bufman->PinPage(pid0, &buf0);
        VarlenDataPage pg0(buf0);
        auto hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
        hdr0->m_prev_pid = rpid;
        g_bufman->MarkDirty(bufid0);
        g_bufman->UnpinPage(bufid0);

        // prepare the left page
        char *lbuf;
        ScopedBufferId lbufid = g_bufman->PinPage(lpid, &lbuf);
        // MUST NOT copy the page header data portion as we're allocating
        // and deallocating pages across the loops!
        memcpy(lbuf + sizeof(PageHeaderData), lpg_copy + sizeof(PageHeaderData),
               (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage lpg(lbuf);
        ASSERT_LE(lpg_nslots, lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1);
        auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();
        for (SlotId sid = lpg.GetMaxSlotId(); sid > lpg_nslots; --sid) {
            lhdr->m_totrlen -= lpg.GetRecord(sid).GetLength();
            ASSERT_NO_ERROR(lpg.RemoveSlot(sid));
        }
        lhdr->m_next_pid = rpid;
        g_bufman->MarkDirty(lbufid);
        g_bufman->UnpinPage(lbufid);

        // prepare the parent page
        char *pbuf;
        ScopedBufferId pbufid = g_bufman->PinPage(ppid, &pbuf);
        memcpy(pbuf + sizeof(PageHeaderData), ppg_copy + sizeof(PageHeaderData),
              (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage ppg(pbuf);
        ASSERT_EQ(ppg.GetMaxSlotId(), 2);
        auto prechdr1 = (BTreeInternalRecordHeaderData*) ppg.GetRecordBuffer(1, nullptr);
        prechdr1->m_child_pid = lpid;
        auto prechdr2 = (BTreeInternalRecordHeaderData*) ppg.GetRecordBuffer(2, nullptr);
        prechdr2->m_child_pid = rpid;
        g_bufman->MarkDirty(pbufid);
        g_bufman->UnpinPage(pbufid);

        // now force a buffer flush
        ASSERT_NO_ERROR(ForceFlushBuffer());

        // pin the three pages back in the buffer pool
        BufferId lbufid_ = g_bufman->PinPage(lpid, &lbuf);
        BufferId rbufid_ = g_bufman->PinPage(rpid, &rbuf);
        pbufid = g_bufman->PinPage(ppid, &pbuf);

        // and try to merge the left and right page
        bool merge_succeeded;
        ASSERT_NO_ERROR(merge_succeeded =
            MergePages(m_idx_2, lbufid_, rbufid_, pbufid, 1));

        // merge will succeed except when we merge the first 11 slots on the
        // left and the last slot on the right, the page usage of which will be
        // excatly 12 bytes over the page size.
        bool expected_to_succeed = lpg_nslots < 11;
        ASSERT_EQ(merge_succeeded, expected_to_succeed);

        // unpin all these pages
        ASSERT_NO_ERROR(g_bufman->UnpinPage(pbufid));
        if (!merge_succeeded) {
            g_bufman->UnpinPage(lbufid_);
            g_bufman->UnpinPage(rbufid_);
        }

        // If the merge is successful, we flush the buffer pool in order to
        // check if the dirty bits are set correctly. Otherwise, we should not
        // flush the buffer pool, in case the pages are modified but the
        // dirty bits were not set.
        if (merge_succeeded) {
            ASSERT_NO_ERROR(ForceFlushBuffer());
        }

        if (merge_succeeded) {
            // check if the right page is freed
            rbufid = g_bufman->PinPage(rpid, &rbuf);
            PageHeaderData *rph = (PageHeaderData*) rbuf;
            ASSERT_FALSE(rph->IsAllocated())
                << "merge should free the right page upon succesful return";
            g_bufman->UnpinPage(rbufid);

            // check if the pid 0 page has the correct left sibling pointer
            bufid0 = g_bufman->PinPage(pid0, &buf0);
            pg0 = buf0;
            hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
            ASSERT_EQ(hdr0->m_prev_pid, lpid);
            ASSERT_EQ(hdr0->m_next_pid, INVALID_PID);
            ASSERT_FALSE(hdr0->IsRootPage());
            ASSERT_TRUE(hdr0->IsLeafPage());
            g_bufman->UnpinPage(bufid0);

            // check if the internal record pointing to the right page is
            // deleted from the parent page
            pbufid = g_bufman->PinPage(ppid, &pbuf);
            ppg = pbuf;
            // should just have one internal record left on the parent page
            ASSERT_EQ(ppg.GetMaxSlotId(), 1);
            ASSERT_EQ(ppg.GetRecord(1).GetLength(), BTreeInternalRecordHeaderSize);
            auto prechdr1 =
                (BTreeInternalRecordHeaderData*) ppg.GetRecordBuffer(1, nullptr);
            ASSERT_EQ(prechdr1->m_child_pid, lpid);
            auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
            ASSERT_EQ(phdr->m_totrlen, BTreeInternalRecordHeaderSize);
            ASSERT_FALSE(phdr->IsRootPage());
            ASSERT_FALSE(phdr->IsLeafPage());
            g_bufman->UnpinPage(pbufid);

            // check if the left page looks good
            lbufid = g_bufman->PinPage(lpid, &lbuf);
            lpg = lbuf;
            lhdr = (BTreePageHeaderData*) lpg.GetUserData();
            ASSERT_EQ(lhdr->m_prev_pid, INVALID_PID);
            ASSERT_EQ(lhdr->m_next_pid, pid0);
            ASSERT_TRUE(lhdr->IsLeafPage());
            ASSERT_FALSE(lhdr->IsRootPage());

            ASSERT_EQ(lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1,
                      lpg_nslots + rpg_nslots);
            VarlenDataPage lpg_old(lpg_copy);
            VarlenDataPage rpg_old(rpg_copy);
            FieldOffset totrlen = 0;
            for (SlotId sid = lpg.GetMinSlotId(); sid <= lpg.GetMaxSlotId();
                    ++sid) {
                Record oldrec;
                if (sid <= lpg_nslots) {
                    oldrec = lpg_old.GetRecord(sid);
                } else {
                    oldrec = rpg_old.GetRecord(sid - lpg_nslots
                                               + n_to_truncate_on_right);
                }
                Record newrec = lpg.GetRecord(sid);
                ASSERT_EQ(newrec.GetLength(), oldrec.GetLength()) << sid;

                auto oldrechdr = (BTreeLeafRecordHeaderData*) oldrec.GetData();
                auto newrechdr = (BTreeLeafRecordHeaderData*) newrec.GetData();
                ASSERT_EQ(newrechdr->m_recid, oldrechdr->m_recid) << sid;

                absl::string_view oldkey =
                    read_f2_payload(oldrechdr->GetPayload());
                absl::string_view newkey =
                    read_f2_payload(newrechdr->GetPayload());
                ASSERT_EQ(newkey, oldkey);

                totrlen += newrec.GetLength();
            }
            ASSERT_EQ(lhdr->m_totrlen, totrlen);
            g_bufman->UnpinPage(lbufid);
        } else {
            // check if the right page looks good
            rbufid = g_bufman->PinPage(rpid, &rbuf);
            PageHeaderData *rph = (PageHeaderData*) rbuf;
            ASSERT_TRUE(rph->IsAllocated())
                << "merge should not free the right page upon unsuccessful "
                   "return";
            rpg = rbuf;
            rhdr = (BTreePageHeaderData*) rpg.GetUserData();
            ASSERT_EQ(rhdr->m_prev_pid, lpid);
            ASSERT_EQ(rhdr->m_next_pid, pid0);
            ASSERT_TRUE(rhdr->IsLeafPage());
            ASSERT_FALSE(rhdr->IsRootPage());

            ASSERT_EQ(rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1,
                      rpg_nslots);
            VarlenDataPage rpg_old(rpg_copy);
            FieldOffset totrlen_right = 0;
            for (SlotId sid = rpg.GetMinSlotId(); sid <= rpg.GetMaxSlotId();
                    ++sid) {
                Record newrec = rpg.GetRecord(sid);
                Record oldrec = rpg_old.GetRecord(sid + n_to_truncate_on_right);
                ASSERT_EQ(newrec.GetLength(), oldrec.GetLength());

                auto oldrechdr = (BTreeLeafRecordHeaderData*) oldrec.GetData();
                auto newrechdr = (BTreeLeafRecordHeaderData*) newrec.GetData();
                ASSERT_EQ(newrechdr->m_recid, oldrechdr->m_recid) << sid;

                absl::string_view oldkey =
                    read_f2_payload(oldrechdr->GetPayload());
                absl::string_view newkey =
                    read_f2_payload(newrechdr->GetPayload());
                ASSERT_EQ(newkey, oldkey);

                totrlen_right += newrec.GetLength();
            }
            ASSERT_EQ(rhdr->m_totrlen, totrlen_right);
            g_bufman->UnpinPage(rbufid);

            // check if the pid 0 page has the correct left sibling pointer
            bufid0 = g_bufman->PinPage(pid0, &buf0);
            pg0 = buf0;
            hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
            ASSERT_EQ(hdr0->m_prev_pid, rpid);
            ASSERT_EQ(hdr0->m_next_pid, INVALID_PID);
            ASSERT_FALSE(hdr0->IsRootPage());
            ASSERT_TRUE(hdr0->IsLeafPage());
            g_bufman->UnpinPage(bufid0);

            // check if the parent page still has two internal records
            pbufid = g_bufman->PinPage(ppid, &pbuf);
            ppg = pbuf;
            // should just have one internal record left on the parent page
            ASSERT_EQ(ppg.GetMaxSlotId(), 2);
            ASSERT_EQ(ppg.GetRecord(1).GetLength(),
                      BTreeInternalRecordHeaderSize);
            auto prechdr1 = (BTreeInternalRecordHeaderData*)
                ppg.GetRecordBuffer(1, nullptr);
            ASSERT_EQ(prechdr1->m_child_pid, lpid);

            ASSERT_EQ(ppg.GetRecord(2).GetLength(),
                      BTreeInternalRecordHeaderSize + 384);
            auto prechdr2 = (BTreeInternalRecordHeaderData*)
                ppg.GetRecordBuffer(2, nullptr);
            ASSERT_EQ(prechdr2->m_child_pid, rpid);
            ASSERT_EQ(prechdr2->m_heap_recid, ppg_rec2_recid);
            absl::string_view prec2_key =
                read_f2_payload(prechdr2->GetPayload());
            absl::string_view expected_prec2_key =
                get_skey(13 ^ m_reorder_mask);
            ASSERT_EQ(prec2_key, expected_prec2_key);

            auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
            ASSERT_EQ(phdr->m_totrlen, BTreeInternalRecordHeaderSize * 2 + 384);
            ASSERT_FALSE(phdr->IsRootPage());
            ASSERT_FALSE(phdr->IsLeafPage());
            g_bufman->UnpinPage(pbufid);

            // check if the left page looks good
            lbufid = g_bufman->PinPage(lpid, &lbuf);
            lpg = lbuf;
            lhdr = (BTreePageHeaderData*) lpg.GetUserData();
            ASSERT_EQ(lhdr->m_prev_pid, INVALID_PID);
            ASSERT_EQ(lhdr->m_next_pid, rpid);
            ASSERT_TRUE(lhdr->IsLeafPage());
            ASSERT_FALSE(lhdr->IsRootPage());

            ASSERT_EQ(lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1,
                      lpg_nslots);
            VarlenDataPage lpg_old(lpg_copy);
            FieldOffset totrlen_left = 0;
            for (SlotId sid = lpg.GetMinSlotId(); sid <= lpg.GetMaxSlotId();
                    ++sid) {
                Record oldrec = lpg_old.GetRecord(sid);
                Record newrec = lpg.GetRecord(sid);
                ASSERT_EQ(newrec.GetLength(), oldrec.GetLength()) << sid;

                auto oldrechdr = (BTreeLeafRecordHeaderData*) oldrec.GetData();
                auto newrechdr = (BTreeLeafRecordHeaderData*) newrec.GetData();
                ASSERT_EQ(newrechdr->m_recid, oldrechdr->m_recid) << sid;

                absl::string_view oldkey =
                    read_f2_payload(oldrechdr->GetPayload());
                absl::string_view newkey =
                    read_f2_payload(newrechdr->GetPayload());
                ASSERT_EQ(newkey, oldkey);

                totrlen_left += newrec.GetLength();
            }
            ASSERT_EQ(lhdr->m_totrlen, totrlen_left);
            g_bufman->UnpinPage(lbufid);
        }
    }

    TDB_TEST_END
}

TEST_F(BasicTestBTreeMergePages, TestMergeInternalPageWithExactFill) {
    TDB_TEST_BEGIN

    // strlen: 380 * 10 + 180 + 172
    // payload: 0 + 384 * 9 + 184 + 176
    // reclen: 16 + 400 * 9 + 200 + 192
    ASSERT_EQ(BTreeComputePageUsage(12, 9 * 400 + 16 + 200 + 192), 4096);

    ScopedBufferId bufid0 =
        CreateNewBTreePage(m_idx_2, false, false, INVALID_PID, INVALID_PID);
    PageNumber pid0 = g_bufman->GetPageNumber(bufid0);
    // we can unpin page 0 for now
    g_bufman->UnpinPage(bufid0);

    // creates a parent page
    ScopedBufferId pbufid =
        CreateNewBTreePage(m_idx_2, false, false, INVALID_PID, INVALID_PID);
    PageNumber ppid = g_bufman->GetPageNumber(pbufid);
    char *pbuf = g_bufman->GetBuffer(pbufid);
    VarlenDataPage ppg(pbuf);
    // inserts two dummy records into the parent page, we will fill their
    // values a bit later
    //
    // We intentionally do not insert a third record here pointing to pid0,
    // because it may be on a right sibling of the parent page instead. The
    // MergePages() implementation should not use the slot id to find the right
    // sibling of the right page. Instead, it should use the m_next_pid in the
    // header.
    BTreeInternalRecordHeaderData ppg_rec1_data;
    Record ppg_rec1((char*) &ppg_rec1_data, BTreeInternalRecordHeaderSize);
    ASSERT_NO_ERROR(ppg.InsertRecordAt(ppg.GetMaxSlotId() + 1, ppg_rec1));
    ASSERT_NE(ppg_rec1.GetRecordId().sid, INVALID_SID);
    IndexKey *ppg_rec2_key = get_key_f2(13 ^ m_reorder_mask);
    RecordId ppg_rec2_recid{
        (PageNumber)(100 + 13), (SlotId)((13 ^ m_reorder_mask) + 1)};
    maxaligned_char_buf ppg_rec2_buf_tmp;
    maxaligned_char_buf ppg_rec2_buf;
    CreateLeafRecord(m_idx_2, ppg_rec2_key, ppg_rec2_recid, ppg_rec2_buf_tmp);
    CreateInternalRecord(m_idx_2, Record(ppg_rec2_buf_tmp),
                         /* child_pid = */INVALID_PID,
                         true, ppg_rec2_buf);
    Record ppg_rec2(ppg_rec2_buf);
    ASSERT_NO_ERROR(ppg.InsertRecordAt(ppg.GetMaxSlotId() + 1, ppg_rec2));
    ASSERT_NE(ppg_rec2.GetRecordId().sid, INVALID_SID);

    auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
    phdr->m_totrlen = ppg_rec1.GetLength() + ppg_rec2.GetLength();

    // make a copy of the parent page for later overwrites
    alignas(CACHELINE_SIZE) char ppg_copy[PAGE_SIZE];
    memcpy(ppg_copy, pbuf, PAGE_SIZE);

    g_bufman->MarkDirty(pbufid);
    g_bufman->UnpinPage(pbufid);

    // creates two new leaf pages followed by pid0
    ScopedBufferId lbufid =
        CreateNewBTreePage(m_idx_2, false, false, INVALID_PID, INVALID_PID);
    PageNumber lpid = g_bufman->GetPageNumber(lbufid);
    char *lbuf = g_bufman->GetBuffer(lbufid);
    VarlenDataPage lpg(lbuf);
    auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();

    // inserts 1 to 12 into the left page, inserting an additional record will
    // fail
    ASSERT_EQ(lhdr->m_totrlen, 0);
    maxaligned_char_buf recbuf;
    maxaligned_char_buf recbuf_tmp;
    for (int64_t i = 1; i <= 13; ++i) {
        FieldOffset keylen;
        if (i <= 10) {
            keylen = 380;
        } else if (i == 11) {
            keylen = 180;
        } else if (i == 12) {
            keylen = 172;
        } else {
            keylen = 4;
        }
        IndexKey *key = get_key_f2(i ^ m_reorder_mask, keylen);
        RecordId recid{(PageNumber)(100 + i), (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_2, key, recid, recbuf_tmp);
        CreateInternalRecord(m_idx_2, Record(recbuf_tmp),
                             /*child_pid =*/(PageNumber)(10000 + i),
                             true, recbuf);
        ASSERT_EQ(recbuf.size(), keylen + 20);
        Record rec(recbuf);
        if (i == 1) {
            rec.GetLength() = BTreeInternalRecordHeaderSize;
            ASSERT(rec.GetLength(), 16);
        }
        ASSERT_NO_ERROR(lpg.InsertRecordAt(lpg.GetMaxSlotId() + 1, rec));
        ASSERT_EQ(rec.GetRecordId().sid != INVALID_SID, i < 13)
            << "the " << i << "th insertion on the leaf page is expected to "
            << ((i < 13) ? "succeed" : "fail");
        if (i < 13) {
            lhdr->m_totrlen += rec.GetLength();
        }
    }

    // make a copy of the original left page so we can its non fm header
    // portion with the loaded data
    alignas(CACHELINE_SIZE) char lpg_copy[PAGE_SIZE];
    memcpy(lpg_copy, lbuf, PAGE_SIZE);

    // drop the pin for now
    g_bufman->MarkDirty(lbufid);
    g_bufman->UnpinPage(lbufid);

    // Instead of creating a new right page here, we directly initializes
    // in our local buffer since the right page will be deleted in every
    // successful merge test. We can use this to overwrite the non fm header
    // portion of the page.
    alignas(CACHELINE_SIZE) char rpg_copy[PAGE_SIZE];
    std::memset(rpg_copy, 0, PAGE_SIZE);
    BTreePageHeaderData::Initialize(rpg_copy,
                                    0,
                                    /*prev_pid =*/lpid,
                                    /*next_pid =*/pid0);
    VarlenDataPage rpg(rpg_copy);
    auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
    // insert keys 13 - 24, 25 should fail
    for (int64_t i = 13; i <= 25; ++i) {
        FieldOffset keylen;
        if (i <= 22) {
            keylen = 380;
        } else if (i == 23) {
            keylen = 172;
        } else if (i == 24) {
            keylen = 180;
        } else {
            keylen = 4;
        }
        IndexKey *key = get_key_f2(i ^ m_reorder_mask, keylen);
        RecordId recid{(PageNumber)(100 + i), (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_2, key, recid, recbuf_tmp);
        CreateInternalRecord(m_idx_2, Record(recbuf_tmp),
                             /*child_pid =*/(PageNumber)(10000 + i),
                             true, recbuf);
        ASSERT_EQ(recbuf.size(), keylen + 20);
        Record rec(recbuf);
        if (i == 13) {
            rec.GetLength() = BTreeInternalRecordHeaderSize;
            ASSERT(rec.GetLength(), 16);
        }
        ASSERT_NO_ERROR(rpg.InsertRecordAt(rpg.GetMaxSlotId() + 1, rec));
        ASSERT_EQ(rec.GetRecordId().sid != INVALID_SID, i < 25)
            << "the " << i << "th insertion on the leaf page is expected to "
            << ((i < 25) ? "succeed" : "fail");
        if (i < 25) {
            rhdr->m_totrlen += rec.GetLength();
        }
    }

    for (SlotId lpg_nslots: {1, 2, 3, 6, 9, 10, 11})
    for (SlotId rpg_nslots: {12 - lpg_nslots})
    for (FieldOffset pkey_len : {
            380,
            (lpg_nslots == 10) ? 172 :
            (lpg_nslots == 11) ? 180
            : -1}) {

        // skip the invalid tests
        if (pkey_len == -1)
            continue;

        ASSERT_EQ(MAXALIGN(pkey_len + 20), pkey_len + 20);

        // prepare the pages

        // prepare the right page first
        ScopedBufferId rbufid =
            CreateNewBTreePage(m_idx_2, false, false, INVALID_PID, INVALID_PID);
        PageNumber rpid = g_bufman->GetPageNumber(rbufid);
        char *rbuf = g_bufman->GetBuffer(rbufid);
        // MUST NOT copy the page header data portion as we're allocating
        // and deallocating pages across the loops!
        memcpy(rbuf + sizeof(PageHeaderData), rpg_copy + sizeof(PageHeaderData),
               (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage rpg(rbuf);
        auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
        ASSERT_LE(rpg_nslots, rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1);
        SlotId n_to_truncate_on_right = rpg.GetMaxSlotId() - rpg.GetMinSlotId()
            + 1 - rpg_nslots;
        for (SlotId sid = rpg.GetMinSlotId();
                    sid < rpg.GetMinSlotId() + n_to_truncate_on_right; ++sid) {
            rhdr->m_totrlen -= rpg.GetRecord(sid).GetLength();
        }
        if (n_to_truncate_on_right > 0) {
            ASSERT_NO_ERROR(
                PurgeFirstNSlotsOnPage(rbuf, n_to_truncate_on_right));
            //ASSERT_NO_ERROR(rpg.ShiftSlots(n_to_truncate_on_right, true));
        }
        // truncate the (new) first record on the right page
        Record first_rec_on_right = rpg.GetRecord(1);
        rhdr->m_totrlen -= first_rec_on_right.GetLength();
        rhdr->m_totrlen += BTreeInternalRecordHeaderSize;
        first_rec_on_right.GetLength() = BTreeInternalRecordHeaderSize;
        ASSERT_TRUE(rpg.UpdateRecord(1, first_rec_on_right));
        ASSERT_NE(first_rec_on_right.GetRecordId().sid, INVALID_SID);
        rhdr->m_prev_pid = lpid;
        rhdr->m_next_pid = pid0;
        g_bufman->MarkDirty(rbufid);
        g_bufman->UnpinPage(rbufid);

        // prepare the page 0 (right most)
        char *buf0;
        ScopedBufferId bufid0 = g_bufman->PinPage(pid0, &buf0);
        VarlenDataPage pg0(buf0);
        auto hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
        hdr0->m_prev_pid = rpid;
        g_bufman->MarkDirty(bufid0);
        g_bufman->UnpinPage(bufid0);

        // prepare the left page
        char *lbuf;
        ScopedBufferId lbufid = g_bufman->PinPage(lpid, &lbuf);
        // MUST NOT copy the page header data portion as we're allocating
        // and deallocating pages across the loops!
        memcpy(lbuf + sizeof(PageHeaderData), lpg_copy + sizeof(PageHeaderData),
               (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage lpg(lbuf);
        ASSERT_LE(lpg_nslots, lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1);
        auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();
        for (SlotId sid = lpg.GetMaxSlotId(); sid > lpg_nslots; --sid) {
            lhdr->m_totrlen -= lpg.GetRecord(sid).GetLength();
            ASSERT_NO_ERROR(lpg.RemoveSlot(sid));
        }
        lhdr->m_next_pid = rpid;
        g_bufman->MarkDirty(lbufid);
        g_bufman->UnpinPage(lbufid);

        // prepare the parent page
        char *pbuf;
        ScopedBufferId pbufid = g_bufman->PinPage(ppid, &pbuf);
        memcpy(pbuf + sizeof(PageHeaderData), ppg_copy + sizeof(PageHeaderData),
              (size_t) PAGE_SIZE - sizeof(PageHeaderData));
        VarlenDataPage ppg(pbuf);
        ASSERT_EQ(ppg.GetMaxSlotId(), 2);
        auto prechdr1 = (BTreeInternalRecordHeaderData*)
            ppg.GetRecordBuffer(1, nullptr);
        prechdr1->m_child_pid = lpid;

        // shorten the key length if this test case requires us to do so
        auto prec2 = ppg.GetRecord(2);
        if (pkey_len + 20 != prec2.GetLength()) {
            ASSERT_LE(pkey_len + 20, prec2.GetLength());

            auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
            phdr->m_totrlen -= prec2.GetLength();
            phdr->m_totrlen += 20 + pkey_len;

            IndexKey *key = get_key_f2(13 ^ m_reorder_mask, pkey_len);
            RecordId recid{
                (PageNumber)(100 + 13), (SlotId)((13 ^ m_reorder_mask) + 1)};
            maxaligned_char_buf recbuf_tmp;
            maxaligned_char_buf recbuf;
            CreateLeafRecord(m_idx_2, key, recid, recbuf_tmp);
            CreateInternalRecord(m_idx_2, Record(recbuf_tmp),
                                 /* child_pid = */rpid,
                                 true, recbuf);
            Record rec(recbuf);
            ASSERT_EQ(rec.GetLength(), pkey_len + 20);
            ASSERT_TRUE(ppg.UpdateRecord(2, rec));
            ASSERT_NE(rec.GetRecordId().sid, INVALID_SID);
        } else {
            auto prechdr2 = (BTreeInternalRecordHeaderData*) prec2.GetData();
            prechdr2->m_child_pid = rpid;
        }
        g_bufman->MarkDirty(pbufid);
        g_bufman->UnpinPage(pbufid);

        // now force a buffer flush
        ASSERT_NO_ERROR(ForceFlushBuffer());

        // pin the three pages back in the buffer pool
        BufferId lbufid_ = g_bufman->PinPage(lpid, &lbuf);
        BufferId rbufid_ = g_bufman->PinPage(rpid, &rbuf);
        pbufid = g_bufman->PinPage(ppid, &pbuf);

        // and try to merge the left and right page
        bool merge_succeeded;
        ASSERT_NO_ERROR(merge_succeeded =
            MergePages(m_idx_2, lbufid_, rbufid_, pbufid, 1));

        bool expected_to_succeed;
        if (lpg_nslots <= 9) {
            // merging fewer than 9 of 380's on the left and a 380 on parent
            // and the last a few slots on the right, which should exactly fill
            // the page to PAGE_SIZE
            ASSERT_EQ(pkey_len, 380);
            expected_to_succeed = true;
        } else {
            // otherwise, the only case that's going to succeed is
            // merging the first 10 slots (0 + 380 * 9) on the left,
            // and 2 slots on the right (0 + 200), and one from the parent (192)
            if (lpg_nslots == 10 && pkey_len < 380) {
                expected_to_succeed = true;
            } else {
                expected_to_succeed = false;
            }
        }
        ASSERT_EQ(merge_succeeded, expected_to_succeed);

        // unpin all these pages
        ASSERT_NO_ERROR(g_bufman->UnpinPage(pbufid));
        if (!merge_succeeded) {
            g_bufman->UnpinPage(lbufid_);
            g_bufman->UnpinPage(rbufid_);
        }

        // If the merge is successful, we flush the buffer pool in order to
        // check if the dirty bits are set correctly. Otherwise, we should not
        // flush the buffer pool, in case the pages are modified but the
        // dirty bits were not set.
        if (merge_succeeded) {
            ASSERT_NO_ERROR(ForceFlushBuffer());
        }

        if (merge_succeeded) {
            // check if the right page is freed
            rbufid = g_bufman->PinPage(rpid, &rbuf);
            PageHeaderData *rph = (PageHeaderData*) rbuf;
            ASSERT_FALSE(rph->IsAllocated())
                << "merge should free the right page upon succesful return";
            g_bufman->UnpinPage(rbufid);

            // check if the pid 0 page has the correct left sibling pointer
            bufid0 = g_bufman->PinPage(pid0, &buf0);
            pg0 = buf0;
            hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
            ASSERT_EQ(hdr0->m_prev_pid, lpid);
            ASSERT_EQ(hdr0->m_next_pid, INVALID_PID);
            ASSERT_FALSE(hdr0->IsRootPage());
            ASSERT_FALSE(hdr0->IsLeafPage());
            g_bufman->UnpinPage(bufid0);

            // check if the internal record pointing to the right page is
            // deleted from the parent page
            pbufid = g_bufman->PinPage(ppid, &pbuf);
            ppg = pbuf;
            // should just have one internal record left on the parent page
            ASSERT_EQ(ppg.GetMaxSlotId(), 1);
            ASSERT_EQ(ppg.GetRecord(1).GetLength(), BTreeInternalRecordHeaderSize);
            auto prechdr1 =
                (BTreeInternalRecordHeaderData*) ppg.GetRecordBuffer(1, nullptr);
            ASSERT_EQ(prechdr1->m_child_pid, lpid);
            auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
            ASSERT_EQ(phdr->m_totrlen, BTreeInternalRecordHeaderSize);
            ASSERT_FALSE(phdr->IsRootPage());
            ASSERT_FALSE(phdr->IsLeafPage());
            g_bufman->UnpinPage(pbufid);

            // check if the left page looks good
            lbufid = g_bufman->PinPage(lpid, &lbuf);
            lpg = lbuf;
            lhdr = (BTreePageHeaderData*) lpg.GetUserData();
            ASSERT_EQ(lhdr->m_prev_pid, INVALID_PID);
            ASSERT_EQ(lhdr->m_next_pid, pid0);
            ASSERT_FALSE(lhdr->IsLeafPage());
            ASSERT_FALSE(lhdr->IsRootPage());

            ASSERT_EQ(lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1,
                      lpg_nslots + rpg_nslots);
            VarlenDataPage lpg_old(lpg_copy);
            VarlenDataPage rpg_old(rpg_copy);
            VarlenDataPage ppg_old(ppg_copy);
            FieldOffset totrlen = 0;
            for (SlotId sid = lpg.GetMinSlotId(); sid <= lpg.GetMaxSlotId();
                    ++sid) {
                Record oldrec;
                maxaligned_char_buf recbuf;
                if (sid <= lpg_nslots) {
                    oldrec = lpg_old.GetRecord(sid);
                } else if (sid == lpg_nslots + 1) {
                    IndexKey *key = get_key_f2(13 ^ m_reorder_mask, pkey_len);
                    RecordId recid{
                        (PageNumber)(100 + 13), (SlotId)((13 ^ m_reorder_mask) + 1)};
                    maxaligned_char_buf recbuf_tmp;
                    CreateLeafRecord(m_idx_2, key, recid, recbuf_tmp);
                    CreateInternalRecord(m_idx_2, Record(recbuf_tmp),
                        /* child_pid = */(PageNumber)(10000 + 12
                            + n_to_truncate_on_right + 1),
                                         true, recbuf);
                    oldrec = Record(recbuf);
                    ASSERT_EQ(oldrec.GetLength(), pkey_len + 20);
                } else {
                    oldrec = rpg_old.GetRecord(sid - lpg_nslots
                                               + n_to_truncate_on_right);
                }
                Record newrec = lpg.GetRecord(sid);
                ASSERT_EQ(newrec.GetLength(), oldrec.GetLength()) << sid;

                auto oldrechdr =
                    (BTreeInternalRecordHeaderData*) oldrec.GetData();
                auto newrechdr =
                    (BTreeInternalRecordHeaderData*) newrec.GetData();
                ASSERT_EQ(newrechdr->m_child_pid, oldrechdr->m_child_pid)
                    << sid;

                if (sid != lpg.GetMinSlotId()) {
                    ASSERT_EQ(oldrechdr->m_heap_recid, newrechdr->m_heap_recid)
                        << sid;

                    absl::string_view oldkey =
                        read_f2_payload(oldrechdr->GetPayload());
                    absl::string_view newkey =
                        read_f2_payload(newrechdr->GetPayload());
                    ASSERT_EQ(newkey, oldkey) << sid;
                }

                totrlen += newrec.GetLength();
            }
            ASSERT_EQ(lhdr->m_totrlen, totrlen);
            g_bufman->UnpinPage(lbufid);
        } else {
            // check if the right page looks good
            rbufid = g_bufman->PinPage(rpid, &rbuf);
            PageHeaderData *rph = (PageHeaderData*) rbuf;
            ASSERT_TRUE(rph->IsAllocated())
                << "merge should not free the right page upon unsuccessful "
                   "return";
            rpg = rbuf;
            rhdr = (BTreePageHeaderData*) rpg.GetUserData();
            ASSERT_EQ(rhdr->m_prev_pid, lpid);
            ASSERT_EQ(rhdr->m_next_pid, pid0);
            ASSERT_FALSE(rhdr->IsLeafPage());
            ASSERT_FALSE(rhdr->IsRootPage());

            ASSERT_EQ(rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1,
                      rpg_nslots);
            VarlenDataPage rpg_old(rpg_copy);
            FieldOffset totrlen_right = 0;
            for (SlotId sid = rpg.GetMinSlotId(); sid <= rpg.GetMaxSlotId();
                    ++sid) {
                Record newrec = rpg.GetRecord(sid);
                Record oldrec = rpg_old.GetRecord(sid + n_to_truncate_on_right);

                auto oldrechdr =
                    (BTreeInternalRecordHeaderData*) oldrec.GetData();
                auto newrechdr =
                    (BTreeInternalRecordHeaderData*) newrec.GetData();
                ASSERT_EQ(newrechdr->m_child_pid, oldrechdr->m_child_pid)
                    << sid;

                if (sid != rpg.GetMinSlotId()) {
                    ASSERT_EQ(newrec.GetLength(), oldrec.GetLength());
                    ASSERT_EQ(oldrechdr->m_heap_recid, newrechdr->m_heap_recid)
                        << sid;

                    absl::string_view oldkey =
                        read_f2_payload(oldrechdr->GetPayload());
                    absl::string_view newkey =
                        read_f2_payload(newrechdr->GetPayload());
                    ASSERT_EQ(newkey, oldkey) << sid;
                } else {
                    ASSERT_EQ(newrec.GetLength(),
                              BTreeInternalRecordHeaderSize);
                }

                totrlen_right += newrec.GetLength();
            }
            ASSERT_EQ(rhdr->m_totrlen, totrlen_right);
            g_bufman->UnpinPage(rbufid);

            // check if the pid 0 page has the correct left sibling pointer
            bufid0 = g_bufman->PinPage(pid0, &buf0);
            pg0 = buf0;
            hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
            ASSERT_EQ(hdr0->m_prev_pid, rpid);
            ASSERT_EQ(hdr0->m_next_pid, INVALID_PID);
            ASSERT_FALSE(hdr0->IsRootPage());
            ASSERT_FALSE(hdr0->IsLeafPage());
            g_bufman->UnpinPage(bufid0);

            // check if the parent page still has two internal records
            pbufid = g_bufman->PinPage(ppid, &pbuf);
            ppg = pbuf;
            // should just have one internal record left on the parent page
            ASSERT_EQ(ppg.GetMaxSlotId(), 2);
            ASSERT_EQ(ppg.GetRecord(1).GetLength(),
                      BTreeInternalRecordHeaderSize);
            auto prechdr1 = (BTreeInternalRecordHeaderData*)
                ppg.GetRecordBuffer(1, nullptr);
            ASSERT_EQ(prechdr1->m_child_pid, lpid);

            ASSERT_EQ(ppg.GetRecord(2).GetLength(), pkey_len + 20);
            auto prechdr2 = (BTreeInternalRecordHeaderData*)
                ppg.GetRecordBuffer(2, nullptr);
            ASSERT_EQ(prechdr2->m_child_pid, rpid);
            ASSERT_EQ(prechdr2->m_heap_recid, ppg_rec2_recid);
            absl::string_view prec2_key =
                read_f2_payload(prechdr2->GetPayload());
            absl::string_view expected_prec2_key =
                get_skey(13 ^ m_reorder_mask, pkey_len);
            ASSERT_EQ(prec2_key, expected_prec2_key);

            auto phdr = (BTreePageHeaderData*) ppg.GetUserData();
            ASSERT_EQ(phdr->m_totrlen,
                      BTreeInternalRecordHeaderSize * 2 + pkey_len + 4);
            ASSERT_FALSE(phdr->IsRootPage());
            ASSERT_FALSE(phdr->IsLeafPage());
            g_bufman->UnpinPage(pbufid);

            // check if the left page looks good
            lbufid = g_bufman->PinPage(lpid, &lbuf);
            lpg = lbuf;
            lhdr = (BTreePageHeaderData*) lpg.GetUserData();
            ASSERT_EQ(lhdr->m_prev_pid, INVALID_PID);
            ASSERT_EQ(lhdr->m_next_pid, rpid);
            ASSERT_FALSE(lhdr->IsLeafPage());
            ASSERT_FALSE(lhdr->IsRootPage());

            ASSERT_EQ(lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1,
                      lpg_nslots);
            VarlenDataPage lpg_old(lpg_copy);
            FieldOffset totrlen_left = 0;
            for (SlotId sid = lpg.GetMinSlotId(); sid <= lpg.GetMaxSlotId();
                    ++sid) {
                Record oldrec = lpg_old.GetRecord(sid);
                Record newrec = lpg.GetRecord(sid);
                ASSERT_EQ(newrec.GetLength(), oldrec.GetLength()) << sid;

                auto oldrechdr = (BTreeInternalRecordHeaderData*)
                    oldrec.GetData();
                auto newrechdr = (BTreeInternalRecordHeaderData*)
                    newrec.GetData();
                ASSERT_EQ(newrechdr->m_child_pid, oldrechdr->m_child_pid)
                    << sid;

                if (sid != lpg.GetMinSlotId()) {
                    ASSERT_EQ(oldrechdr->m_heap_recid, newrechdr->m_heap_recid)
                        << sid;

                    absl::string_view oldkey =
                        read_f2_payload(oldrechdr->GetPayload());
                    absl::string_view newkey =
                        read_f2_payload(newrechdr->GetPayload());
                    ASSERT_EQ(newkey, oldkey) << sid;
                }

                totrlen_left += newrec.GetLength();
            }
            ASSERT_EQ(lhdr->m_totrlen, totrlen_left);
            g_bufman->UnpinPage(lbufid);
        }
    }

    TDB_TEST_END
}

static void
PurgeFirstNSlotsOnPage(char *pg, SlotId n) {
    // XXX a hard-coded layout of the VarlenDataPageHeader. This must change
    // if the struct changes in VarlenDataPage.cpp.
    struct Hdr {
        PageHeaderData m_ph;
        FieldOffset m_ph_sz;
        FieldOffset m_fs_begin;
        bool m_has_hole : 1;
        bool : 1;
        SlotId m_cnt : 14;
        SlotId : 2;
        SlotId m_nslots : 14;
    };
    Hdr *hdr = (Hdr*) pg;

    if (n == 0)
        return;

    ASSERT_LE(n, hdr->m_nslots);
    ASSERT_EQ(hdr->m_cnt, hdr->m_nslots);
    char *slot_array_src = pg + PAGE_SIZE - (size_t) hdr->m_nslots * 4;
    char *slot_array_tgt = pg + PAGE_SIZE - (size_t) (hdr->m_nslots - n) * 4;
    memmove(slot_array_tgt, slot_array_src, 4 * (hdr->m_nslots - n));
    hdr->m_nslots -= n;
    hdr->m_cnt -= n;
    // We don't attempt to meaningfully set m_has_hole here and will rely on
    // the VarlenDagaPage implementation to correctly compact the page!
    hdr->m_has_hole = true;
}

}    // namespace taco
