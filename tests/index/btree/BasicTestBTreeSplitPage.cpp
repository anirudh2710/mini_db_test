#define DEFAULT_BUFPOOL_SIZE 4
#include "index/btree/TestBTree.h"

namespace taco {

using BasicTestBTreeSplitPage = TestBTree;

// this test assumes page size is 4096
static_assert(PAGE_SIZE == 4096);

TEST_F(BasicTestBTreeSplitPage, TestSplitLeafPageWithUniformReclenSmallKey) {
    TDB_TEST_BEGIN

    // the leaf page in m_idx_1 will fit 202 leaf tuples but not 203
    ASSERT_EQ(BTreeComputePageUsage(202, 16 * 202), 4080);
    ASSERT_GT(BTreeComputePageUsage(203, 16 * 203), 4096);

    ScopedBufferId bufid0 =
        CreateNewBTreePage(m_idx_1, false, true, INVALID_PID, INVALID_PID);
    PageNumber pid0 = g_bufman->GetPageNumber(bufid0);
    char *buf0 = g_bufman->GetBuffer(bufid0);
    VarlenDataPage pg0(buf0);
    auto hdr0 = (BTreePageHeaderData *) pg0.GetUserData();

    // creates a new leaf page
    ScopedBufferId bufid =
        CreateNewBTreePage(m_idx_1, false, true, INVALID_PID, pid0);
    PageNumber pid = g_bufman->GetPageNumber(bufid);
    hdr0->m_prev_pid = pid;
    g_bufman->MarkDirty(bufid0);
    g_bufman->UnpinPage(bufid0);

    // inserts 1 to 202 into the page, inserting an additional record will fail
    char *buf = g_bufman->GetBuffer(bufid);
    VarlenDataPage pg(buf);
    auto hdr = (BTreePageHeaderData*) pg.GetUserData();
    ASSERT_EQ(hdr->m_totrlen, 0);
    maxaligned_char_buf recbuf;
    for (int64_t i = 1; i <= 203; ++i) {
        Datum d = Datum::From(i);
        IndexKey::Construct(m_index_key.get(), &d, 1);
        IndexKey *key = (IndexKey*) m_index_key.get();
        RecordId recid{(PageNumber)(100 + i), (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_1, key, recid, recbuf);
        Record rec(recbuf);
        ASSERT_EQ(recbuf.size(), 16);
        ASSERT_NO_ERROR(pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec));
        ASSERT_EQ(rec.GetRecordId().sid != INVALID_SID, i < 203)
            << "the " << i << "th insertion on the leaf page is expected to "
            << ((i < 203) ? "succeed" : "fail");
        if (i < 203) {
            hdr->m_totrlen += rec.GetLength();
        }
    }

    // make a copy of the original page so that we can test multiple insertion
    // point on the same page for several times
    alignas(8) char pg_copy[PAGE_SIZE];
    memcpy(pg_copy, buf, PAGE_SIZE);

    // drop the pins on bufid, we'll pin it in the loop
    g_bufman->MarkDirty(bufid);
    g_bufman->UnpinPage(bufid);

    // now test a few different insertion points
    constexpr SlotId expected_split_sid1 = 101;
    constexpr SlotId expected_split_sid2 = 102;

    auto get_osid =
    [&](SlotId insertion_sid, SlotId sid) -> SlotId {
        if (sid < insertion_sid) {
            return sid;
        } else if (sid == insertion_sid) {
            return INVALID_SID;
        } else {
            return sid - 1;
        }
    };

    for (SlotId insertion_sid : { 1, 60, 100, 101, 102, 103, 160, 202, 203 }) {
        // first force a buffer flush since we might have marked the page
        // as dirty
        ASSERT_NO_ERROR(ForceFlushBuffer());
        ScopedBufferId bufid = g_bufman->PinPage(pid, &buf);
        VarlenDataPage lpg(buf);

        Datum d = Datum::From((int64_t) insertion_sid);
        IndexKey::Construct(m_index_key.get(), &d, 1);
        IndexKey *key = (IndexKey*) m_index_key.get();
        RecordId recid{100, INVALID_SID};
        maxaligned_char_buf idxrecbuf;
        ASSERT_NO_ERROR(CreateLeafRecord(m_idx_1, key, recid, idxrecbuf));

        maxaligned_char_buf sepkey_rec =
            SplitPage(m_idx_1, bufid, insertion_sid, std::move(idxrecbuf));
        auto sepkey_hdr = (BTreeInternalRecordHeaderData*) sepkey_rec.data();

        // flush the buffer again and pin the left page
        g_bufman->UnpinPage(bufid);
        ASSERT_NO_ERROR(ForceFlushBuffer());
        bufid = g_bufman->PinPage(pid, &buf);
        lpg = buf;

        // check header updates in the left page
        auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();
        ASSERT_EQ(lhdr->m_prev_pid, INVALID_PID);
        ASSERT_FALSE(lhdr->IsRootPage());
        ASSERT_TRUE(lhdr->IsLeafPage());

        // the split sid must be either 101 or 102 (including the new rec)
        // check the record id first (which uniquely identifies one tuple)
        SlotId osid_alt1 = get_osid(insertion_sid, expected_split_sid1 + 1);
        SlotId osid_alt2 = get_osid(insertion_sid, expected_split_sid2 + 1);

        RecordId recid_alt1{(PageNumber)(100 + osid_alt1),
                            (osid_alt1 == INVALID_SID) ? INVALID_SID :
                                (SlotId)((osid_alt1 ^ m_reorder_mask) + 1)};
        RecordId recid_alt2{(PageNumber)(100 + osid_alt2),
                            (osid_alt2 == INVALID_SID) ? INVALID_SID :
                                (SlotId)((osid_alt2 ^ m_reorder_mask) + 1)};
        ASSERT_TRUE(EqualsToOneOf(sepkey_hdr->m_heap_recid, recid_alt1,
                                                            recid_alt2))
            << insertion_sid;

        // check the key payload
        SlotId split_sid = (sepkey_hdr->m_heap_recid == recid_alt1)
            ? expected_split_sid1 : expected_split_sid2;
        SlotId osid = get_osid(insertion_sid, split_sid + 1);
        int64_t expected_sepkey = (osid == INVALID_SID) ? insertion_sid : osid;
        int64_t sepkey = read_f1_payload(sepkey_hdr->GetPayload());
        ASSERT_EQ(sepkey, expected_sepkey);

        // check the child pid
        PageNumber rpid = sepkey_hdr->m_child_pid;
        ASSERT_NE(rpid, INVALID_PID);
        ASSERT_EQ(lhdr->m_next_pid, rpid);

        // make sure the old right page's left sibling link has been updated
        bufid0 = g_bufman->PinPage(pid0, &buf0);
        VarlenDataPage pg0(buf0);
        auto hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
        ASSERT_EQ(hdr0->m_prev_pid, rpid);
        ASSERT_EQ(hdr0->m_next_pid, INVALID_PID);

        // recover the old right page's left sibling for next iteration
        hdr0->m_prev_pid = pid;
        g_bufman->MarkDirty(bufid0);
        g_bufman->UnpinPage(bufid0);

        // try to pin the new right sibling
        char *rbuf;
        ScopedBufferId rbufid = g_bufman->PinPage(rpid, &rbuf);
        VarlenDataPage rpg(rbuf);
        auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
        ASSERT_TRUE(rhdr->IsLeafPage());
        ASSERT_FALSE(rhdr->IsRootPage());
        ASSERT_EQ(rhdr->m_prev_pid, pid);
        ASSERT_EQ(rhdr->m_next_pid, pid0);

        // check number of records on each page
        SlotId expected_num_recs_left = split_sid;
        SlotId expected_num_recs_right = 203 - split_sid;
        SlotId num_recs_left = lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1;
        SlotId num_recs_right = rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1;
        ASSERT_EQ(num_recs_left, expected_num_recs_left)  << insertion_sid;
        ASSERT_EQ(num_recs_right, expected_num_recs_right) << insertion_sid;

        // check left and right page payload
        FieldOffset totrlen_left = 0,
                    totrlen_right = 0;
        for (SlotId sid = 1; sid <= 203; ++sid) {
            Record rec;
            if (sid <= split_sid) {
                rec = lpg.GetRecord(sid);
                totrlen_left += rec.GetLength();
            } else {
                rec = rpg.GetRecord(sid - split_sid);
                totrlen_right += rec.GetLength();
            }
            ASSERT_EQ(rec.GetLength(), 16);
            auto rechdr = (BTreeLeafRecordHeaderData*) rec.GetData();

            SlotId osid = get_osid(insertion_sid, sid);
            RecordId expected_recid{
                (PageNumber)(100 + osid),
                (osid == INVALID_SID) ? INVALID_SID :
                    (SlotId)((osid ^ m_reorder_mask) + 1)
            };
            ASSERT_EQ(rechdr->m_recid, expected_recid) << sid;

            int64_t key = read_f1_payload(rechdr->GetPayload());
            int64_t expected_key = (osid == INVALID_SID) ? insertion_sid : osid;
            ASSERT_EQ(key, expected_key) << sid;
        }

        // check totrlen
        ASSERT_EQ(lhdr->m_totrlen, totrlen_left);
        ASSERT_EQ(rhdr->m_totrlen, totrlen_right);

        // recover leftmost page for next iteration
        memcpy(buf, pg_copy, PAGE_SIZE);
        g_bufman->MarkDirty(bufid);
    }

    TDB_TEST_END
}

TEST_F(BasicTestBTreeSplitPage, TestSplitLeafPageWithUniformReclenLargeKey) {
    TDB_TEST_BEGIN

    // the leaf page in m_idx_2 will fit 10 leaf tuples but not 11
    ASSERT_EQ(BTreeComputePageUsage(10, 392 * 10), 4000);
    ASSERT_GT(BTreeComputePageUsage(11, 392 * 11), 4096);

    ScopedBufferId bufid0 =
        CreateNewBTreePage(m_idx_2, false, true, INVALID_PID, INVALID_PID);
    PageNumber pid0 = g_bufman->GetPageNumber(bufid0);
    char *buf0 = g_bufman->GetBuffer(bufid0);
    VarlenDataPage pg0(buf0);
    auto hdr0 = (BTreePageHeaderData *) pg0.GetUserData();

    // creates a new leaf page
    ScopedBufferId bufid =
        CreateNewBTreePage(m_idx_2, false, true, INVALID_PID, pid0);
    PageNumber pid = g_bufman->GetPageNumber(bufid);
    hdr0->m_prev_pid = pid;
    g_bufman->MarkDirty(bufid0);
    g_bufman->UnpinPage(bufid0);

    // inserts 1 to 10 into the page, inserting an additional record will fail
    char *buf = g_bufman->GetBuffer(bufid);
    VarlenDataPage pg(buf);
    auto hdr = (BTreePageHeaderData*) pg.GetUserData();
    ASSERT_EQ(hdr->m_totrlen, 0);
    maxaligned_char_buf recbuf;
    for (int64_t i = 1; i <= 11; ++i) {
        snprintf(&m_strbuf[0], 11, "0x%08lx", i);
        m_strbuf[10] = 'A';
        Datum d = Datum::FromVarlenAsStringView(m_strbuf);
        IndexKey::Construct(m_index_key.get(), &d, 1);
        IndexKey *key = (IndexKey*) m_index_key.get();
        RecordId recid{(PageNumber)(100 + i), (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_2, key, recid, recbuf);
        Record rec(recbuf);
        ASSERT_EQ(recbuf.size(), 392);
        ASSERT_NO_ERROR(pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec));
        ASSERT_EQ(rec.GetRecordId().sid != INVALID_SID, i < 11)
            << "the " << i << "th insertion on the leaf page is expected to "
            << ((i < 11) ? "succeed" : "fail");
        if (i < 11) {
            hdr->m_totrlen += rec.GetLength();
        }
    }

    // make a copy of the original page so that we can test multiple insertion
    // point on the same page for several times
    alignas(8) char pg_copy[PAGE_SIZE];
    memcpy(pg_copy, buf, PAGE_SIZE);

    // drop the pin here and start the tests
    g_bufman->MarkDirty(bufid);
    g_bufman->UnpinPage(bufid);

    // now test a few different insertion points
    constexpr SlotId expected_split_sid1 = 5;
    constexpr SlotId expected_split_sid2 = 6;

    auto get_osid =
    [&](SlotId insertion_sid, SlotId sid) -> SlotId {
        if (sid < insertion_sid) {
            return sid;
        } else if (sid == insertion_sid) {
            return INVALID_SID;
        } else {
            return sid - 1;
        }
    };

    for (SlotId insertion_sid : { 1, 3, 4, 5, 6, 7, 10, 11 }) {
        // force a buffer flush to reset the dirty bits
        ASSERT_NO_ERROR(ForceFlushBuffer());
        ScopedBufferId bufid = g_bufman->PinPage(pid, &buf);
        VarlenDataPage lpg(buf);

        snprintf(&m_strbuf[0], 11, "0x%08lx", (int64_t) insertion_sid);
        m_strbuf[10] = 'A';
        Datum d = Datum::FromVarlenAsStringView(m_strbuf);
        IndexKey::Construct(m_index_key.get(), &d, 1);
        IndexKey *key = (IndexKey*) m_index_key.get();
        RecordId recid{100, INVALID_SID};
        maxaligned_char_buf idxrecbuf;
        ASSERT_NO_ERROR(CreateLeafRecord(m_idx_2, key, recid, idxrecbuf));

        maxaligned_char_buf sepkey_rec =
            SplitPage(m_idx_2, bufid, insertion_sid, std::move(idxrecbuf));
        auto sepkey_hdr = (BTreeInternalRecordHeaderData*) sepkey_rec.data();

        // flush the buffer and pin the left page again to make sure
        // the dirty bits are correctly set
        g_bufman->UnpinPage(bufid);
        ASSERT_NO_ERROR(ForceFlushBuffer());
        bufid = g_bufman->PinPage(pid, &buf);
        lpg = buf;

        // check header updates in the left page
        auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();
        ASSERT_EQ(lhdr->m_prev_pid, INVALID_PID);
        ASSERT_FALSE(lhdr->IsRootPage());
        ASSERT_TRUE(lhdr->IsLeafPage());

        // the split sid must be either 101 or 102 (including the new rec)
        // check the record id first (which uniquely identifies one tuple)
        SlotId osid_alt1 = get_osid(insertion_sid, expected_split_sid1 + 1);
        SlotId osid_alt2 = get_osid(insertion_sid, expected_split_sid2 + 1);

        RecordId recid_alt1{(PageNumber)(100 + osid_alt1),
                            (osid_alt1 == INVALID_SID) ? INVALID_SID :
                                (SlotId)((osid_alt1 ^ m_reorder_mask) + 1)};
        RecordId recid_alt2{(PageNumber)(100 + osid_alt2),
                            (osid_alt2 == INVALID_SID) ? INVALID_SID :
                                (SlotId)((osid_alt2 ^ m_reorder_mask) + 1)};
        ASSERT_TRUE(EqualsToOneOf(sepkey_hdr->m_heap_recid, recid_alt1,
                                                            recid_alt2))
            << insertion_sid;

        // check the key payload
        SlotId split_sid = (sepkey_hdr->m_heap_recid == recid_alt1)
            ? expected_split_sid1 : expected_split_sid2;
        SlotId osid = get_osid(insertion_sid, split_sid + 1);
        int64_t expected_sepkey_i = (osid == INVALID_SID) ? insertion_sid : osid;
        snprintf(&m_strbuf[0], 11, "0x%08lx", expected_sepkey_i);
        m_strbuf[10] = 'A';
        absl::string_view expected_sepkey = m_strbuf;
        absl::string_view sepkey = read_f2_payload(sepkey_hdr->GetPayload());
        ASSERT_EQ(sepkey, expected_sepkey);

        // check the child pid
        PageNumber rpid = sepkey_hdr->m_child_pid;
        ASSERT_NE(rpid, INVALID_PID);
        ASSERT_EQ(lhdr->m_next_pid, rpid);

        // make sure the old right page's left sibling link has been updated
        bufid0 = g_bufman->PinPage(pid0, &buf0);
        VarlenDataPage pg0(buf0);
        auto hdr0 = (BTreePageHeaderData*) pg0.GetUserData();
        ASSERT_EQ(hdr0->m_prev_pid, rpid);
        ASSERT_EQ(hdr0->m_next_pid, INVALID_PID);

        // recover the old right page's left sibling for next iteration
        hdr0->m_prev_pid = pid;
        g_bufman->MarkDirty(bufid0);
        g_bufman->UnpinPage(bufid0);

        // try to pin the new right sibling
        char *rbuf;
        ScopedBufferId rbufid = g_bufman->PinPage(rpid, &rbuf);
        VarlenDataPage rpg(rbuf);
        auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
        ASSERT_TRUE(rhdr->IsLeafPage());
        ASSERT_FALSE(rhdr->IsRootPage());
        ASSERT_EQ(rhdr->m_prev_pid, pid);
        ASSERT_EQ(rhdr->m_next_pid, pid0);

        // check number of records on each page
        SlotId expected_num_recs_left = split_sid;
        SlotId expected_num_recs_right = 11 - split_sid;
        SlotId num_recs_left = lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1;
        SlotId num_recs_right = rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1;
        ASSERT_EQ(num_recs_left, expected_num_recs_left)  << insertion_sid;
        ASSERT_EQ(num_recs_right, expected_num_recs_right) << insertion_sid;

        // check left and right page payload
        FieldOffset totrlen_left = 0,
                    totrlen_right = 0;
        for (SlotId sid = 1; sid <= 11; ++sid) {
            Record rec;
            if (sid <= split_sid) {
                rec = lpg.GetRecord(sid);
                totrlen_left += rec.GetLength();
            } else {
                rec = rpg.GetRecord(sid - split_sid);
                totrlen_right += rec.GetLength();
            }
            ASSERT_EQ(rec.GetLength(), 392);
            auto rechdr = (BTreeLeafRecordHeaderData*) rec.GetData();

            SlotId osid = get_osid(insertion_sid, sid);
            RecordId expected_recid{
                (PageNumber)(100 + osid),
                (osid == INVALID_SID) ? INVALID_SID :
                    (SlotId)((osid ^ m_reorder_mask) + 1)
            };
            ASSERT_EQ(rechdr->m_recid, expected_recid) << sid;

            int64_t expected_key_i = (osid == INVALID_SID) ? insertion_sid : osid;
            snprintf(&m_strbuf[0], 11, "0x%08lx", expected_key_i);
            m_strbuf[10] = 'A';
            absl::string_view expected_key = m_strbuf;
            absl::string_view key = read_f2_payload(rechdr->GetPayload());
            ASSERT_EQ(key, expected_key) << sid;
        }

        // check totrlen
        ASSERT_EQ(lhdr->m_totrlen, totrlen_left);
        ASSERT_EQ(rhdr->m_totrlen, totrlen_right);

        // recover leftmost page for next iteration
        memcpy(buf, pg_copy, PAGE_SIZE);
        g_bufman->MarkDirty(bufid);
    }

    TDB_TEST_END
}

TEST_F(BasicTestBTreeSplitPage, TestSplitInternalPageWithUniformReclenSmallKey) {
    TDB_TEST_BEGIN

    // the internal page in m_idx_1 will fit 145 internal tuples but not 146
    ASSERT_EQ(BTreeComputePageUsage(145, 24 * 144 + 16), 4092);
    ASSERT_GT(BTreeComputePageUsage(146, 24 * 145 + 16), 4096);

    // creates a new internal page
    ScopedBufferId bufid =
        CreateNewBTreePage(m_idx_1, true, false, INVALID_PID, INVALID_PID);
    PageNumber pid = g_bufman->GetPageNumber(bufid);

    // inserts 1 to 145 into the page, inserting an additional record will fail
    char *buf = g_bufman->GetBuffer(bufid);
    VarlenDataPage pg(buf);
    auto hdr = (BTreePageHeaderData*) pg.GetUserData();
    ASSERT_EQ(hdr->m_totrlen, 0);
    maxaligned_char_buf recbuf;
    for (int64_t i = 1; i <= 146; ++i) {
        maxaligned_char_buf recbuf_tmp;
        Datum d = Datum::From(i);
        IndexKey::Construct(m_index_key.get(), &d, 1);
        IndexKey *key = (IndexKey*) m_index_key.get();
        RecordId recid{(PageNumber)(100 + i), (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_1, key, recid, recbuf_tmp);
        CreateInternalRecord(m_idx_1, Record(recbuf_tmp),
                             (PageNumber)(i + 10000),
                             true, recbuf);
        Record rec(recbuf);
        ASSERT_EQ(recbuf.size(), 24);
        if (i == 1) {
            rec.GetLength() = 16;
        }
        ASSERT_NO_ERROR(pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec));
        ASSERT_EQ(rec.GetRecordId().sid != INVALID_SID, i < 146)
            << "the " << i << "th insertion on the leaf page is expected to 146"
            << ((i < 146) ? "succeed" : "fail");
        if (i < 146) {
            hdr->m_totrlen += rec.GetLength();
        }
    }

    // make a copy of the original page so that we can test multiple insertion
    // point on the same page for several times
    alignas(8) char pg_copy[PAGE_SIZE];
    memcpy(pg_copy, buf, PAGE_SIZE);

    // unpin the page and start the tests
    g_bufman->MarkDirty(bufid);
    g_bufman->UnpinPage(bufid);

    // now test a few different insertion points
    constexpr SlotId expected_split_sid = 73;

    auto get_osid =
    [&](SlotId insertion_sid, SlotId sid) -> SlotId {
        if (sid < insertion_sid) {
            return sid;
        } else if (sid == insertion_sid) {
            return INVALID_SID;
        } else {
            return sid - 1;
        }
    };

    for (SlotId insertion_sid : { 2, 40, 72, 73, 74, 75, 110, 145, 146 }) {
        // force a flush here to reset the dirty bits
        ASSERT_NO_ERROR(ForceFlushBuffer());
        ScopedBufferId bufid = g_bufman->PinPage(pid, &buf);
        VarlenDataPage lpg(buf);

        maxaligned_char_buf idxrecbuf;
        maxaligned_char_buf recbuf_tmp;
        Datum d = Datum::From((int64_t) insertion_sid);
        IndexKey::Construct(m_index_key.get(), &d, 1);
        IndexKey *key = (IndexKey*) m_index_key.get();
        RecordId recid{100, INVALID_SID};
        CreateLeafRecord(m_idx_1, key, recid, recbuf_tmp);
        CreateInternalRecord(m_idx_1, Record(recbuf_tmp),
                             10000,
                             true, idxrecbuf);

        maxaligned_char_buf sepkey_rec =
            SplitPage(m_idx_1, bufid, insertion_sid, std::move(idxrecbuf));
        auto sepkey_hdr = (BTreeInternalRecordHeaderData*) sepkey_rec.data();

        // flush the buffer and pin the left page again to make sure the
        // dirty bits are set
        g_bufman->UnpinPage(bufid);
        ASSERT_NO_ERROR(ForceFlushBuffer());
        bufid = g_bufman->PinPage(pid, &buf);
        lpg = buf;

        // check header updates in the left page
        auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();
        ASSERT_EQ(lhdr->m_prev_pid, INVALID_PID);
        ASSERT_FALSE(lhdr->IsRootPage());
        ASSERT_FALSE(lhdr->IsLeafPage());

        // the split sid must be either 101 or 102 (including the new rec)
        // check the record id first (which uniquely identifies one tuple)
        SlotId expected_osid= get_osid(insertion_sid, expected_split_sid + 1);

        RecordId expected_recid{(PageNumber)(100 + expected_osid),
                            (expected_osid == INVALID_SID) ? INVALID_SID :
                                (SlotId)((expected_osid ^ m_reorder_mask) + 1)};
        ASSERT_EQ(sepkey_hdr->m_heap_recid, expected_recid)
            << insertion_sid;

        // check the key payload
        SlotId split_sid = expected_split_sid;
        SlotId osid = get_osid(insertion_sid, split_sid + 1);
        int64_t expected_sepkey = (osid == INVALID_SID) ? insertion_sid : osid;
        int64_t sepkey = read_f1_payload(sepkey_hdr->GetPayload());
        ASSERT_EQ(sepkey, expected_sepkey);

        // check the child pid
        PageNumber rpid = sepkey_hdr->m_child_pid;
        ASSERT_NE(rpid, INVALID_PID);
        ASSERT_EQ(lhdr->m_next_pid, rpid);

        // try to pin the new right sibling
        char *rbuf;
        ScopedBufferId rbufid = g_bufman->PinPage(rpid, &rbuf);
        VarlenDataPage rpg(rbuf);
        auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
        ASSERT_FALSE(rhdr->IsLeafPage());
        ASSERT_FALSE(rhdr->IsRootPage());
        ASSERT_EQ(rhdr->m_prev_pid, pid);
        ASSERT_EQ(rhdr->m_next_pid, INVALID_PID);

        // check number of records on each page
        SlotId expected_num_recs_left = split_sid;
        SlotId expected_num_recs_right = 146 - split_sid;
        SlotId num_recs_left = lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1;
        SlotId num_recs_right = rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1;
        ASSERT_EQ(num_recs_left, expected_num_recs_left)  << insertion_sid;
        ASSERT_EQ(num_recs_right, expected_num_recs_right) << insertion_sid;

        // check left and right page payload
        FieldOffset totrlen_left = 0,
                    totrlen_right = 0;
        for (SlotId sid = 1; sid <= 146; ++sid) {
            Record rec;
            if (sid <= split_sid) {
                rec = lpg.GetRecord(sid);
                if (sid == 1) {
                    ASSERT_EQ(rec.GetLength(), BTreeInternalRecordHeaderSize);
                } else {
                    ASSERT_EQ(rec.GetLength(), 24);
                }
                totrlen_left += rec.GetLength();
            } else {
                rec = rpg.GetRecord(sid - split_sid);
                if (sid == split_sid + 1) {
                    ASSERT_EQ(rec.GetLength(), BTreeInternalRecordHeaderSize);
                } else {
                    ASSERT_EQ(rec.GetLength(), 24);
                }
                totrlen_right += rec.GetLength();
            }
            auto rechdr = (BTreeInternalRecordHeaderData*) rec.GetData();

            SlotId osid = get_osid(insertion_sid, sid);
            if (sid != 1 && sid != split_sid + 1) {
                // skip testing the heap recid if this is the first internal
                // record on either of the page, which should be undefined
                RecordId expected_recid{
                    (PageNumber)(100 + osid),
                    (osid == INVALID_SID) ? INVALID_SID :
                        (SlotId)((osid ^ m_reorder_mask) + 1)
                };
                ASSERT_EQ(rechdr->m_heap_recid, expected_recid) << sid;
            }

            PageNumber expected_child_pid = 10000 + osid;
            ASSERT_EQ(rechdr->m_child_pid, expected_child_pid);

            if (sid != 1 && sid != split_sid + 1) {
                int64_t key = read_f1_payload(rechdr->GetPayload());
                int64_t expected_key = (osid == INVALID_SID) ? insertion_sid : osid;
                ASSERT_EQ(key, expected_key) << sid;
            }
        }

        // check totrlen
        ASSERT_EQ(lhdr->m_totrlen, totrlen_left) << insertion_sid;
        ASSERT_EQ(rhdr->m_totrlen, totrlen_right) << insertion_sid;

        // recover leftmost page for next iteration
        memcpy(buf, pg_copy, PAGE_SIZE);
        g_bufman->MarkDirty(bufid);
    }

    TDB_TEST_END
}

TEST_F(BasicTestBTreeSplitPage, TestSplitInternalPageWithUniformReclenLargeKey) {
    TDB_TEST_BEGIN

    // the internal page in m_idx_2 will fit 10 internal tuples but not 11
    ASSERT_EQ(BTreeComputePageUsage(10, 400 * 9 + 16), 3696);
    ASSERT_GT(BTreeComputePageUsage(11, 400 * 10 + 16), 4096);

    // creates a new leaf page
    ScopedBufferId bufid =
        CreateNewBTreePage(m_idx_2, true, false, INVALID_PID, INVALID_PID);
    PageNumber pid = g_bufman->GetPageNumber(bufid);

    // inserts 1 to 10 into the page, inserting an additional record will fail
    char *buf = g_bufman->GetBuffer(bufid);
    VarlenDataPage pg(buf);
    auto hdr = (BTreePageHeaderData*) pg.GetUserData();
    ASSERT_EQ(hdr->m_totrlen, 0);
    maxaligned_char_buf recbuf;
    for (int64_t i = 1; i <= 11; ++i) {
        maxaligned_char_buf recbuf_tmp;
        snprintf(&m_strbuf[0], 11, "0x%08lx", i);
        m_strbuf[10] = 'A';
        Datum d = Datum::FromVarlenAsStringView(m_strbuf);
        IndexKey::Construct(m_index_key.get(), &d, 1);
        IndexKey *key = (IndexKey*) m_index_key.get();
        RecordId recid{(PageNumber)(100 + i), (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_2, key, recid, recbuf_tmp);
        CreateInternalRecord(m_idx_2, Record(recbuf_tmp),
                             (PageNumber)(i + 10000),
                             true, recbuf);
        Record rec(recbuf);
        ASSERT_EQ(recbuf.size(), 400);
        if (i == 1) {
            rec.GetLength() = 16;
        }
        ASSERT_NO_ERROR(pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec));
        ASSERT_EQ(rec.GetRecordId().sid != INVALID_SID, i < 11)
            << "the " << i << "th insertion on the leaf page is expected to "
            << ((i < 11) ? "succeed" : "fail");
        if (i < 11) {
            hdr->m_totrlen += rec.GetLength();
        }
    }

    // make a copy of the original page so that we can test multiple insertion
    // point on the same page for several times
    alignas(8) char pg_copy[PAGE_SIZE];
    memcpy(pg_copy, buf, PAGE_SIZE);

    // drop the pin and start the tests
    g_bufman->MarkDirty(bufid);
    g_bufman->UnpinPage(bufid);

    // now test a few different insertion points
    constexpr SlotId expected_split_sid1 = 5;
    constexpr SlotId expected_split_sid2 = 6;

    auto get_osid =
    [&](SlotId insertion_sid, SlotId sid) -> SlotId {
        if (sid < insertion_sid) {
            return sid;
        } else if (sid == insertion_sid) {
            return INVALID_SID;
        } else {
            return sid - 1;
        }
    };

    for (SlotId insertion_sid : { 2, 3, 4, 5, 6, 7, 10, 11 }) {
        // force a flush here to reset the dirty bits
        ASSERT_NO_ERROR(ForceFlushBuffer());
        ScopedBufferId bufid = g_bufman->PinPage(pid, &buf);
        VarlenDataPage lpg(buf);

        snprintf(&m_strbuf[0], 11, "0x%08lx", (int64_t) insertion_sid);
        m_strbuf[10] = 'A';
        Datum d = Datum::FromVarlenAsStringView(m_strbuf);
        IndexKey::Construct(m_index_key.get(), &d, 1);
        IndexKey *key = (IndexKey*) m_index_key.get();
        RecordId recid{100, INVALID_SID};
        maxaligned_char_buf idxrecbuf_tmp;
        maxaligned_char_buf idxrecbuf;
        ASSERT_NO_ERROR(CreateLeafRecord(m_idx_2, key, recid, idxrecbuf_tmp));
        CreateInternalRecord(m_idx_2, Record(idxrecbuf_tmp),
                             10000,
                             true, idxrecbuf);

        maxaligned_char_buf sepkey_rec =
            SplitPage(m_idx_2, bufid, insertion_sid, std::move(idxrecbuf));
        auto sepkey_hdr = (BTreeInternalRecordHeaderData*) sepkey_rec.data();

        // flush the buffer and pin the left page again to make sure the dirty
        // bits are correctly set
        g_bufman->UnpinPage(bufid);
        ASSERT_NO_ERROR(ForceFlushBuffer());
        bufid = g_bufman->PinPage(pid, &buf);
        lpg = buf;

        // check header updates in the left page
        auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();
        ASSERT_EQ(lhdr->m_prev_pid, INVALID_PID);
        ASSERT_FALSE(lhdr->IsRootPage());
        ASSERT_FALSE(lhdr->IsLeafPage());

        // the split sid must be either 101 or 102 (including the new rec)
        // check the record id first (which uniquely identifies one tuple)
        SlotId osid_alt1 = get_osid(insertion_sid, expected_split_sid1 + 1);
        SlotId osid_alt2 = get_osid(insertion_sid, expected_split_sid2 + 1);

        RecordId recid_alt1{(PageNumber)(100 + osid_alt1),
                            (osid_alt1 == INVALID_SID) ? INVALID_SID :
                                (SlotId)((osid_alt1 ^ m_reorder_mask) + 1)};
        RecordId recid_alt2{(PageNumber)(100 + osid_alt2),
                            (osid_alt2 == INVALID_SID) ? INVALID_SID :
                                (SlotId)((osid_alt2 ^ m_reorder_mask) + 1)};
        ASSERT_TRUE(EqualsToOneOf(sepkey_hdr->m_heap_recid, recid_alt1,
                                                            recid_alt2))
            << insertion_sid;

        // check the key payload
        SlotId split_sid = (sepkey_hdr->m_heap_recid == recid_alt1)
            ? expected_split_sid1 : expected_split_sid2;
        SlotId osid = get_osid(insertion_sid, split_sid + 1);
        int64_t expected_sepkey_i = (osid == INVALID_SID) ? insertion_sid : osid;
        snprintf(&m_strbuf[0], 11, "0x%08lx", expected_sepkey_i);
        m_strbuf[10] = 'A';
        absl::string_view expected_sepkey = m_strbuf;
        absl::string_view sepkey = read_f2_payload(sepkey_hdr->GetPayload());
        ASSERT_EQ(sepkey, expected_sepkey);

        // check the child pid
        PageNumber rpid = sepkey_hdr->m_child_pid;
        ASSERT_NE(rpid, INVALID_PID);
        ASSERT_EQ(lhdr->m_next_pid, rpid);

        // try to pin the new right sibling
        char *rbuf;
        ScopedBufferId rbufid = g_bufman->PinPage(rpid, &rbuf);
        VarlenDataPage rpg(rbuf);
        auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
        ASSERT_FALSE(rhdr->IsLeafPage());
        ASSERT_FALSE(rhdr->IsRootPage());
        ASSERT_EQ(rhdr->m_prev_pid, pid);
        ASSERT_EQ(rhdr->m_next_pid, INVALID_PID);

        // check number of records on each page
        SlotId expected_num_recs_left = split_sid;
        SlotId expected_num_recs_right = 11 - split_sid;
        SlotId num_recs_left = lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1;
        SlotId num_recs_right = rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1;
        ASSERT_EQ(num_recs_left, expected_num_recs_left)  << insertion_sid;
        ASSERT_EQ(num_recs_right, expected_num_recs_right) << insertion_sid;

        // check left and right page payload
        FieldOffset totrlen_left = 0,
                    totrlen_right = 0;
        for (SlotId sid = 1; sid <= 11; ++sid) {
            Record rec;
            if (sid <= split_sid) {
                rec = lpg.GetRecord(sid);
                if (sid == 1) {
                    ASSERT_EQ(rec.GetLength(), BTreeInternalRecordHeaderSize);
                } else {
                    ASSERT_EQ(rec.GetLength(), 400);
                }
                totrlen_left += rec.GetLength();
            } else {
                rec = rpg.GetRecord(sid - split_sid);
                if (sid == split_sid + 1) {
                    ASSERT_EQ(rec.GetLength(), BTreeInternalRecordHeaderSize);
                } else {
                    ASSERT_EQ(rec.GetLength(), 400);
                }
                totrlen_right += rec.GetLength();
            }
            auto rechdr = (BTreeInternalRecordHeaderData*) rec.GetData();

            SlotId osid = get_osid(insertion_sid, sid);
            if (sid != 1 && sid != split_sid + 1) {
                // skip testing the heap recid if this is the first internal
                // record on either of the page, which should be undefined
                RecordId expected_recid{
                    (PageNumber)(100 + osid),
                    (osid == INVALID_SID) ? INVALID_SID :
                        (SlotId)((osid ^ m_reorder_mask) + 1)
                };
                ASSERT_EQ(rechdr->m_heap_recid, expected_recid) << sid;
            }

            PageNumber expected_child_pid = 10000 + osid;
            ASSERT_EQ(rechdr->m_child_pid, expected_child_pid);

            if (sid != 1 && sid != split_sid + 1) {
                int64_t expected_key_i = (osid == INVALID_SID) ? insertion_sid : osid;
                snprintf(&m_strbuf[0], 11, "0x%08lx", expected_key_i);
                m_strbuf[10] = 'A';
                absl::string_view expected_key = m_strbuf;
                absl::string_view key = read_f2_payload(rechdr->GetPayload());
                ASSERT_EQ(key, expected_key) << sid;
            }
        }

        // check totrlen
        ASSERT_EQ(lhdr->m_totrlen, totrlen_left);
        ASSERT_EQ(rhdr->m_totrlen, totrlen_right);

        // recover leftmost page for next iteration
        memcpy(buf, pg_copy, PAGE_SIZE);
        g_bufman->MarkDirty(bufid);
    }

    TDB_TEST_END
}

}    // namespace taco
