#define DEFAULT_BUFPOOL_SIZE 100
#include "index/btree/TestBTree.h"
#include "utils/fsutils.h"

ABSL_FLAG(std::string, db_path, "",
        "set this to override the default db path that contains "
        "the required preloaded b-tree");

namespace taco {

using BasicTestBTreeDelete = TestBTree;

TEST_F(BasicTestBTreeDelete, TestBTreeDeleteFindKey) {
    TDB_TEST_BEGIN
    // creates a new leaf page
    BufferId bufid =
        CreateNewBTreePage(m_idx_1, false, true, INVALID_PID, INVALID_PID);
    char *buf = g_bufman->GetBuffer(bufid);
    BufferId ref_bufid = bufid;

    VarlenDataPage pg(buf);
    maxaligned_char_buf recbuf;
    for (int64_t i = 1; i <= 202; ++i) {
        if (i == 23 || i == 87) continue;
        IndexKey *key = get_key_f1((i * 2 - 1) ^ m_reorder_mask);
        RecordId recid{100, (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_1, key, recid, recbuf);
        Record rec(recbuf);
        ASSERT_NO_ERROR(pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec));
        if (i == 59) {
            recid.sid += 2;
            CreateLeafRecord(m_idx_1, key, recid, recbuf);
            Record rec2(recbuf);
            ASSERT_NO_ERROR(pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec2));
            recid.sid += 3;
            CreateLeafRecord(m_idx_1, key, recid, recbuf);
            Record rec3(recbuf);
            ASSERT_NO_ERROR(pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec3));
        }
    }

    std::vector<PathItem> path = {g_bufman->GetPageNumber(bufid)};
    {
        IndexKey *key = get_key_f1(167 ^ m_reorder_mask);
        RecordId recid{0, 0};
        ASSERT_EQ(FindDeletionSlotIdOnLeaf(m_idx_1, key, recid, bufid, path), 85);
        ASSERT_EQ(bufid, ref_bufid);
        path.clear();
    }

    {
        IndexKey *key = get_key_f1(117 ^ m_reorder_mask);
        RecordId recid{100, (SlotId)((59 ^ m_reorder_mask) + 1)};
        ASSERT_EQ(FindDeletionSlotIdOnLeaf(m_idx_1, key, recid, bufid, path), 58);
        ASSERT_EQ(bufid, ref_bufid);
        path.clear();
    }

    {
        IndexKey *key = get_key_f1(117 ^ m_reorder_mask);
        RecordId recid{100, (SlotId)((59 ^ m_reorder_mask) + 2)};
        ASSERT_EQ(FindDeletionSlotIdOnLeaf(m_idx_1, key, recid, bufid, path), INVALID_SID);
        ASSERT_EQ(bufid, ref_bufid);
        path.clear();
    }

    {
        IndexKey *key = get_key_f1(117 ^ m_reorder_mask);
        RecordId recid{100, (SlotId)((59 ^ m_reorder_mask) + 6)};
        ASSERT_EQ(FindDeletionSlotIdOnLeaf(m_idx_1, key, recid, bufid, path), 60);
        ASSERT_EQ(bufid, ref_bufid);
        path.clear();
    }

    {
        IndexKey *key = get_key_f1(403 ^ m_reorder_mask);
        RecordId recid{100, (SlotId)(202 ^ m_reorder_mask)};
        ASSERT_EQ(FindDeletionSlotIdOnLeaf(m_idx_1, key, recid, bufid, path), INVALID_SID);
        ASSERT_EQ(bufid, ref_bufid);
        path.clear();
    }


    g_bufman->UnpinPage(bufid);


    TDB_TEST_END
}

TEST_F(BasicTestBTreeDelete, TestBTreeDeleteFindKeyOverflow) {
    TDB_TEST_BEGIN

    // creates a new leaf page
    BufferId bufid1 =
        CreateNewBTreePage(m_idx_1, false, true, INVALID_PID, INVALID_PID);
    char *buf1 = g_bufman->GetBuffer(bufid1);
    PageNumber pgno1 = g_bufman->GetPageNumber(bufid1);

    BufferId bufid2 =
        CreateNewBTreePage(m_idx_1, false, true, pgno1, INVALID_PID);
    char *buf2 = g_bufman->GetBuffer(bufid2);
    PageNumber pgno2 = g_bufman->GetPageNumber(bufid2);

    VarlenDataPage pg1(buf1), pg2(buf2);
    ((BTreePageHeaderData*)pg1.GetUserData())->m_next_pid = pgno2;
    maxaligned_char_buf recbuf;
    for (int64_t i = 1; i <= 202; ++i) {
        if (i == 23 || i == 87) continue;
        IndexKey *key = get_key_f1((i * 2 - 1) ^ m_reorder_mask);
        RecordId recid{100, (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_1, key, recid, recbuf);
        Record rec(recbuf);
        ASSERT_NO_ERROR(pg1.InsertRecordAt(pg1.GetMaxSlotId() + 1, rec));
        if (i == 59) {
            recid.sid += 2;
            CreateLeafRecord(m_idx_1, key, recid, recbuf);
            Record rec2(recbuf);
            ASSERT_NO_ERROR(pg1.InsertRecordAt(pg1.GetMaxSlotId() + 1, rec2));
            recid.sid += 3;
            CreateLeafRecord(m_idx_1, key, recid, recbuf);
            Record rec3(recbuf);
            ASSERT_NO_ERROR(pg1.InsertRecordAt(pg1.GetMaxSlotId() + 1, rec3));
        }
    }


    for (int64_t i = 1; i <= 202; ++i) {
        if (i == 23 || i == 87) continue;
        IndexKey *key = get_key_f1((404 + i * 2 - 1) ^ m_reorder_mask);
        RecordId recid{101, (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_1, key, recid, recbuf);
        Record rec(recbuf);
        ASSERT_NO_ERROR(pg2.InsertRecordAt(pg2.GetMaxSlotId() + 1, rec));
        if (i == 59) {
            recid.sid += 2;
            CreateLeafRecord(m_idx_1, key, recid, recbuf);
            Record rec2(recbuf);
            ASSERT_NO_ERROR(pg2.InsertRecordAt(pg2.GetMaxSlotId() + 1, rec2));
            recid.sid += 3;
            CreateLeafRecord(m_idx_1, key, recid, recbuf);
            Record rec3(recbuf);
            ASSERT_NO_ERROR(pg2.InsertRecordAt(pg2.GetMaxSlotId() + 1, rec3));
        }
    }

    g_bufman->MarkDirty(bufid1);
    g_bufman->MarkDirty(bufid2);
    g_bufman->UnpinPage(bufid2);

    BufferId search_bufid = bufid1;
    IndexKey *key = get_key_f1(405 ^ m_reorder_mask);
    RecordId recid{0, 0};
    std::vector<PathItem> path = {g_bufman->GetPageNumber(search_bufid)};
    ASSERT_EQ(FindDeletionSlotIdOnLeaf(m_idx_1, key, recid, search_bufid, path), 1);
    ASSERT_EQ(g_bufman->GetPageNumber(search_bufid), pgno2);
    path.clear();

    g_bufman->UnpinPage(search_bufid);

    TDB_TEST_END
}

TEST_F(BasicTestBTreeDelete, TestBTreeDeleteSlot) {
    TDB_TEST_BEGIN

    // creates a new leaf page
    BufferId bufid =
        CreateNewBTreePage(m_idx_1, false, true, INVALID_PID, INVALID_PID);
    char *buf = g_bufman->GetBuffer(bufid);

    VarlenDataPage pg(buf);
    maxaligned_char_buf recbuf;
    for (int64_t i = 1; i <= 202; ++i) {
        if (i == 23 || i == 87) continue;
        IndexKey *key = get_key_f1((i * 2 - 1) ^ m_reorder_mask);
        RecordId recid{100, (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_1, key, recid, recbuf);
        Record rec(recbuf);
        ASSERT_NO_ERROR(pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec));
        if (i == 59) {
            recid.sid += 2;
            CreateLeafRecord(m_idx_1, key, recid, recbuf);
            Record rec2(recbuf);
            ASSERT_NO_ERROR(pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec2));
            recid.sid += 3;
            CreateLeafRecord(m_idx_1, key, recid, recbuf);
            Record rec3(recbuf);
            ASSERT_NO_ERROR(pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec3));
        }
    }
    ASSERT_EQ(pg.GetMaxSlotId(), 202);
    g_bufman->MarkDirty(bufid);

    ASSERT_NO_ERROR(DeleteSlotOnPage(m_idx_1, bufid, 17));
    ASSERT_NO_ERROR(DeleteSlotOnPage(m_idx_1, bufid, 101));

    ASSERT_EQ(*(int64_t*)((BTreeLeafRecordHeaderData*)pg.GetRecord(17).GetData())->GetPayload(), 35);
    ASSERT_EQ(*(int64_t*)((BTreeLeafRecordHeaderData*)pg.GetRecord(72).GetData())->GetPayload(), 143);
    ASSERT_EQ(*(int64_t*)((BTreeLeafRecordHeaderData*)pg.GetRecord(200).GetData())->GetPayload(), 403);

    ASSERT_EQ(pg.GetMaxSlotId(), 200);

    g_bufman->UnpinPage(bufid);

    TDB_TEST_END

}

TEST_F(BasicTestBTreeDelete, TestBTreeDeleteSlotRoot) {
    TDB_TEST_BEGIN

    // creates a new leaf page
    BufferId bufid =
        CreateNewBTreePage(m_idx_1, true, false, INVALID_PID, INVALID_PID);
    char *buf = g_bufman->GetBuffer(bufid);

    VarlenDataPage pg(buf);
    maxaligned_char_buf recbuf;
    recbuf.resize(BTreeInternalRecordHeaderSize, 0);
    auto rechdr = (BTreeInternalRecordHeaderData *) recbuf.data();
    rechdr->m_child_pid = 1000;
    Record rec(recbuf);
    pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec);
    g_bufman->MarkDirty(bufid);

    ASSERT_EQ(pg.GetMaxSlotId(), 1);

    ASSERT_NO_ERROR(DeleteSlotOnPage(m_idx_1, bufid, 1));

    ASSERT_EQ(pg.GetMaxSlotId(), 0);
    ASSERT_TRUE(((BTreePageHeaderData*)pg.GetUserData())->IsLeafPage());

    g_bufman->UnpinPage(bufid);

    TDB_TEST_END
}

struct BasicTestBTreeDelete1: public TestBTree {
protected:
    void SetUp() override {
        TestEnableLogging();
        std::string dpath = absl::GetFlag(FLAGS_db_path);
        if (!dpath.empty()) {
            LOG(kInfo, "Makeing a copy of the user provided db path" ,
                        g_test_existing_db_path);
        } else {
            dpath =
                SRCDIR "/data/preloaded_btree/btree_delete_test_merge";
            LOG(kInfo, "Making a copy of the default db path");
        }
        TestDisableLogging();

        std::string tmpdir = MakeTempDir();

        // copy the files
        copy_dir(dpath.c_str(), tmpdir.c_str());

        // use the copied temp dir as our db path
        g_test_existing_db_path = tmpdir;
        TestBTree::SetUp();
    }
};

TEST_F(BasicTestBTreeDelete1, TestBTreeDeleteSingleMerge) {
    TDB_TEST_BEGIN

    // We created a dummy table to indicate this is the preloaded DB with
    // the B-tree we test on. We also store the seed as the first row in the
    // table.
    Oid dummy_tabid = g_catcache->FindTableByName("BTreeDeleteTestMergeDummyTable");
    ASSERT_NE(dummy_tabid, InvalidOid) <<
        "The current DB instance does not contain the preloaded B-tree we are "
        "expecting.";
    ASSERT_EQ(m_idx_2->GetTreeHeight(), 3);

    // the page counts in the tree initially:
    //            2
    //     6             5
    // 6 6 6 5 6 6    6 6 5 6 6
    //
    // leaf keys are 1 to 66 except 19 and 49

    // delete from the 4th page on the leaf level counting from the left
    // This will cause a single merge with the 5th page.
    int i = 24;
    IndexKey *key = get_key_f2(i ^ m_reorder_mask);
    RecordId recid;
    RecordId expected_recid((PageNumber)(100 + i),
                            (SlotId)((i ^ m_reorder_mask) + 1));
    ASSERT_TRUE(m_idx_2->DeleteKey(key, recid));
    ASSERT_EQ(recid, expected_recid);

    // Now the tree should look like:
    // tree page counts:
    //            2
    //     5             5
    // 6 6 6 10 6    6 6 5 6 6
    //
    // leaf keys are 1 to 66 except 19, 24, and 49

    // Make sure the tree looks like what we thought:
    std::vector<PathItem> search_path;
    ScopedBufferId bufid = FindLeafPage(m_idx_2, nullptr, RecordId(),
                                        &search_path);
    ASSERT_EQ(search_path.size(), 2);

    std::vector<int64_t> ikeys;
    ikeys.reserve(63);
    for (int64_t i = 1; i <= 66; ++i) {
        if (i == 19 || i == 24 || i == 49)
            continue;
        ikeys.push_back(i);
    }
    auto ikeys_iter = ikeys.begin();

    // check leaf level
    std::vector<PageNumber> pids;
    pids.reserve(10);
    for (int i = 1; i <= 10; ++i) {
        ASSERT(bufid.IsValid());
        pids.push_back(g_bufman->GetPageNumber(bufid));
        char *buf = g_bufman->GetBuffer(bufid);
        VarlenDataPage pg(buf);
        auto hdr = (BTreePageHeaderData*) pg.GetUserData();
        ASSERT_FALSE(hdr->IsRootPage());
        ASSERT_TRUE(hdr->IsLeafPage());
        SlotId expected_nslots;
        if (i == 4) {
            expected_nslots = 10;
        } else if (i == 8) {
            expected_nslots = 5;
        } else {
            expected_nslots = 6;
        }
        ASSERT_EQ(pg.GetMaxSlotId() - pg.GetMinSlotId() + 1, expected_nslots)
            << i;

        // check the keys
        for (SlotId sid = pg.GetMinSlotId(); sid <= pg.GetMaxSlotId(); ++sid) {
            ASSERT_NE(ikeys_iter - ikeys.begin(), ikeys.size());
            int64_t ikey = *ikeys_iter;
            absl::string_view expected_key = get_skey(ikey ^ m_reorder_mask);
            RecordId expected_recid((PageNumber)(100 + ikey),
                                    (SlotId)((ikey ^ m_reorder_mask) + 1));

            Record rec = pg.GetRecord(sid);
            ASSERT_EQ(rec.GetLength(), 392);
            auto rechdr = (BTreeLeafRecordHeaderData*) rec.GetData();
            ASSERT_EQ(rechdr->m_recid, expected_recid);
            absl::string_view key = read_f2_payload(rechdr->GetPayload());
            ASSERT_EQ(key, expected_key);
            ++ikeys_iter;
        }

        PageNumber rpid = hdr->m_next_pid;
        g_bufman->UnpinPage(bufid);
        if (rpid != INVALID_PID)
            bufid = g_bufman->PinPage(rpid, &buf);
    }
    ASSERT_FALSE(bufid.IsValid());

    // check one level above leaf
    {
        char *buf;
        bufid = g_bufman->PinPage(search_path[1].pid, &buf);
    }
    auto pids_iter = pids.begin();
    std::vector<PageNumber> pids2;
    pids2.reserve(2);
    int64_t j = 1;
    for (int i = 1; i <= 2; ++i) {
        ASSERT(bufid.IsValid());
        pids2.push_back(g_bufman->GetPageNumber(bufid));
        char *buf = g_bufman->GetBuffer(bufid);
        VarlenDataPage pg(buf);
        auto hdr = (BTreePageHeaderData*) pg.GetUserData();
        ASSERT_FALSE(hdr->IsRootPage());
        ASSERT_FALSE(hdr->IsLeafPage());
        SlotId expected_nslots = 5;
        ASSERT_EQ(pg.GetMaxSlotId() - pg.GetMinSlotId() + 1, expected_nslots);

        for (SlotId sid = pg.GetMinSlotId(); sid <= pg.GetMaxSlotId(); ++sid) {
            ASSERT_NE(pids_iter - pids.begin(), pids.size());
            Record rec = pg.GetRecord(sid);

            auto rechdr = (BTreeInternalRecordHeaderData*) rec.GetData();
            ASSERT_EQ(rechdr->m_child_pid, *pids_iter) << sid;

            if (sid == pg.GetMinSlotId()) {
                ASSERT_EQ(rec.GetLength(), BTreeInternalRecordHeaderSize);
            } else {
                int64_t ikey = j * 6 + 1;
                ++j;
                // skip the merged pages or those does not have keys
                while (j == 4 || j == 6) ++j;

                absl::string_view expected_key = get_skey(ikey ^ m_reorder_mask);
                RecordId expected_recid((PageNumber)(100 + ikey),
                                        (SlotId)((ikey ^ m_reorder_mask) + 1));

                ASSERT_EQ(rec.GetLength(), 400) << sid;
                ASSERT_EQ(rechdr->m_heap_recid, expected_recid) << sid;
                absl::string_view key = read_f2_payload(rechdr->GetPayload());
                ASSERT_EQ(key, expected_key);
            }
            ++pids_iter;
        }

        PageNumber rpid = hdr->m_next_pid;
        g_bufman->UnpinPage(bufid);
        if (rpid != INVALID_PID)
            bufid = g_bufman->PinPage(rpid, &buf);
    }
    ASSERT_FALSE(bufid.IsValid());

    // check root level
    {
        char *buf;
        bufid = g_bufman->PinPage(search_path[0].pid, &buf);
        VarlenDataPage pg(buf);
        auto hdr = (BTreePageHeaderData*) pg.GetUserData();
        ASSERT_TRUE(hdr->IsRootPage());
        ASSERT_FALSE(hdr->IsLeafPage());
        ASSERT_EQ(pg.GetMaxSlotId() - pg.GetMinSlotId() + 1, 2);

        for (SlotId sid = pg.GetMinSlotId(); sid <= pg.GetMaxSlotId(); ++sid) {
            ASSERT_LE((size_t) sid, pids2.size());
            Record rec = pg.GetRecord(sid);

            auto rechdr = (BTreeInternalRecordHeaderData*) rec.GetData();
            ASSERT_EQ(rechdr->m_child_pid, pids2[sid - 1]) << sid;

            if (sid == pg.GetMinSlotId()) {
                ASSERT_EQ(rec.GetLength(), BTreeInternalRecordHeaderSize);
            } else {
                int64_t ikey = 37;
                absl::string_view expected_key = get_skey(ikey ^ m_reorder_mask);
                RecordId expected_recid((PageNumber)(100 + ikey),
                                        (SlotId)((ikey ^ m_reorder_mask) + 1));

                ASSERT_EQ(rec.GetLength(), 400) << sid;
                ASSERT_EQ(rechdr->m_heap_recid, expected_recid) << sid;
                absl::string_view key = read_f2_payload(rechdr->GetPayload());
                ASSERT_EQ(key, expected_key);
            }
        }

        g_bufman->UnpinPage(bufid);
    }

    TDB_TEST_END
}

TEST_F(BasicTestBTreeDelete1, TestBTreeDeleteRecursiveMerge) {
    TDB_TEST_BEGIN

    // We created a dummy table to indicate this is the preloaded DB with
    // the B-tree we test on. We also store the seed as the first row in the
    // table.
    Oid dummy_tabid = g_catcache->FindTableByName("BTreeDeleteTestMergeDummyTable");
    ASSERT_NE(dummy_tabid, InvalidOid) <<
        "The current DB instance does not contain the preloaded B-tree we are "
        "expecting.";
    ASSERT_EQ(m_idx_2->GetTreeHeight(), 3);

    // the page counts in the tree initially:
    //            2
    //     6             5
    // 6 6 6 5 6 6    6 6 5 6 6
    //
    // leaf keys are 1 to 66 except 19 and 49

    // delete from the 4th page on the leaf level counting from the left
    // This will cause a single merge with the 5th page.
    int i = 50;
    IndexKey *key = get_key_f2(i ^ m_reorder_mask);
    RecordId recid;
    RecordId expected_recid((PageNumber)(100 + i),
                            (SlotId)((i ^ m_reorder_mask) + 1));
    ASSERT_TRUE(m_idx_2->DeleteKey(key, recid));
    ASSERT_EQ(recid, expected_recid);

    // Now the tree should look like:
    // tree page counts:
    //         10
    // 6 6 6 5 6 6 6 6 10 6
    //
    // leaf keys are 1 to 66 except 19, 49 and 50

    // Make sure the tree looks like what we thought:
    std::vector<PathItem> search_path;
    ScopedBufferId bufid = FindLeafPage(m_idx_2, nullptr, RecordId(),
                                        &search_path);
    ASSERT_EQ(search_path.size(), 1);

    std::vector<int64_t> ikeys;
    ikeys.reserve(63);
    for (int64_t i = 1; i <= 66; ++i) {
        if (i == 19 || i == 49 || i == 50)
            continue;
        ikeys.push_back(i);
    }
    auto ikeys_iter = ikeys.begin();

    // check leaf level
    std::vector<PageNumber> pids;
    pids.reserve(10);
    for (int i = 1; i <= 10; ++i) {
        ASSERT(bufid.IsValid());
        pids.push_back(g_bufman->GetPageNumber(bufid));
        char *buf = g_bufman->GetBuffer(bufid);
        VarlenDataPage pg(buf);
        auto hdr = (BTreePageHeaderData*) pg.GetUserData();
        ASSERT_FALSE(hdr->IsRootPage());
        ASSERT_TRUE(hdr->IsLeafPage());
        SlotId expected_nslots;
        if (i == 4) {
            expected_nslots = 5;
        } else if (i == 9) {
            expected_nslots = 10;
        } else {
            expected_nslots = 6;
        }
        ASSERT_EQ(pg.GetMaxSlotId() - pg.GetMinSlotId() + 1, expected_nslots)
            << i;

        // check the keys
        for (SlotId sid = pg.GetMinSlotId(); sid <= pg.GetMaxSlotId(); ++sid) {
            ASSERT_NE(ikeys_iter - ikeys.begin(), ikeys.size());
            int64_t ikey = *ikeys_iter;
            absl::string_view expected_key = get_skey(ikey ^ m_reorder_mask);
            RecordId expected_recid((PageNumber)(100 + ikey),
                                    (SlotId)((ikey ^ m_reorder_mask) + 1));

            Record rec = pg.GetRecord(sid);
            ASSERT_EQ(rec.GetLength(), 392);
            auto rechdr = (BTreeLeafRecordHeaderData*) rec.GetData();
            ASSERT_EQ(rechdr->m_recid, expected_recid);
            absl::string_view key = read_f2_payload(rechdr->GetPayload());
            ASSERT_EQ(key, expected_key);
            ++ikeys_iter;
        }

        PageNumber rpid = hdr->m_next_pid;
        g_bufman->UnpinPage(bufid);
        if (rpid != INVALID_PID)
            bufid = g_bufman->PinPage(rpid, &buf);
    }
    ASSERT_FALSE(bufid.IsValid());

    // check the new root level
    {
        char *buf;
        bufid = g_bufman->PinPage(search_path[0].pid, &buf);
    }
    auto pids_iter = pids.begin();
    int64_t j = 1;
    for (int i = 1; i <= 1; ++i) {
        ASSERT(bufid.IsValid());
        char *buf = g_bufman->GetBuffer(bufid);
        VarlenDataPage pg(buf);
        auto hdr = (BTreePageHeaderData*) pg.GetUserData();
        ASSERT_TRUE(hdr->IsRootPage());
        ASSERT_FALSE(hdr->IsLeafPage());
        SlotId expected_nslots = 10;
        ASSERT_EQ(pg.GetMaxSlotId() - pg.GetMinSlotId() + 1, expected_nslots);

        for (SlotId sid = pg.GetMinSlotId(); sid <= pg.GetMaxSlotId(); ++sid) {
            ASSERT_NE(pids_iter - pids.begin(), pids.size());
            Record rec = pg.GetRecord(sid);

            auto rechdr = (BTreeInternalRecordHeaderData*) rec.GetData();
            ASSERT_EQ(rechdr->m_child_pid, *pids_iter) << sid;

            if (sid == pg.GetMinSlotId()) {
                ASSERT_EQ(rec.GetLength(), BTreeInternalRecordHeaderSize);
            } else {
                int64_t ikey = j * 6 + 1;
                ++j;
                // skip the merged pages or those does not have keys
                while (j == 9) ++j;

                absl::string_view expected_key = get_skey(ikey ^ m_reorder_mask);
                RecordId expected_recid((PageNumber)(100 + ikey),
                                        (SlotId)((ikey ^ m_reorder_mask) + 1));

                ASSERT_EQ(rec.GetLength(), 400) << sid;
                ASSERT_EQ(rechdr->m_heap_recid, expected_recid) << sid;
                absl::string_view key = read_f2_payload(rechdr->GetPayload());
                ASSERT_EQ(key, expected_key);
            }
            ++pids_iter;
        }

        PageNumber rpid = hdr->m_next_pid;
        g_bufman->UnpinPage(bufid);
        if (rpid != INVALID_PID)
            bufid = g_bufman->PinPage(rpid, &buf);
    }
    ASSERT_FALSE(bufid.IsValid());

    TDB_TEST_END
}

struct BasicTestBTreeDelete2: public TestBTree {
protected:
    void SetUp() override {
        TestEnableLogging();
        std::string dpath = absl::GetFlag(FLAGS_db_path);
        if (!dpath.empty()) {
            LOG(kInfo, "Makeing a copy of the user provided db path" ,
                        g_test_existing_db_path);
        } else {
            dpath =
                SRCDIR "/data/preloaded_btree/btree_full_scan_varlen";
            LOG(kInfo, "Making a copy of the default db path");
        }
        TestDisableLogging();

        std::string tmpdir = MakeTempDir();

        // copy the files
        copy_dir(dpath.c_str(), tmpdir.c_str());

        // use the copied temp dir as our db path
        g_test_existing_db_path = tmpdir;
        TestBTree::SetUp();
    }
};

TEST_F(BasicTestBTreeDelete2, TestBTreeDeleteVarlenKey) {
    TDB_TEST_BEGIN

    uint32_t seed;

    // We created a dummy table to indicate this is the preloaded DB with
    // the B-tree we test on. We also store the seed as the first row in the
    // table.
    Oid dummy_tabid = g_catcache->FindTableByName("TestBTreeFullScanVarlenDummyTable");
    ASSERT_NE(dummy_tabid, InvalidOid) <<
        "The current DB instance does not contain the preloaded B-tree we are "
        "expecting.";
    ASSERT_GT(m_idx_2->GetTreeHeight(), 3);
    {
        std::shared_ptr<const TableDesc> dummy_tabdesc =
            g_catcache->FindTableDesc(dummy_tabid);
        ASSERT_NE(dummy_tabdesc.get(), nullptr);
        std::unique_ptr<Table> tab = Table::Create(std::move(dummy_tabdesc));
        const Schema *sch = tab->GetTableDesc()->GetSchema();
        ASSERT_EQ(sch->GetNumFields(), 1);
        ASSERT_EQ(sch->GetFieldTypeId(0), initoids::TYP_UINT4);

        // get the seed
        auto iter = tab->StartScan();
        ASSERT_TRUE(iter.Next());
        const char *payload = iter.GetCurrentRecord().GetData();
        seed = sch->GetField(0, payload).GetUInt32();
    }

    // this random number generator should use the same seed as we generated
    // the data
    std::mt19937 gen(seed);
    std::uniform_int_distribution<size_t> dis(190, 380);
    std::vector<std::pair<std::string, RecordId>> keys;
    for (int64_t i = 1; i <= 4000; ++i) {
        size_t len = dis(gen);
        RecordId recid{100, (SlotId)(get_ikey(i) + 1)};
        keys.push_back({cast_as_string(get_skey(i, len)), recid});
    }

    RecordId recid{123, (SlotId)(get_ikey(2016) + 1)};
    keys.push_back({cast_as_string(get_skey(2016, 264)), recid});

    RecordId recid2{321, (SlotId)(get_ikey(2016) + 1)};
    keys.push_back({cast_as_string(get_skey(2016, 311)), recid2});

    // delete the keys in a random order in a few batches
    std::shuffle(keys.begin(), keys.end(), gen);

    int64_t i = 0;
    for (int64_t n: {1000, 1000, 1000, 500, 400, 50, 20, 25, 4, 3}) {
        int64_t j = i + n;
        while (i < j) {
            RecordId recid = keys[i].second;
            Datum d = Datum::FromVarlenAsStringView(keys[i].first);
            IndexKey::Construct(m_index_key.get(), &d, 1);
            IndexKey *key = (IndexKey*) m_index_key.get();
            ASSERT_TRUE(m_idx_2->DeleteKey(key, recid));
            ASSERT_EQ(recid, keys[i].second);
            ++i;
        }

        // check if we still have all the remaining keys
        std::vector<std::pair<std::string, RecordId>> remkeys(
            keys.begin() + j, keys.end());
        std::sort(remkeys.begin(), remkeys.end());
        auto key_iter = remkeys.begin();
        auto tree_iter = m_idx_2->StartScan(nullptr, false, nullptr, false);
        int k = 0;
        while (tree_iter->Next()) {
            ASSERT_NE(key_iter, remkeys.end()) << k;
            absl::string_view expected_key(key_iter->first);
            absl::string_view key =
                read_f2_payload(tree_iter->GetCurrentItem().GetData());
            ASSERT_EQ(key, expected_key) << k << ' ' << j;
            ASSERT_EQ(tree_iter->GetCurrentRecordId(), key_iter->second) << k;
            ++k;
            ++key_iter;
        }
    }

    ASSERT_EQ(m_idx_2->GetTreeHeight(), 1);
    ASSERT_TRUE(m_idx_2->IsEmpty());

    TDB_TEST_END
}

}   // namespace taco
