#define DEFAULT_BUFPOOL_SIZE 100
#include "index/btree/TestBTree.h"

ABSL_FLAG(std::string, db_path, "",
        "set this to override the default db path that contains "
        "the required preloaded b-tree");

namespace taco {

using BasicTestBTreeSearch = TestBTree;

TEST_F(BasicTestBTreeSearch, TestBTreeTupleCompareFullKey) {
    TDB_TEST_BEGIN

    ScopedBufferId bufid =
        CreateNewBTreePage(m_idx_1, false, true, INVALID_PID, INVALID_PID);
    char *buf = g_bufman->GetBuffer(bufid);

    VarlenDataPage pg(buf);
    maxaligned_char_buf recbuf;

    int i, j;
    IndexKey *key_i, *key_j;
    RecordId recid;

    i = 0 ^ m_reorder_mask;
    j = 1 ^ m_reorder_mask;
    key_i = get_key_f1(i);
    recid = RecordId(100, (SlotId)((i ^ m_reorder_mask) + 1));
    CreateLeafRecord(m_idx_1, key_i, recid, recbuf);
    key_j = get_key_f1(j);
    EXPECT_GT(BTreeTupleCompare(m_idx_1, key_j, recid, recbuf.data(), true), 0);

    i = 2 ^ m_reorder_mask;
    j = 1 ^ m_reorder_mask;
    key_i = get_key_f1(i);
    recid = RecordId(100, (SlotId)((i ^ m_reorder_mask) + 1));
    CreateLeafRecord(m_idx_1, key_i, recid, recbuf);
    key_j = get_key_f1(j);
    EXPECT_LT(BTreeTupleCompare(m_idx_1, key_j, recid, recbuf.data(), true), 0);

    i = 1 ^ m_reorder_mask;
    j = 1 ^ m_reorder_mask;
    key_i = get_key_f1(i);
    recid = RecordId(100, (SlotId)((i ^ m_reorder_mask) + 1));
    CreateLeafRecord(m_idx_1, key_i, recid, recbuf);
    key_j = get_key_f1(j);
    EXPECT_EQ(BTreeTupleCompare(m_idx_1, key_j, recid, recbuf.data(), true), 0);

    TDB_TEST_END
}

TEST_F(BasicTestBTreeSearch, TestBTreeTupleComparePrefixKey) {
    TDB_TEST_BEGIN

    ScopedBufferId bufid =
        CreateNewBTreePage(m_idx_1, false, true, INVALID_PID, INVALID_PID);
    char *buf = g_bufman->GetBuffer(bufid);

    VarlenDataPage pg(buf);
    maxaligned_char_buf recbuf;

    int i, j;
    IndexKey *key_i_j, *key_i, *key_j;
    RecordId recid;

    i = 0 ^ m_reorder_mask;
    j = 1 ^ m_reorder_mask;
    key_i_j = get_key_f1_f2(i, j);
    recid = RecordId(INVALID_PID, (SlotId)((i ^ m_reorder_mask) + 1));
    CreateLeafRecord(m_idx_1_2, key_i_j, recid, recbuf);
    key_i = get_key_f1(i);
    EXPECT_LT(BTreeTupleCompare(m_idx_1_2, key_i, recid, recbuf.data(), true), 0);

    key_i_j = get_key_f1_f2(i, j);
    recid = RecordId(INVALID_PID, (SlotId)((i ^ m_reorder_mask) + 1));
    CreateLeafRecord(m_idx_1_2, key_i_j, recid, recbuf);
    key_j = get_key_f1(j);
    EXPECT_GT(BTreeTupleCompare(m_idx_1_2, key_j, recid, recbuf.data(), true), 0);

    TDB_TEST_END
}

TEST_F(BasicTestBTreeSearch, TestBinarySearchOnLeafPageFixedlen) {
    TDB_TEST_BEGIN
    // creates a new leaf page
    ScopedBufferId bufid =
        CreateNewBTreePage(m_idx_1, true, true, INVALID_PID, INVALID_PID);
    char *buf = g_bufman->GetBuffer(bufid);

    // 1, 3, 5, 7, ....., 81, 85, ......, 159, 161, 161, 163, ..., 403
    VarlenDataPage pg(buf);
    maxaligned_char_buf recbuf;
    for (int64_t i = 1; i <= 202; ++i) {
        if (i == 42) continue;
        IndexKey *key = get_key_f1((i * 2 - 1) ^ m_reorder_mask);
        RecordId recid{100, (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_1, key, recid, recbuf);
        Record rec(recbuf);
        ASSERT_NO_ERROR(pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec));
        if (i == 81) {
            RecordId recid{101, (SlotId)((i ^ m_reorder_mask) + 1)};
            CreateLeafRecord(m_idx_1, key, recid, recbuf);
            Record rec(recbuf);
            ASSERT_NO_ERROR(pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec));
        }
    }

    RecordId lookup_rid{100, (SlotId)((1 ^ m_reorder_mask))};
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(0 ^ m_reorder_mask), INFINITY_RECORDID), 0);
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(1 ^ m_reorder_mask), lookup_rid), 0);
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(1 ^ m_reorder_mask), INFINITY_RECORDID), 1);
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(2 ^ m_reorder_mask), INFINITY_RECORDID), 1);

    lookup_rid.sid = (SlotId)((161 ^ m_reorder_mask));
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(161 ^ m_reorder_mask), lookup_rid), 79);
    lookup_rid.sid += 1;
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(161 ^ m_reorder_mask), lookup_rid), 79);
    lookup_rid.pid += 1;
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(161 ^ m_reorder_mask), lookup_rid), 80);
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(161 ^ m_reorder_mask), INFINITY_RECORDID), 81);
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(1000 ^ m_reorder_mask), INFINITY_RECORDID), 202);

    TDB_TEST_END
}

TEST_F(BasicTestBTreeSearch, TestBinarySearchOnInternalPageFixedlen) {
    TDB_TEST_BEGIN
    //create a new empty internal page.
    ScopedBufferId bufid =
        CreateNewBTreePage(m_idx_1, false, false, INVALID_PID, INVALID_PID);

    char *buf = g_bufman->GetBuffer(bufid);

    VarlenDataPage pg(buf);
    maxaligned_char_buf recbuf;
    recbuf.resize(BTreeInternalRecordHeaderSize, 0);
    auto rechdr = (BTreeInternalRecordHeaderData *) recbuf.data();
    rechdr->m_child_pid = 1000;
    Record rec(recbuf);
    pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec);

    // x 2 4 6 ... 72 76 78 ... 174 176 176 ... 288
    PageNumber cnt = 1;
    for (int64_t i = 1; i <= 144; ++i) {
        if (i == 37) continue;
        IndexKey *key = get_key_f1((i * 2) ^ m_reorder_mask);
        recbuf.resize(BTreeInternalRecordHeaderSize, 0);
        auto rechdr = (BTreeInternalRecordHeaderData *) recbuf.data();
        rechdr->m_child_pid = 1000 + cnt;
        RecordId recid{1000 + cnt, (SlotId)((i ^ m_reorder_mask) + 1)};
        rechdr->m_heap_recid = recid;
        m_idx_1->GetKeySchema()->WritePayloadToBuffer(key->ToNullableDatumVector(), recbuf);
        Record rec(recbuf);
        pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec);
        ++cnt;
        if (i == 88) {
            IndexKey *key = get_key_f1((i * 2) ^ m_reorder_mask);
            recbuf.resize(BTreeInternalRecordHeaderSize, 0);
            auto rechdr = (BTreeInternalRecordHeaderData *) recbuf.data();
            rechdr->m_child_pid = 1000 + cnt;
            RecordId recid{1000 + cnt, (SlotId)((i ^ m_reorder_mask) + 1)};
            rechdr->m_heap_recid = recid;
            m_idx_1->GetKeySchema()->WritePayloadToBuffer(key->ToNullableDatumVector(), recbuf);
            Record rec(recbuf);
            pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec);
            ++cnt;
        }
    }

    RecordId lookup_rid{1000, (SlotId)((1 ^ m_reorder_mask))};
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(-1000 ^ m_reorder_mask), INFINITY_RECORDID), 1);
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(0 ^ m_reorder_mask), lookup_rid), 1);
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(2 ^ m_reorder_mask), lookup_rid), 1);
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(2 ^ m_reorder_mask), INFINITY_RECORDID), 2);
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(74 ^ m_reorder_mask), lookup_rid), 37);

    lookup_rid.pid = 1087;
    lookup_rid.sid = (SlotId)((88 ^ m_reorder_mask));
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(176 ^ m_reorder_mask), lookup_rid), 87);
    lookup_rid.sid += 1;
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(176 ^ m_reorder_mask), lookup_rid), 88);
    lookup_rid.pid += 1;
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(176 ^ m_reorder_mask), lookup_rid), 89);
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(176 ^ m_reorder_mask), INFINITY_RECORDID), 89);
    ASSERT_EQ(BinarySearchOnPage(m_idx_1, buf, get_key_f1(10000 ^ m_reorder_mask), INFINITY_RECORDID), 145);

    TDB_TEST_END
}

TEST_F(BasicTestBTreeSearch, TestBinarySearchOnLeafPageVarlen) {
    TDB_TEST_BEGIN
    // creates a new leaf page
    ScopedBufferId bufid =
        CreateNewBTreePage(m_idx_2, true, true, INVALID_PID, INVALID_PID);
    char *buf = g_bufman->GetBuffer(bufid);

    size_t lengths[10] = {300, 350, 380, 333, 364, 298, 377, 321, 284, 324};
    VarlenDataPage pg(buf);
    maxaligned_char_buf recbuf;
    for (int64_t i = 1; i <= 10; ++i) {
        IndexKey *key = get_key_f2((i * 2 - 1) ^ m_reorder_mask, lengths[i - 1]);
        RecordId recid{100, (SlotId)((i ^ m_reorder_mask) + 1)};
        CreateLeafRecord(m_idx_2, key, recid, recbuf);
        Record rec(recbuf);
        ASSERT_NO_ERROR(pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec));
    }

    RecordId lookup_rid{100, (SlotId)((1 ^ m_reorder_mask))};
    ASSERT_EQ(BinarySearchOnPage(m_idx_2, buf, get_key_f2(0 ^ m_reorder_mask, 100), INFINITY_RECORDID), 0);
    ASSERT_EQ(BinarySearchOnPage(m_idx_2, buf, get_key_f2(1 ^ m_reorder_mask, 200), INFINITY_RECORDID), 0);
    ASSERT_EQ(BinarySearchOnPage(m_idx_2, buf, get_key_f2(1 ^ m_reorder_mask, 300), lookup_rid), 0);
    ASSERT_EQ(BinarySearchOnPage(m_idx_2, buf, get_key_f2(1 ^ m_reorder_mask, 300), INFINITY_RECORDID), 1);
    ASSERT_EQ(BinarySearchOnPage(m_idx_2, buf, get_key_f2(9 ^ m_reorder_mask, 380), lookup_rid), 5);
    ASSERT_EQ(BinarySearchOnPage(m_idx_2, buf, get_key_f2(15 ^ m_reorder_mask, 290), INFINITY_RECORDID), 7);

    TDB_TEST_END
}

TEST_F(BasicTestBTreeSearch, TestBinarySearchOnInternalPageVarlen) {
    TDB_TEST_BEGIN
    //create a new empty internal page.
    ScopedBufferId bufid =
        CreateNewBTreePage(m_idx_2, false, false, INVALID_PID, INVALID_PID);

    char *buf = g_bufman->GetBuffer(bufid);

    size_t lengths[10] = {302, 351, 378, 363, 314, 288, 357, 341, 294};

    VarlenDataPage pg(buf);
    maxaligned_char_buf recbuf;
    recbuf.resize(BTreeInternalRecordHeaderSize, 0);
    auto rechdr = (BTreeInternalRecordHeaderData *) recbuf.data();
    rechdr->m_child_pid = 1000;
    Record rec(recbuf);
    pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec);

    for (int64_t i = 1; i <= 9; ++i) {
        IndexKey *key = get_key_f2((i * 3) ^ m_reorder_mask, lengths[i - 1]);
        recbuf.resize(BTreeInternalRecordHeaderSize, 0);
        auto rechdr = (BTreeInternalRecordHeaderData *) recbuf.data();
        rechdr->m_child_pid = 1000 + (PageNumber)i;
        RecordId recid{1000 + (PageNumber)i, (SlotId)((i ^ m_reorder_mask) + 1)};
        rechdr->m_heap_recid = recid;
        m_idx_2->GetKeySchema()->WritePayloadToBuffer(key->ToNullableDatumVector(), recbuf);
        Record rec(recbuf);
        pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec);
    }

    RecordId lookup_rid{1000, (SlotId)((1 ^ m_reorder_mask))};
    ASSERT_EQ(BinarySearchOnPage(m_idx_2, buf, get_key_f2(0 ^ m_reorder_mask, 150), lookup_rid), 1);
    ASSERT_EQ(BinarySearchOnPage(m_idx_2, buf, get_key_f2(2 ^ m_reorder_mask, 150), INFINITY_RECORDID), 1);
    ASSERT_EQ(BinarySearchOnPage(m_idx_2, buf, get_key_f2(3 ^ m_reorder_mask, 300), INFINITY_RECORDID), 1);
    ASSERT_EQ(BinarySearchOnPage(m_idx_2, buf, get_key_f2(3 ^ m_reorder_mask, 302), lookup_rid), 1);
    ASSERT_EQ(BinarySearchOnPage(m_idx_2, buf, get_key_f2(3 ^ m_reorder_mask, 302), INFINITY_RECORDID), 2);
    ASSERT_EQ(BinarySearchOnPage(m_idx_2, buf, get_key_f2(3 ^ m_reorder_mask, 380), lookup_rid), 2);
    ASSERT_EQ(BinarySearchOnPage(m_idx_2, buf, get_key_f2(12 ^ m_reorder_mask, 363), lookup_rid), 4);
    ASSERT_EQ(BinarySearchOnPage(m_idx_2, buf, get_key_f2(21 ^ m_reorder_mask, 357), INFINITY_RECORDID), 8);

    TDB_TEST_END
}

// Below are tests that require preloaded b-trees. We create a slightly
// different BasicTestBTreeSearch class that sets the default db path. However,
// caller can always overrides the path using the --db_path command line arg.

struct BasicTestBTreeSearch1: public TestBTree {
protected:
    void SetUp() override {
        TestEnableLogging();
        g_test_existing_db_path = absl::GetFlag(FLAGS_db_path);
        if (!g_test_existing_db_path.empty()) {
            LOG(kInfo, "Using the user provided db path" ,
                        g_test_existing_db_path);
        } else {
            g_test_existing_db_path =
                SRCDIR "/data/preloaded_btree/btree_full_scan_fixedlen";
            LOG(kInfo, "Using the default db path");
        }
        TestDisableLogging();

        TestBTree::SetUp();
    }
};

TEST_F(BasicTestBTreeSearch1, TestBTreeFullScanFixedlen) {
    TDB_TEST_BEGIN

    // We created a dummy table to indicate this is the preloaded DB with
    // the B-tree we test on.
    Oid dummy_tabid = g_catcache->FindTableByName("TestBTreeFullScanFixedlenDummyTable");
    ASSERT_NE(dummy_tabid, InvalidOid) <<
        "The current DB instance does not contain the preloaded B-tree we are "
        "expecting.";
    ASSERT_EQ(m_idx_1->GetTreeHeight(), 3);

    std::vector<int64_t> keys;
    keys.reserve(40001);
    for (int64_t i = 1; i <= 40000; ++i) {
        keys.emplace_back(get_ikey(i));
    }
    keys.emplace_back(get_ikey(2022));

    std::sort(keys.begin(), keys.end());

    std::unique_ptr<Index::Iterator> it;
    ASSERT_NO_ERROR(it = m_idx_1->StartScan(nullptr, false, nullptr, false));
    size_t now = 0;
    bool flag = false;
    while (it->Next()) {
        auto key = read_f1_payload(it->GetCurrentItem().GetData());
        auto recid = it->GetCurrentRecordId();
        ASSERT_EQ(key, keys[now]);

        if (keys[now] == get_ikey(2022) && flag) {
            ASSERT_EQ(RecordId(101, (SlotId)(keys[now] + 1)), recid);
        } else {
            ASSERT_EQ(RecordId(100, (SlotId)(keys[now] + 1)), recid);

            if (keys[now] == get_ikey(2022)) flag = true;
        }

        ++now;
    }
    ASSERT_EQ(now, keys.size());
    it->EndScan();

    TDB_TEST_END
}

struct BasicTestBTreeSearch2: public TestBTree {
protected:
    void SetUp() override {
        TestEnableLogging();
        g_test_existing_db_path = absl::GetFlag(FLAGS_db_path);
        if (!g_test_existing_db_path.empty()) {
            LOG(kInfo, "Using the user provided db path" ,
                        g_test_existing_db_path);
        } else {
            g_test_existing_db_path =
                SRCDIR "/data/preloaded_btree/btree_range_scan_fixedlen";
            LOG(kInfo, "Using the default db path");
        }
        TestDisableLogging();

        TestBTree::SetUp();
    }
};

TEST_F(BasicTestBTreeSearch2, TestBTreeRangeScanFixedlen) {
    TDB_TEST_BEGIN

    // We created a dummy table to indicate this is the preloaded DB with
    // the B-tree we test on.
    Oid dummy_tabid = g_catcache->FindTableByName("TestBTreeRangeScanFixedlenDummyTable");
    ASSERT_NE(dummy_tabid, InvalidOid) <<
        "The current DB instance does not contain the preloaded B-tree we are "
        "expecting.";
    ASSERT_EQ(m_idx_1->GetTreeHeight(), 3);

    std::vector<int64_t> keys;
    keys.reserve(40001);
    for (int64_t i = 1; i <= 40000; ++i) {
        keys.emplace_back(get_ikey(i));
    }

    keys.emplace_back(get_ikey(2020));

    std::sort(keys.begin(), keys.end());

    std::unique_ptr<Index::Iterator> it;
    auto low = get_key_f1(1982)->Copy();
    std::vector<Datum> low_data;
    low->DeepCopy(m_idx_1->GetKeySchema(), low_data);
    auto high = get_key_f1(1296);
    ASSERT_NO_ERROR(it = m_idx_1->StartScan(low.get(), true, high, false));
    auto ref_it_low = std::upper_bound(keys.begin(), keys.end(), get_ikey(1982));
    auto ref_it_high = std::upper_bound(keys.begin(), keys.end(), get_ikey(1296));
    auto ref_it = ref_it_low;
    bool flag = false;
    size_t cnt = 0;
    while (it->Next()) {
        auto key = read_f1_payload(it->GetCurrentItem().GetData());
        auto recid = it->GetCurrentRecordId();
        ASSERT_EQ(key, *ref_it);

        if (*ref_it == get_ikey(2020) && flag) {
            ASSERT_EQ(RecordId(101, (SlotId)((*ref_it) + 1)), recid);
        } else {
            ASSERT_EQ(RecordId(100, (SlotId)((*ref_it) + 1)), recid);

            if (*ref_it == get_ikey(2020)) {
                flag = true;
            }
        }
        ++cnt;
        ++ref_it;
    }
    ASSERT_EQ(cnt, ref_it_high - ref_it_low);

    it->EndScan();

    TDB_TEST_END
}

struct BasicTestBTreeSearch3: public TestBTree {
protected:
    void SetUp() override {
        TestEnableLogging();
        g_test_existing_db_path = absl::GetFlag(FLAGS_db_path);
        if (!g_test_existing_db_path.empty()) {
            LOG(kInfo, "Using the user provided db path" ,
                        g_test_existing_db_path);
        } else {
            g_test_existing_db_path =
                SRCDIR "/data/preloaded_btree/btree_full_scan_varlen";
            LOG(kInfo, "Using the default db path");
        }
        TestDisableLogging();

        TestBTree::SetUp();
    }
};

TEST_F(BasicTestBTreeSearch3, TestBTreeFullScanVarlen) {
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

    std::sort(keys.begin(), keys.end(),
        [](const std::pair<std::string, RecordId> &left, const std::pair<std::string, RecordId>& right) {
            return (left.first < right.first) || (left.first == right.first && left.second < right.second);
        });

    std::unique_ptr<Index::Iterator> it;
    ASSERT_NO_ERROR(it = m_idx_2->StartScan(nullptr, false, nullptr, false));
    size_t now = 0;
    while (it->Next()) {
        auto key = read_f2_payload(it->GetCurrentItem().GetData());
        auto recid = it->GetCurrentRecordId();
        ASSERT_EQ(key, keys[now].first);
        ASSERT_EQ(recid, keys[now].second);

        ++now;
    }
    ASSERT_EQ(now, keys.size());
    it->EndScan();

    TDB_TEST_END
}

struct BasicTestBTreeSearch4: public TestBTree {
protected:
    void SetUp() override {
        TestEnableLogging();
        g_test_existing_db_path = absl::GetFlag(FLAGS_db_path);
        if (!g_test_existing_db_path.empty()) {
            LOG(kInfo, "Using the user provided db path" ,
                        g_test_existing_db_path);
        } else {
            g_test_existing_db_path =
                SRCDIR "/data/preloaded_btree/btree_range_scan_varlen";
            LOG(kInfo, "Using the default db path");
        }
        TestDisableLogging();

        TestBTree::SetUp();
    }
};

TEST_F(BasicTestBTreeSearch4, TestBTreeRangeScanVarlen) {
    TDB_TEST_BEGIN

    uint32_t seed;

    // We created a dummy table to indicate this is the preloaded DB with
    // the B-tree we test on. We also store the seed as the first row in the
    // table.
    Oid dummy_tabid = g_catcache->FindTableByName("TestBTreeRangeScanVarlenDummyTable");
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

    std::mt19937 gen(seed);
    std::uniform_int_distribution<size_t> dis(190, 380);
    std::vector<std::pair<std::string, RecordId>> keys;
    for (int64_t i = 1; i <= 4000; ++i) {
        size_t len = dis(gen);
        RecordId recid{100, (SlotId)(get_ikey(i) + 1)};
        keys.push_back({cast_as_string(get_skey(i, len)), recid});
    }

    RecordId recid{123, (SlotId)(get_ikey(1993) + 1)};
    keys.push_back({cast_as_string(get_skey(1993, 301)), recid});

    RecordId recid2{321, (SlotId)(get_ikey(1993) + 1)};
    keys.push_back({cast_as_string(get_skey(1993, 273)), recid2});

    auto cmp = [](const std::pair<std::string, RecordId> &left, const std::pair<std::string, RecordId>& right) {
            return (left.first < right.first) || (left.first == right.first && left.second < right.second);
        };
    std::sort(keys.begin(), keys.end(), cmp);

    std::unique_ptr<Index::Iterator> it;
    auto low = get_key_f2(2077, 180)->Copy();
    std::vector<Datum> low_data;
    low->DeepCopy(m_idx_2->GetKeySchema(), low_data);
    auto high = get_key_f2(1069, 360);
    ASSERT_NO_ERROR(it = m_idx_2->StartScan(low.get(), false, high, true));

    auto ref_it_low = std::lower_bound(keys.begin(), keys.end(),
        std::make_pair(cast_as_string(get_skey(2077, 180)), INFINITY_RECORDID), cmp);
    auto ref_it_high = std::lower_bound(keys.begin(), keys.end(),
        std::make_pair(cast_as_string(get_skey(1069, 360)), INFINITY_RECORDID), cmp);
    auto ref_it = ref_it_low;
    size_t cnt = 0;
    while (it->Next()) {
        auto key = read_f2_payload(it->GetCurrentItem().GetData());
        auto recid = it->GetCurrentRecordId();
        ASSERT_EQ(key, ref_it->first);
        ASSERT_EQ(recid, ref_it->second);

        ++ref_it;
        ++cnt;
    }
    ASSERT_EQ(cnt, ref_it_high - ref_it_low);
    it->EndScan();

    TDB_TEST_END
}

}   // namespace taco
