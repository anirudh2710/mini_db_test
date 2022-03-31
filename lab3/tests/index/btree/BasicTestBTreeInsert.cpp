#define DEFAULT_BUFPOOL_SIZE 100
#include "index/btree/TestBTree.h"

ABSL_FLAG(int64_t, test_btree_insert_no_split_n, 200,
          "number of keys to insert in BasicTestBTreeInsert.TestBTreeInsertNoSplit");
ABSL_FLAG(int64_t, test_btree_insert_single_split_n, 400,
          "number of keys to insert in BasicTestBTreeInsert.TestBTreeInsertSingleSplit");
// + 1 key (key = get_ikey(1068))
ABSL_FLAG(int64_t, test_btree_insert_recursive_split_n, 40000,
          "number of keys to insert in BasicTestBTreeInsert.TestBTreeInsertRecursiveSplit");
ABSL_FLAG(int64_t, test_btree_insert_unique_n, 40000,
          "number of keys to insert in BasicTestBTreeInsert.TestBTreeInsertUnique");
ABSL_FLAG(int64_t, test_btree_insert_varlen_key_n, 40000,
          "number of keys to insert in BasicTestBTreeInsert.TestBTreeInsertVarlenKey");

namespace taco {

using BasicTestBTreeInsert = TestBTree;

TEST_F(BasicTestBTreeInsert, TestBTreeInsertFindSlotOnLeaf) {
    TDB_TEST_BEGIN

    {
        // creates a new leaf page
        ScopedBufferId bufid =
            CreateNewBTreePage(m_idx_1, false, true, INVALID_PID, INVALID_PID);
        char *buf = g_bufman->GetBuffer(bufid);

        VarlenDataPage pg(buf);
        maxaligned_char_buf recbuf;
        for (int64_t i = 1; i <= 100; ++i) {
            Datum d = Datum::From(i);
            IndexKey::Construct(m_index_key.get(), &d, 1);
            IndexKey *key = (IndexKey*) m_index_key.get();
            RecordId recid{100, (SlotId)((i ^ m_reorder_mask) + 1)};
            CreateLeafRecord(m_idx_1, key, recid, recbuf);
            Record rec(recbuf);
            ASSERT_NO_ERROR(pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec));
        }

        Datum d = Datum::From(55);
        RecordId new_recid{101, 1};
        IndexKey::Construct(m_index_key.get(), &d, 1);
        IndexKey *key = (IndexKey*) m_index_key.get();
        ASSERT_EQ(FindInsertionSlotIdOnLeaf(m_idx_1, key, new_recid, bufid), 56);
    }

    {
        // creates a new leaf page
        ScopedBufferId bufid =
            CreateNewBTreePage(m_idx_1_uniq, false, true, INVALID_PID, INVALID_PID);
        char *buf = g_bufman->GetBuffer(bufid);

        RecordId new_recid{101, 1};
        {
            Datum d = Datum::From(10);
            IndexKey::Construct(m_index_key.get(), &d, 1);
            IndexKey *key = (IndexKey*) m_index_key.get();
            ASSERT_EQ(FindInsertionSlotIdOnLeaf(m_idx_1_uniq, key, new_recid, bufid), 1);
        }
        // inserts 1 to 202 into the page, inserting an additional record will fail
        VarlenDataPage pg(buf);
        maxaligned_char_buf recbuf;
        for (int64_t i = 1; i <= 100; ++i) {
            if (i == 30) continue;
            Datum d = Datum::From(i);
            IndexKey::Construct(m_index_key.get(), &d, 1);
            IndexKey *key = (IndexKey*) m_index_key.get();
            RecordId recid{100, (SlotId)((i ^ m_reorder_mask) + 1)};
            CreateLeafRecord(m_idx_1_uniq, key, recid, recbuf);
            Record rec(recbuf);
            ASSERT_NO_ERROR(pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec));
        }

        {
            Datum d = Datum::From(10);
            IndexKey::Construct(m_index_key.get(), &d, 1);
            IndexKey *key = (IndexKey*) m_index_key.get();
            ASSERT_EQ(FindInsertionSlotIdOnLeaf(m_idx_1_uniq, key, new_recid, bufid), INVALID_SID);
        }

        {
            Datum d = Datum::From(30);
            IndexKey::Construct(m_index_key.get(), &d, 1);
            IndexKey *key = (IndexKey*) m_index_key.get();
            ASSERT_EQ(FindInsertionSlotIdOnLeaf(m_idx_1_uniq, key, new_recid, bufid), 30);
        }

        {
            Datum d = Datum::From(101);
            IndexKey::Construct(m_index_key.get(), &d, 1);
            IndexKey *key = (IndexKey*) m_index_key.get();
            ASSERT_EQ(FindInsertionSlotIdOnLeaf(m_idx_1_uniq, key, new_recid, bufid), 100);
        }

        for (int64_t i = 101; i <= 203; ++i) {
            Datum d = Datum::From(i);
            IndexKey::Construct(m_index_key.get(), &d, 1);
            IndexKey *key = (IndexKey*) m_index_key.get();
            RecordId recid{100, (SlotId)((i ^ m_reorder_mask) + 1)};
            CreateLeafRecord(m_idx_1, key, recid, recbuf);
            Record rec(recbuf);
            ASSERT_NO_ERROR(pg.InsertRecordAt(pg.GetMaxSlotId() + 1, rec));
        }

        {
            Datum d = Datum::From(204);
            IndexKey::Construct(m_index_key.get(), &d, 1);
            IndexKey *key = (IndexKey*) m_index_key.get();
            ASSERT_EQ(FindInsertionSlotIdOnLeaf(m_idx_1_uniq, key, new_recid, bufid), 203);
        }
    }

    TDB_TEST_END
}


TEST_F(BasicTestBTreeInsert, TestBTreeInsertOnPageNoSplit) {
    TDB_TEST_BEGIN

    // create a new leaf page
    BufferId bufid =
        CreateNewBTreePage(m_idx_1, true, true, INVALID_PID, INVALID_PID);
    PageNumber pid = g_bufman->GetPageNumber(bufid);

    RecordId new_recid {101, 1};
    Datum d = Datum::From(30);
    IndexKey::Construct(m_index_key.get(), &d, 1);
    IndexKey *key = (IndexKey*) m_index_key.get();
    maxaligned_char_buf recbuf;
    CreateLeafRecord(m_idx_1, key, new_recid, recbuf);
    SlotId insert_sid;
    ASSERT_EQ(insert_sid = FindInsertionSlotIdOnLeaf(m_idx_1_uniq, key, new_recid, bufid), 1);
    std::vector<PathItem> path = {};
    ASSERT_NO_ERROR(InsertRecordOnPage(m_idx_1, bufid, insert_sid, std::move(recbuf), std::move(path)));

    char* frame = nullptr;
    for (int64_t i = 1; i <= 202; ++i) {
        if (i == 30) continue;
        BufferId bufid = g_bufman->PinPage(pid, &frame);
        Datum d = Datum::From(i);
        IndexKey::Construct(m_index_key.get(), &d, 1);
        IndexKey *key = (IndexKey*) m_index_key.get();
        RecordId recid{100, (SlotId)((i ^ m_reorder_mask) + 1)};
        maxaligned_char_buf recbuf;
        CreateLeafRecord(m_idx_1, key, recid, recbuf);
        SlotId insert_sid;
        ASSERT_EQ(insert_sid = FindInsertionSlotIdOnLeaf(m_idx_1_uniq, key, new_recid, bufid), i);
        std::vector<PathItem> path = {};
        ASSERT_NO_ERROR(InsertRecordOnPage(m_idx_1, bufid, insert_sid, std::move(recbuf), std::move(path)));
    }

    TDB_TEST_END
}

TEST_F(BasicTestBTreeInsert, TestBTreeInsertNoSplit) {
    TDB_TEST_BEGIN

    std::vector<int64_t> keys;

    int64_t n = absl::GetFlag(FLAGS_test_btree_insert_no_split_n);
    for (int64_t i = 1; i <= n; ++i) {
        keys.emplace_back(get_ikey(i));
        RecordId recid{100, (SlotId)(get_ikey(i) + 1)};
        ASSERT_TRUE(m_idx_1->InsertKey(get_key_f1(i), recid));
    }

    ASSERT_EQ(m_idx_1->GetTreeHeight(), 1);

    std::sort(keys.begin(), keys.end());

    std::unique_ptr<Index::Iterator> it;
    ASSERT_NO_ERROR(it = m_idx_1->StartScan(nullptr, false, nullptr, false));
    size_t now = 0;
    while (it->Next()) {
        auto key = read_f1_payload(it->GetCurrentItem().GetData());
        auto recid = it->GetCurrentRecordId();
        ASSERT_EQ(key, keys[now]);
        ASSERT_EQ(RecordId(100, (SlotId)(keys[now] + 1)), recid);
        ++now;
    }
    ASSERT_EQ(now, keys.size());
    it->EndScan();

    TDB_TEST_END
}

TEST_F(BasicTestBTreeInsert, TestBTreeInsertSingleSplit) {
    TDB_TEST_BEGIN

    int64_t n = absl::GetFlag(FLAGS_test_btree_insert_single_split_n);
    std::vector<int64_t> keys;
    keys.reserve(n + 1);
    for (int64_t i = 1; i <= n; ++i) {
        keys.emplace_back(get_ikey(i));
        RecordId recid{100, (SlotId)(get_ikey(i) + 1)};
        ASSERT_TRUE(m_idx_1->InsertKey(get_key_f1(i), recid));
    }

    keys.emplace_back(get_ikey(30));
    RecordId recid{101, (SlotId)(get_ikey(30) + 1)};
    ASSERT_TRUE(m_idx_1->InsertKey(get_key_f1(30), recid));

    ASSERT_EQ(m_idx_1->GetTreeHeight(), 2);

    std::sort(keys.begin(), keys.end());

    std::unique_ptr<Index::Iterator> it;
    ASSERT_NO_ERROR(it = m_idx_1->StartScan(nullptr, false, nullptr, false));
    size_t now = 0;
    bool flag = false;
    while (it->Next()) {
        auto key = read_f1_payload(it->GetCurrentItem().GetData());
        auto recid = it->GetCurrentRecordId();
        ASSERT_EQ(key, keys[now]);

        if (keys[now] == get_ikey(30) && flag) {
            ASSERT_EQ(RecordId(101, (SlotId)(keys[now] + 1)), recid);
        } else {
            ASSERT_EQ(RecordId(100, (SlotId)(keys[now] + 1)), recid);

            if (keys[now] == get_ikey(30)) flag = true;
        }

        ++now;
    }
    ASSERT_EQ(now, keys.size());
    it->EndScan();

    TDB_TEST_END
}


TEST_F(BasicTestBTreeInsert, TestBTreeInsertRecursiveSplit) {
    TDB_TEST_BEGIN

    int64_t n = absl::GetFlag(FLAGS_test_btree_insert_recursive_split_n);
    std::vector<int64_t> keys;
    keys.reserve(n + 1);
    for (int64_t i = 1; i <= n; ++i) {
        keys.push_back(get_ikey(i));
        RecordId recid{100, (SlotId)(get_ikey(i) + 1)};
        ASSERT_TRUE(m_idx_2->InsertKey(get_key_f2(i), recid));
    }

    keys.push_back(get_ikey(1068));
    RecordId recid{101, (SlotId)(get_ikey(1068) + 1)};
    ASSERT_TRUE(m_idx_2->InsertKey(get_key_f2(1068), recid));
    ASSERT_GT(m_idx_2->GetTreeHeight(), 3);

    std::sort(keys.begin(), keys.end());

    std::unique_ptr<Index::Iterator> it;
    ASSERT_NO_ERROR(it = m_idx_2->StartScan(nullptr, false, nullptr, false));
    size_t now = 0;
    bool flag = false;
    while (it->Next()) {
        auto key = read_f2_payload(it->GetCurrentItem().GetData());
        auto recid = it->GetCurrentRecordId();
        snprintf(&m_strbuf[0], 11, "0x%08lx", keys[now]);
        m_strbuf[10] = 'A';
        auto ref_now = absl::string_view(m_strbuf.c_str(), 380);
        ASSERT_EQ(key, ref_now);

        if (keys[now] == get_ikey(1068) && flag) {
            ASSERT_EQ(RecordId(101, (SlotId)(keys[now] + 1)), recid);
        } else {
            ASSERT_EQ(RecordId(100, (SlotId)(keys[now] + 1)), recid);

            if (keys[now] == get_ikey(1068)) flag = true;
        }

        ++now;
    }
    ASSERT_EQ(now, keys.size());
    it->EndScan();

    TDB_TEST_END
}

TEST_F(BasicTestBTreeInsert, TestBTreeInsertUnique) {
    TDB_TEST_BEGIN

    int64_t n = absl::GetFlag(FLAGS_test_btree_insert_unique_n);
    std::vector<int64_t> keys;
    keys.reserve(n);
    for (int64_t i = 1; i <= n; ++i) {
        keys.push_back(get_ikey(i));
        RecordId recid{100, (SlotId)(get_ikey(i) + 1)};
        ASSERT_TRUE(m_idx_2_uniq->InsertKey(get_key_f2(i), recid));
    }

    RecordId recid{101, (SlotId)(get_ikey(1068) + 1)};
    ASSERT_FALSE(m_idx_2_uniq->InsertKey(get_key_f2(1068), recid));

    ASSERT_GT(m_idx_2_uniq->GetTreeHeight(), 1);
    std::sort(keys.begin(), keys.end());

    std::unique_ptr<Index::Iterator> it;
    ASSERT_NO_ERROR(it = m_idx_2_uniq->StartScan(nullptr, false, nullptr, false));
    size_t now = 0;
    while (it->Next()) {
        auto key = read_f2_payload(it->GetCurrentItem().GetData());
        auto recid = it->GetCurrentRecordId();
        snprintf(&m_strbuf[0], 11, "0x%08lx", keys[now]);
        m_strbuf[10] = 'A';
        auto ref_now = absl::string_view(m_strbuf.c_str(), 380);
        ASSERT_EQ(key, ref_now);
        ASSERT_EQ(RecordId(100, (SlotId)(keys[now] + 1)), recid);
        ++now;
    }
    ASSERT_EQ(now, keys.size());
    it->EndScan();

    TDB_TEST_END
}

TEST_F(BasicTestBTreeInsert, TestBTreeInsertVarlenKey) {
    TDB_TEST_BEGIN

    int64_t n = absl::GetFlag(FLAGS_test_btree_insert_varlen_key_n);
    const uint32_t seed = absl::GetFlag(FLAGS_test_btree_seed);
    std::mt19937 gen(seed);
    std::uniform_int_distribution<size_t> dis(190, 380);
    std::vector<std::pair<std::string, RecordId>> keys;
    for (int64_t i = 1; i <= n; ++i) {
        size_t len = dis(gen);
        RecordId recid{100, (SlotId)(get_ikey(i) + 1)};
        keys.push_back({cast_as_string(get_skey(i, len)), recid});
        ASSERT_TRUE(m_idx_2->InsertKey(get_key_f2(i, len), recid));
    }

    ASSERT_GT(m_idx_2->GetTreeHeight(), 3);

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

}   // namespace taco

