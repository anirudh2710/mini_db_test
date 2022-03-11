#ifndef STORAGE_TESTTABLE_COMMON_H
#error "must include storage/TestTable_common.h first"
#endif

#include <random>

#include <absl/flags/flag.h>
#include <absl/strings/str_cat.h>

#include "catalog/TableDesc.h"
#include "storage/FileManager.h"
#include "storage/VarlenDataPage.h"
#include "storage/Table.h"
#include "utils/misc.h"
#include "utils/typsupp/varchar.h"
#include "utils/zerobuf.h"


ABSL_FLAG(uint32_t, test_table_seed, 0x3f220108u,
          "The seed used for generating random records in "
          "table tests.");

namespace taco {

using RNG = std::mt19937;

class TEST_SUITE_NAME: public TEST_CLS {
protected:
    void SetUp() override {
        TEST_CLS::SetUp();

        m_dbpath = g_db->GetLastDBPath();
        m_recs.clear();
        m_rng.seed(absl::GetFlag(FLAGS_test_table_seed));
        m_recid = 0;
    }

    void TearDown() override {
        TEST_CLS::TearDown();
    }

    void ReopenDB(absl::string_view callsite_file,
                  uint64_t callsite_lineno) {
        ASSERT_NO_ERROR(g_db->close())
            << "failed to close the DB. Note called from "
            << callsite_file << ":" << callsite_lineno;
        ASSERT_NO_ERROR(g_db->open(m_dbpath, GetBufferPoolSize(),
                                    /*create=*/false, /*overwrite=*/false))
            << "failed to reopen the DB. Note called from "
            << callsite_file << ":" << callsite_lineno;
        ASSERT_TRUE(g_db->is_open());
        RecreateTabDescForDBReopen(callsite_file, callsite_lineno);
    }

    Record CreateRecord() {
        maxaligned_char_buf buf;

        std::vector<Datum> data;
        data.emplace_back(Datum::From(m_recid++));
        std::string col1_val =
            GenerateRandomAlphaNumString(m_rng, 1, s_A_coltypparam[1]);
        data.emplace_back(FunctionCallWithTypparam(
            m_varchar_infunc, s_A_coltypparam[1],
            Datum::FromCString(col1_val.c_str())));
        m_tabdesc_a->GetSchema()->WritePayloadToBuffer(data, buf);

        Record rec(buf);
        m_recs.emplace_back(std::move(buf));
        return rec;
    }

    ::testing::AssertionResult
    MatchesSomeRecord(const Record &rec, std::vector<bool> &bm) {
        try {
            Datum col0 = m_tabdesc_a->GetSchema()->GetField(0, rec.GetData());
            uint32_t col0_val = col0.GetUInt32();

            if (col0_val < m_recs.size()) {
                if ((size_t) rec.GetLength() == m_recs[col0_val].size()) {
                    if (0 == std::memcmp(rec.GetData(), m_recs[col0_val].data(),
                                         m_recs[col0_val].size())) {
                        if (!bm[col0_val]) {
                            bm[col0_val] = true;
                            return AssertionSuccess();
                        } else {
                            return AssertionFailure() <<
                                "record with key " << col0_val << " appears more "
                                "than once";
                        }
                    }
                }
            }

            Datum col1 = m_tabdesc_a->GetSchema()->GetField(1, rec.GetData());
            Datum expected_col1 = (col0_val < m_recs.size() && m_recs.size() != 0) ?
                (m_tabdesc_a->GetSchema()->GetField(1, m_recs[col0_val].data())) :
                Datum::FromNull();
            return AssertionFailure() <<
                "record doesn't match any inserted record:\ngot (" << col0_val
                << ", \"" << varchar_to_string_view(col1) << "\"[len = "
                << varchar_to_string_view(col1).length() << "])[total_len="
                << rec.GetLength() << "]\n"
                << ((col0_val >= m_recs.size())
                    ? "but there's no record with matching key "
                    : ((m_recs[col0_val].size() == 0) ? "but should have been deleted"
                        : absl::StrCat("but expecting (\"", col0_val, ", \"",
                        varchar_to_string_view(expected_col1), "\"[len = ",
                        varchar_to_string_view(expected_col1).length(), "])"
                        "[total_len = ", m_recs[col0_val].size(), "]")));
        } catch (const TDBError &e) {
            return AssertionFailure() << e.GetMessage();
        }
    }

    void CheckAllRecords(absl::string_view callsite_file,
                         uint64_t callsite_lineno,
                         Table *tab,
                         uint32_t ndeleted = 0,
                         Table::Iterator iter = Table::Iterator(),
                         std::vector<bool> *p_bm = nullptr) {
        std::vector<bool> local_bm;
        std::vector<bool> &bm = (p_bm) ? *p_bm : local_bm;
        bm.resize(0);
        bm.resize(m_recs.size(), false);
        uint32_t n = 0;
        if (!iter.GetTable()) {
            ASSERT_NO_ERROR(iter = tab->StartScan());
        }
        for (;;) {
            bool has_next;
            ASSERT_NO_ERROR(has_next = iter.Next());
            if (!has_next)
                break;
            ++n;
            ASSERT_LE(n, m_recs.size());

            ASSERT_TRUE(iter.IsAtValidRecord());
            ASSERT_TRUE(iter.GetCurrentRecordId().IsValid())
                << iter.GetCurrentRecordId().ToString();

            const Record &rec = iter.GetCurrentRecord();
            EXPECT_TRUE(MatchesSomeRecord(rec, bm));
        }
        EXPECT_EQ(n + ndeleted, m_recs.size());
        ASSERT_NO_ERROR(iter.EndScan());
    }

    RNG m_rng;
    std::string m_dbpath;
    std::vector<maxaligned_char_buf> m_recs;
    uint32_t m_recid;
};

}   // namespacd taco

namespace taco {

#define ReopenDB(...) ReopenDB(__FILE__, \
                               __LINE__ \
                               IF_NONEMPTY_COMMA(__VA_ARGS__, ) \
                               __VA_ARGS__)
#define CheckAllRecords(...) CheckAllRecords(__FILE__, __LINE__, __VA_ARGS__)

TEST_F(TEST_SUITE_NAME, TestEmptyTable) {
    TDB_TEST_BEGIN

    ASSERT_NO_ERROR(Table::Initialize(m_tabdesc_a.get()));
    std::unique_ptr<Table> tab;
    ASSERT_NO_ERROR(tab = Table::Create(m_tabdesc_a));
    EXPECT_NO_ERROR(tab.reset());

    ReopenDB();
    ASSERT_NO_ERROR(tab = Table::Create(m_tabdesc_a));

    Table::Iterator iter;
    ASSERT_NO_ERROR(iter = tab->StartScan());
    EXPECT_FALSE(iter.Next());
    ASSERT_NO_ERROR(iter.EndScan());

    // make sure iterator can be safely destructed after EndScan() call,
    // even if the table has been destructed.
    EXPECT_NO_ERROR(tab.reset());

    TDB_TEST_END
}

TEST_F(TEST_SUITE_NAME, TestInsertRecord) {
    TDB_TEST_BEGIN

    ASSERT_NO_ERROR(Table::Initialize(m_tabdesc_a.get()));
    std::unique_ptr<Table> tab;
    ASSERT_NO_ERROR(tab = Table::Create(m_tabdesc_a));

    // inserting one record
    Record rec0 = CreateRecord();
    ASSERT_NO_ERROR(tab->InsertRecord(rec0));
    CheckAllRecords(tab.get());

    // inserting a few more records
    for (uint32_t i = 2; i <= 1000; ++i) {
        Record rec = CreateRecord();
        ASSERT_NO_ERROR(tab->InsertRecord(rec));
    }
    CheckAllRecords(tab.get());

    // insert something that doesn't fit
    Record rec_too_long(g_zerobuf, PAGE_SIZE);
    for (uint32_t i = 1; i <= 10; ++i) {
        ASSERT_REGULAR_ERROR(tab->InsertRecord(rec_too_long));
    }
    CheckAllRecords(tab.get());

    // insert a few more records
    for (uint32_t i = 1001; i <= 2000; ++i) {
        Record rec = CreateRecord();
        ASSERT_NO_ERROR(tab->InsertRecord(rec));
    }
    CheckAllRecords(tab.get());

    // reopen the database and check again
    ASSERT_NO_ERROR(tab.reset());
    ReopenDB();
    ASSERT_NO_ERROR(tab = Table::Create(m_tabdesc_a));
    CheckAllRecords(tab.get());

    // insert a few more records
    for (uint32_t i = 2001; i <= 3000; ++i) {
        Record rec = CreateRecord();
        ASSERT_NO_ERROR(tab->InsertRecord(rec));
    }
    CheckAllRecords(tab.get());

    TDB_TEST_END
}

TEST_F(TEST_SUITE_NAME, TestEraseRecord) {
    TDB_TEST_BEGIN

    ASSERT_NO_ERROR(Table::Initialize(m_tabdesc_a.get()));
    std::unique_ptr<Table> tab;
    ASSERT_NO_ERROR(tab = Table::Create(m_tabdesc_a));

    std::vector<RecordId> recids;

    // insert a few records
    for (uint32_t i = 0; i <= 1024; ++i) {
        Record rec = CreateRecord();
        ASSERT_NO_ERROR(tab->InsertRecord(rec)) << i;
        ASSERT_TRUE(rec.GetRecordId().IsValid()) << i;
        recids.push_back(rec.GetRecordId());
    }

    // erase a few records
    uint32_t ndeleted = 0;
    for (uint32_t i = 0; i < recids.size(); i += 16) {
        EXPECT_NO_ERROR(tab->EraseRecord(recids[i])) << i;
        m_recs[i].clear();
        ++ndeleted;
    }
    CheckAllRecords(tab.get(), ndeleted);

    // erasing those records again should end up with errors
    for (uint32_t i = 0; i < recids.size(); i += 16) {
        EXPECT_ANY_ERROR(tab->EraseRecord(recids[i])) << i;
    }
    CheckAllRecords(tab.get(), ndeleted);

    // reopen the database and check again
    tab.reset();
    ReopenDB();
    ASSERT_NO_ERROR(tab = Table::Create(m_tabdesc_a));
    CheckAllRecords(tab.get(), ndeleted);

    // insert a few more records
    for (uint32_t i = 0; i < 1024; ++i) {
        Record rec = CreateRecord();
        ASSERT_NO_ERROR(tab->InsertRecord(rec)) << i;
        ASSERT_TRUE(rec.GetRecordId().IsValid()) << i;
        recids.push_back(rec.GetRecordId());
    }
    CheckAllRecords(tab.get(), ndeleted);

    // erasing all of the first 1024 records
    for (uint32_t i = 0; i <= 1024; ++i) {
        if (i % 16 != 0) {
            EXPECT_NO_ERROR(tab->EraseRecord(recids[i])) << i;
            ++ndeleted;
        }
    }
    CheckAllRecords(tab.get(), ndeleted);

    // try to delete something not in this heap file
    std::unique_ptr<File> f = g_fileman->Open(NEW_REGULAR_FID);
    PageNumber pid_in_another_file = f->GetFirstPageNumber();
    ASSERT_NO_ERROR(f->Close());

    // initialize a data page at the beginning of the new file and inserts
    // one record.
    RecordId recid_in_another_file;
    recid_in_another_file.pid = pid_in_another_file;
    {
        char *pagebuf;
        ScopedBufferId bufid = g_bufman->PinPage(pid_in_another_file, &pagebuf);
        VarlenDataPage::InitializePage(pagebuf);
        VarlenDataPage dp(pagebuf);
        Record rec(m_recs[1]);
        ASSERT_TRUE(dp.InsertRecord(rec));
        recid_in_another_file.sid = rec.GetRecordId().sid;
    }
    ASSERT_TRUE(recid_in_another_file.IsValid());
    EXPECT_REGULAR_ERROR(tab->EraseRecord(recid_in_another_file))
        << "erasing a record ID in a different file should be an error";
    CheckAllRecords(tab.get(), ndeleted);

    // remove all records and check again
    for (uint32_t i = 1025; i < recids.size(); ++i) {
        ASSERT_NO_ERROR(tab->EraseRecord(recids[i])) << i;
        ++ndeleted;
    }
    CheckAllRecords(tab.get(), ndeleted);

    // reopen and check once more
    tab.reset();
    ReopenDB();
    ASSERT_NO_ERROR(tab = Table::Create(m_tabdesc_a));
    CheckAllRecords(tab.get(), ndeleted);

    // make sure the file is now empty
    std::unique_ptr<File> f2 =
        g_fileman->Open(m_tabdesc_a->GetTableEntry()->tabfid());
    EXPECT_EQ(f2->GetFirstPageNumber(), f2->GetLastPageNumber());

    TDB_TEST_END
}

TEST_F(TEST_SUITE_NAME, TestUpdateRecord) {
    TDB_TEST_BEGIN

    ASSERT_NO_ERROR(Table::Initialize(m_tabdesc_a.get()));
    std::unique_ptr<Table> tab;
    ASSERT_NO_ERROR(tab = Table::Create(m_tabdesc_a));

    std::vector<RecordId> recids;

    // insert a few records
    for (uint32_t i = 0; i <= 1024; ++i) {
        Record rec = CreateRecord();
        ASSERT_NO_ERROR(tab->InsertRecord(rec)) << i;
        ASSERT_TRUE(rec.GetRecordId().IsValid()) << i;
        recids.push_back(rec.GetRecordId());
    }

    // update a few records in place in each page
    std::vector<uint32_t> idx_by_rid;
    idx_by_rid.reserve(1025);
    for (uint32_t i = 0; i <= 1024; ++i) {
        idx_by_rid.push_back(i);
    }
    std::sort(idx_by_rid.begin(), idx_by_rid.end(),
        [&](uint32_t i1, uint32_t i2) -> bool {
            const RecordId &r1 = recids[i1];
            const RecordId &r2 = recids[i2];
            return r1.pid< r2.pid || (r1.pid == r2.pid && r1.sid < r2.sid);
        });
    for (uint32_t i = 0; i <= 1024;) {
        uint32_t j = i;
        while (j <= 1024 &&
               recids[idx_by_rid[i]].pid == recids[idx_by_rid[j]].pid) {
            ++j;
        }
        // update something in the middle
        uint32_t k = (i + j) >> 1;
        uint32_t idx_to_update = idx_by_rid[k];
        const RecordId &rid_to_update = recids[idx_to_update];

        std::vector<Datum> data;
        data.emplace_back(m_tabdesc_a->GetSchema()
            ->GetField(0, m_recs[idx_to_update].data()));
        Datum old_col1 = m_tabdesc_a->GetSchema()
            ->GetField(1, m_recs[idx_to_update].data());
        size_t old_col1_len = varchar_to_string_view(old_col1).length();
        // generate a new string
        std::string new_col1_val =
            GenerateRandomAlphaNumString(m_rng, old_col1_len, old_col1_len);
        data.emplace_back(FunctionCallWithTypparam(
            m_varchar_infunc, s_A_coltypparam[1],
            Datum::FromCString(new_col1_val.c_str())));

        maxaligned_char_buf buf;
        buf.reserve(m_recs[idx_to_update].size());
        m_tabdesc_a->GetSchema()->WritePayloadToBuffer(data, buf);
        m_recs[idx_to_update].swap(buf);

        Record new_rec(m_recs[idx_to_update]);
        ASSERT_NO_ERROR(tab->UpdateRecord(rid_to_update, new_rec));
        ASSERT_EQ(new_rec.GetRecordId(), rid_to_update)
            << "expecting in place update of a record of the same length "
            << idx_to_update;
        i = j;
    }
    CheckAllRecords(tab.get());

    // reopen and check again
    ASSERT_NO_ERROR(tab.reset());
    ReopenDB();
    ASSERT_NO_ERROR(tab = Table::Create(m_tabdesc_a));
    CheckAllRecords(tab.get());

    // try to update a few records with something that is too long to fit on an
    // empty page, which should be rejected without changing the set of
    // records in the file
    Record rec_too_long(g_zerobuf, PAGE_SIZE);
    for (uint32_t i = 0; i <= 1024; ++i) {
        if (i % 16 != 0) {
            continue;
        }
        ASSERT_REGULAR_ERROR(tab->UpdateRecord(recids[i], rec_too_long));
    }
    CheckAllRecords(tab.get());

    // insert a few new records here so that we have the last record inserted
    // is on a new page
    std::unordered_set<PageNumber> existing_pid;
    for (uint32_t i = 0; i < recids.size(); ++i) {
        existing_pid.emplace(recids[i].pid);
    }
    uint32_t n_new_recs = 0;
    for (; n_new_recs < PAGE_SIZE; ++n_new_recs) {
        Record rec = CreateRecord();
        ASSERT_NO_ERROR(tab->InsertRecord(rec)) << n_new_recs;
        ASSERT_TRUE(rec.GetRecordId().IsValid()) << n_new_recs;
        if (existing_pid.count(rec.GetRecordId().pid) == 0) {
            break;
        }
    }
    ASSERT_LT(n_new_recs, PAGE_SIZE)
        << "too many insertions needed to fill the existing pages in the file";

    // find the maximum possible record length that could possibly go on a
    // VarlenDataPage. The initial number is expected to be exactly the largest
    // size that could fit unless VarlenDataPageHeader is changed.
    FieldOffset max_reclen =
        PAGE_SIZE - MAXALIGN(8 + sizeof(PageHeaderData)) - MAXALIGN_OF;
    {
        unique_malloced_ptr pbuf(aligned_alloc(8, PAGE_SIZE));
        bool max_reclen_ok;
        {
            memset(pbuf.get(), 0, PAGE_SIZE);
            VarlenDataPage::InitializePage((char*) pbuf.get());
            VarlenDataPage dp((char*) pbuf.get());
            Record rec(g_zerobuf, max_reclen);
            bool res;
            ASSERT_NO_ERROR(res = dp.InsertRecord(rec));
            max_reclen_ok = res && rec.GetRecordId().sid != INVALID_SID;
        }
        for (; max_reclen >= 8 && max_reclen <= (FieldOffset) PAGE_SIZE;) {
            FieldOffset alt_reclen = max_reclen + ((max_reclen_ok) ? 8 : -8);
            Record rec(g_zerobuf, alt_reclen);
            memset(pbuf.get(), 0, PAGE_SIZE);
            VarlenDataPage::InitializePage((char*) pbuf.get());
            VarlenDataPage dp((char*) pbuf.get());
            bool res;
            ASSERT_NO_ERROR(res = dp.InsertRecord(rec));
            bool alt_reclen_ok = res && rec.GetRecordId().sid != INVALID_SID;
            if (max_reclen_ok ^ alt_reclen_ok) {
                max_reclen = (max_reclen_ok) ? max_reclen : alt_reclen;
                break;
            }
        }
        ASSERT_TRUE(max_reclen >= 8 && max_reclen <= (FieldOffset) PAGE_SIZE)
            << max_reclen;
    }
    LOG(kInfo, "max_reclen = " FIELDOFFSET_FORMAT, max_reclen);


    // Try to update some of the records to a record that is definitely too
    // long to fit on the original page, this will create a new page for
    // insertion.
    for (uint32_t i = 1; i < recids.size(); i += 64) {
        // We are using memcmp to compare the records, and thus any bytes after
        // the actual record payload can be found by that. Schema doesn't
        // care about excessive bytes and won't complain if asked to fetch
        // the individual fields.
        ASSERT_GE(max_reclen, m_recs[i].size());
        m_recs[i].resize(max_reclen, 0xd7);

        Record new_rec(m_recs[i]);
        ASSERT_NO_ERROR(tab->UpdateRecord(recids[i], new_rec));
        // shouldn't be in place
        ASSERT_NE(new_rec.GetRecordId(), recids[i]);
    }
    CheckAllRecords(tab.get());

    // reopen and check once more
    ASSERT_NO_ERROR(tab.reset());
    ReopenDB();
    ASSERT_NO_ERROR(tab = Table::Create(m_tabdesc_a));
    CheckAllRecords(tab.get());

    // try to update something not in this heap file
    std::unique_ptr<File> f = g_fileman->Open(NEW_REGULAR_FID);
    PageNumber pid_in_another_file = f->GetFirstPageNumber();
    ASSERT_NO_ERROR(f->Close());

    // initialize a data page at the beginning of the new file and inserts
    // one record.
    RecordId recid_in_another_file;
    recid_in_another_file.pid = pid_in_another_file;
    {
        char *pagebuf;
        ScopedBufferId bufid = g_bufman->PinPage(pid_in_another_file, &pagebuf);
        VarlenDataPage::InitializePage(pagebuf);
        VarlenDataPage dp(pagebuf);
        Record rec(m_recs[1]);
        ASSERT_TRUE(dp.InsertRecord(rec));
        recid_in_another_file.sid = rec.GetRecordId().sid;
    }
    Record new_rec(m_recs[2]);
    EXPECT_REGULAR_ERROR(tab->UpdateRecord(recid_in_another_file, new_rec))
        << "updating a record ID in a different file should be an error";
    CheckAllRecords(tab.get());

    TDB_TEST_END
}

TEST_F(TEST_SUITE_NAME, TestStartScanFrom) {
    TDB_TEST_BEGIN

    ASSERT_NO_ERROR(Table::Initialize(m_tabdesc_a.get()));
    std::unique_ptr<Table> tab;
    ASSERT_NO_ERROR(tab = Table::Create(m_tabdesc_a));

    std::vector<RecordId> recids;

    // insert a few records
    for (uint32_t i = 0; i <= 1024; ++i) {
        Record rec = CreateRecord();
        ASSERT_NO_ERROR(tab->InsertRecord(rec)) << i;
        ASSERT_TRUE(rec.GetRecordId().IsValid()) << i;
        recids.push_back(rec.GetRecordId());
    }
    CheckAllRecords(tab.get());

    // Our file manager should return monotonically increasing page
    // numbers at this time, so the tuples should be iterated in the record id
    // order per spec.
    std::vector<uint32_t> idx_by_rid;
    idx_by_rid.reserve(1025);
    for (uint32_t i = 0; i <= 1024; ++i) {
        idx_by_rid.push_back(i);
    }
    std::sort(idx_by_rid.begin(), idx_by_rid.end(),
        [&](uint32_t i1, uint32_t i2) -> bool {
            const RecordId &r1 = recids[i1];
            const RecordId &r2 = recids[i2];
            return r1.pid< r2.pid || (r1.pid == r2.pid && r1.sid < r2.sid);
        });

    // iterate the files from a few points in the middle
    for (uint32_t i = 0; i <= recids.size(); i += 128) {
        RecordId start_rid = recids[idx_by_rid[i]];
        Table::Iterator iter;
        ASSERT_NO_ERROR(iter = tab->StartScanFrom(start_rid));
        ASSERT_NE(iter.GetTable(), nullptr);
        std::vector<bool> bm;
        CheckAllRecords(tab.get(),
            /*ndeleted=*/i,
            std::move(iter),
            &bm);
        ASSERT_EQ(bm.size(), recids.size());
        for (uint32_t j = 0; j < bm.size(); ++j) {
            if (bm[j]) {
                EXPECT_GE(recids[j], start_rid);
            } else {
                EXPECT_LT(recids[j], start_rid);
            }
        }
    }

    TDB_TEST_END
}

}   // namespace taco
