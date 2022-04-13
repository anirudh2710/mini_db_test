#include "base/TDBDBTest.h"

#include "catalog/CatCache.h"
#include "catalog/TableDesc.h"
#include "plan/TableScan.h"
#include "storage/VarlenDataPage.h"
#include "execution/TableScanState.h"
#include "utils/builtin_funcs.h"
#include "utils/typsupp/varchar.h"

namespace taco {

class BasicTestTableScan: public TDBDBTest {
protected:
    void
    SetUp() override {
        TDBDBTest::SetUp();

        std::shared_ptr<const SysTable_Type> typ =
            g_db->catcache()->FindType(initoids::TYP_VARCHAR);
        ASSERT_NE(typ.get(), nullptr);
        FunctionInfo finfo = FindBuiltinFunction(typ->typinfunc());
        ASSERT_TRUE((bool) finfo);
        m_varchar_infunc = finfo;
    }

    void
    TearDown() override {

        TDBDBTest::TearDown();
    }

    Datum
    ExpectedValueF0(size_t n, bool isnullable) {
        if (isnullable && (n & 1)) {
            return Datum::FromNull();
        }
        return Datum::From(n);
    }

    Datum
    ExpectedValueF1(size_t n, bool isnullable) {
        if (isnullable && (n & 2)) {
            return Datum::FromNull();
        }
        std::string value = std::to_string(n * 10);
        if (value.size() > m_f1_typparam) {
            value = value.substr(0, m_f1_typparam);
        }
        Datum input_datum = Datum::FromCString(value.c_str());
        return FunctionCallWithTypparam(m_varchar_infunc,
                                        m_f1_typparam,
                                        input_datum);
    }

    void
    CreateNewTable(bool isnullable, size_t num_records,
                   std::shared_ptr<const TableDesc> &tabdesc) {
        g_db->CreateTable(
            "TableA",
            {initoids::TYP_UINT4,
             initoids::TYP_VARCHAR},
            {0, m_f1_typparam},
            {},
            {isnullable, isnullable});

        Oid tabid = g_catcache->FindTableByName("TableA");
        ASSERT_NE(tabid, InvalidOid);
        tabdesc = g_catcache->FindTableDesc(tabid);

        if (num_records > 0) {
            std::unique_ptr<Table> tab = Table::Create(tabdesc);
            const Schema *schema = tabdesc->GetSchema();

            // collecting input functions
            std::vector<FunctionInfo> infuncs;
            std::vector<uint64_t> typparam;
            infuncs.reserve(2);
            typparam.reserve(2);
            for (FieldId i = 0; i < 2; ++i) {
                Oid typid = schema->GetFieldTypeId(i);
                typparam.push_back(schema->GetFieldTypeParam(i));
                std::shared_ptr<const SysTable_Type> typ =
                    g_db->catcache()->FindType(typid);
                ASSERT_NE(typ.get(), nullptr);
                FunctionInfo finfo = FindBuiltinFunction(typ->typinfunc());
                ASSERT_TRUE((bool) finfo);
                infuncs.push_back(finfo);
            }

            // insertion

            std::vector<Datum> data;
            data.reserve(2);
            maxaligned_char_buf buf;
            buf.reserve(128);

            auto prepare_recordbuf = [&](uint32_t n) {
                data.clear();
                data.emplace_back(ExpectedValueF0(n, isnullable));
                data.emplace_back(ExpectedValueF1(n, isnullable));
                buf.clear();
                schema->WritePayloadToBuffer(data, buf);
            };

            for (size_t i = 0; i < num_records; ++i) {
                prepare_recordbuf(i);
                Record rec(buf);
                tab->InsertRecord(rec);
            }
        }
    }

    FunctionInfo m_varchar_infunc;
    static constexpr uint64_t m_f1_typparam = 2;
};

TEST_F(BasicTestTableScan, TestScanEmptyTable) {
    TDB_TEST_BEGIN

    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(false, 0, tabdesc);
    std::unique_ptr<TableScan> scan = TableScan::Create(tabdesc);
    ASSERT_NE(scan.get(), nullptr)
        << "failed to create a table scan";

    std::unique_ptr<PlanExecState> scanstate = scan->create_exec_state();
    ASSERT_NE(scanstate.get(), nullptr)
        << "failed to create the table scan state";

    scanstate->init();
    EXPECT_FALSE(scanstate->next_tuple());
    EXPECT_FALSE(scanstate->next_tuple());
    scanstate->close();

    TDB_TEST_END
}

TEST_F(BasicTestTableScan, TestScanOneRecord) {
    TDB_TEST_BEGIN

    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(false, 1, tabdesc);
    std::unique_ptr<TableScan> scan = TableScan::Create(tabdesc);
    ASSERT_NE(scan.get(), nullptr)
        << "failed to create a table scan";

    std::unique_ptr<PlanExecState> scanstate = scan->create_exec_state();
    ASSERT_NE(scanstate.get(), nullptr)
        << "failed to create the table scan state";

    scanstate->init();
    ASSERT_TRUE(scanstate->next_tuple());

    auto f0 = scanstate->get_record()[0];
    EXPECT_FALSE(f0.isnull());
    uint32_t f0_value = f0.GetUInt32();
    EXPECT_EQ(f0_value, ExpectedValueF0(0, false).GetUInt32());

    auto f1 = scanstate->get_record()[1];
    EXPECT_FALSE(f1.isnull());
    absl::string_view f1_value = varchar_to_string_view(f1);
    Datum f1_expected = ExpectedValueF1(0, false);
    absl::string_view f1_expected_value = varchar_to_string_view(f1_expected);
    EXPECT_EQ(f1_value, f1_expected_value);

    ASSERT_FALSE(scanstate->next_tuple());
    ASSERT_FALSE(scanstate->next_tuple());

    scanstate->close();
    TDB_TEST_END
}

TEST_F(BasicTestTableScan, TestScanManyRecords) {
    TDB_TEST_BEGIN

    const size_t n = 5000;
    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(false, n, tabdesc);
    std::unique_ptr<TableScan> scan = TableScan::Create(tabdesc);
    ASSERT_NE(scan.get(), nullptr);

    std::unique_ptr<PlanExecState> scanstate = scan->create_exec_state();
    scanstate->init();

    std::vector<bool> found(n, false);
    size_t n_found = 0;
    for (size_t i = 0; i < n; ++i) {
        ASSERT_TRUE(scanstate->next_tuple()) << i;

        std::vector<NullableDatumRef> rec = scanstate->get_record();
        ASSERT_EQ(rec.size(), (size_t) 2);

        ASSERT_FALSE(rec[0].isnull());
        uint32_t f0_val = rec[0].GetUInt32();
        ASSERT_FALSE(rec[1].isnull());
        absl::string_view f1_val = varchar_to_string_view(rec[1]);

        ASSERT_GE(f0_val, 0) << i;
        ASSERT_LT(f0_val, n) << i;

        ASSERT_FALSE(found[f0_val]) <<
            "the same record has appeared more than once: ("
            << f0_val << ", " << f1_val << ")";
        found[f0_val] = true;
        ++n_found;

        Datum f1_expected = ExpectedValueF1(f0_val, false);
        absl::string_view f1_expected_val = varchar_to_string_view(f1_expected);
        ASSERT_EQ(f1_val, f1_expected_val);
    }

    ASSERT_FALSE(scanstate->next_tuple());
    ASSERT_FALSE(scanstate->next_tuple());
    ASSERT_EQ(n_found, n);

    scanstate->close();

    TDB_TEST_END
}

TEST_F(BasicTestTableScan, TestScanEndAndReinit) {
    TDB_TEST_BEGIN

    size_t n = 5000;
    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(false, n, tabdesc);
    std::unique_ptr<TableScan> scan = TableScan::Create(tabdesc);
    ASSERT_NE(scan.get(), nullptr);

    std::unique_ptr<PlanExecState> scanstate = scan->create_exec_state();
    scanstate->init();

    std::vector<bool> found(n, false);
    size_t n_found = 0;
    for (size_t i = 0; i < n / 2; ++i) {
        ASSERT_TRUE(scanstate->next_tuple()) << i;

        std::vector<NullableDatumRef> rec = scanstate->get_record();
        ASSERT_EQ(rec.size(), (size_t) 2);

        ASSERT_FALSE(rec[0].isnull());
        uint32_t f0_val = rec[0].GetUInt32();
        ASSERT_FALSE(rec[1].isnull());
        absl::string_view f1_val = varchar_to_string_view(rec[1]);

        ASSERT_GE(f0_val, 0) << i;
        ASSERT_LT(f0_val, n) << i;

        ASSERT_FALSE(found[f0_val]) <<
            "the same record has appeared more than once: ("
            << f0_val << ", " << f1_val << ")";
        found[f0_val] = true;
        ++n_found;

        Datum f1_expected = ExpectedValueF1(f0_val, false);
        absl::string_view f1_expected_val = varchar_to_string_view(f1_expected);
        ASSERT_EQ(f1_val, f1_expected_val);
    }

    ASSERT_EQ(n_found, n / 2);

    // do another scan, make sure we find everything
    scanstate->close();
    scanstate->init();

    found.resize(0);
    found.resize(n, false);
    n_found = 0;
    for (size_t i = 0; i < n; ++i) {
        ASSERT_TRUE(scanstate->next_tuple()) << i;

        std::vector<NullableDatumRef> rec = scanstate->get_record();
        ASSERT_EQ(rec.size(), (size_t) 2);

        ASSERT_FALSE(rec[0].isnull());
        uint32_t f0_val = rec[0].GetUInt32();
        ASSERT_FALSE(rec[1].isnull());
        absl::string_view f1_val = varchar_to_string_view(rec[1]);

        ASSERT_GE(f0_val, 0) << i;
        ASSERT_LT(f0_val, n) << i;

        ASSERT_FALSE(found[f0_val]) <<
            "the same record has appeared more than once: ("
            << f0_val << ", " << f1_val << ")";
        found[f0_val] = true;
        ++n_found;

        Datum f1_expected = ExpectedValueF1(f0_val, false);
        absl::string_view f1_expected_val = varchar_to_string_view(f1_expected);
        ASSERT_EQ(f1_val, f1_expected_val);
    }

    ASSERT_FALSE(scanstate->next_tuple());
    ASSERT_FALSE(scanstate->next_tuple());
    ASSERT_EQ(n_found, n);

    scanstate->close();

    TDB_TEST_END
}

TEST_F(BasicTestTableScan, TestScanRewind) {
    TDB_TEST_BEGIN

    size_t n = 5000;
    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(false, n, tabdesc);
    std::unique_ptr<TableScan> scan = TableScan::Create(tabdesc);
    ASSERT_NE(scan.get(), nullptr);

    std::unique_ptr<PlanExecState> scanstate = scan->create_exec_state();
    scanstate->init();

    std::vector<bool> found(n, false);
    size_t round = 0;
    for (size_t rewind_pos : {(size_t) 0,
                              (size_t) 1, n / 4, n / 2, n - 1, n, n}) {
        size_t n_found = 0;
        found.resize(0);
        found.resize(n, false);

        for (size_t i = 0; i < rewind_pos; ++i) {
            ASSERT_TRUE(scanstate->next_tuple()) << i;

            std::vector<NullableDatumRef> rec = scanstate->get_record();
            ASSERT_EQ(rec.size(), (size_t) 2);

            ASSERT_FALSE(rec[0].isnull());
            uint32_t f0_val = rec[0].GetUInt32();
            ASSERT_FALSE(rec[1].isnull());
            absl::string_view f1_val = varchar_to_string_view(rec[1]);

            ASSERT_GE(f0_val, 0) << i;
            ASSERT_LT(f0_val, n) << i;

            ASSERT_FALSE(found[f0_val]) <<
                "the same record has appeared more than once: ("
                << f0_val << ", " << f1_val << ")";
            found[f0_val] = true;
            ++n_found;

            Datum f1_expected = ExpectedValueF1(f0_val, false);
            absl::string_view f1_expected_val = varchar_to_string_view(f1_expected);
            ASSERT_EQ(f1_val, f1_expected_val);
        }

        ASSERT_EQ(n_found, rewind_pos);
        if (rewind_pos == n) {
            ASSERT_FALSE(scanstate->next_tuple());
            ASSERT_FALSE(scanstate->next_tuple());
        }

        ASSERT_NO_ERROR(scanstate->rewind()) << round;
        ++round;
    }

    scanstate->close();

    TDB_TEST_END
}

TEST_F(BasicTestTableScan, TestScanOnTableWithHoles) {
    TDB_TEST_BEGIN

    const size_t n = 5000;

    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(false, n, tabdesc);
    std::unique_ptr<TableScan> scan = TableScan::Create(tabdesc);
    ASSERT_NE(scan.get(), nullptr);

    // Now let's do a few edits to the file.
    // Note that we're expecting at least 10 pages here, because 5000 / (4096 /
    // 8) > 9.7 (could be more).
    // 1) empty the first page
    // 2) empty the 5th page in the middle
    // 3) empty the last.
    // 4) make a few unoccupied slots on each of the pages other than the pages
    // listed above.

    std::unique_ptr<File> f =
        g_fileman->Open(tabdesc->GetTableEntry()->tabfid());
    PageNumber pid = f->GetFirstPageNumber();
    size_t npages_with_unoccupied_slots = 0;
    size_t n_erased = 0;
    size_t npages;
    while (pid != INVALID_PID) {
        char *buf;
        ScopedBufferId bufid = g_bufman->PinPage(pid, &buf);
        if (npages == 0 || npages == 4) {
            memset(buf + sizeof(PageHeaderData), 0,
                   PAGE_SIZE - sizeof(PageHeaderData));
            VarlenDataPage dp(buf);
            n_erased += dp.GetRecordCount();
            VarlenDataPage::InitializePage(buf);
        } else {
            VarlenDataPage dp(buf);
            if (dp.GetMaxSlotId() > 2) {
                if (n & 1) {
                    dp.EraseRecord(dp.GetMinSlotId());
                    ++n_erased;
                } else {
                    dp.EraseRecord(dp.GetMaxSlotId());
                    ++n_erased;
                }

                if (dp.GetMaxSlotId() > 2) {
                    dp.EraseRecord((dp.GetMinSlotId() + dp.GetMaxSlotId()) / 2);
                    ++n_erased;
                }
                ++npages_with_unoccupied_slots;
            } else if (dp.GetMaxSlotId() == 2) {
                dp.EraseRecord(dp.GetMinSlotId());
                ++n_erased;
                ++npages_with_unoccupied_slots;
            }
        }
        g_bufman->MarkDirty(bufid);
        pid = ((PageHeaderData*) buf)->GetNextPageNumber();
        ++npages;
    }

    // We're expecting at least 10 pages in the file.
    ASSERT_GE(npages, 10);

    if (npages_with_unoccupied_slots < 8) {
        TestEnableLogging();
        LOG(kWarning, "We were xpecting a more compact heap file than we had. "
                      "We were only able to make %lu additional pages "
                      "with unoccupied slots other than the two empty pages.",
                      npages_with_unoccupied_slots);
        TestDisableLogging();
    }

    // scan the file and check if we can successfully iterate over everything
    // that's left in the file.
    std::unique_ptr<PlanExecState> scanstate = scan->create_exec_state();
    scanstate->init();

    std::vector<bool> found(n, false);
    size_t n_found = 0;
    for (size_t i = 0; i < n - n_erased; ++i) {
        ASSERT_TRUE(scanstate->next_tuple()) << i;

        std::vector<NullableDatumRef> rec = scanstate->get_record();
        ASSERT_EQ(rec.size(), (size_t) 2);

        ASSERT_FALSE(rec[0].isnull());
        uint32_t f0_val = rec[0].GetUInt32();
        ASSERT_FALSE(rec[1].isnull());
        absl::string_view f1_val = varchar_to_string_view(rec[1]);

        ASSERT_GE(f0_val, 0) << i;
        ASSERT_LT(f0_val, n) << i;

        ASSERT_FALSE(found[f0_val]) <<
            "the same record has appeared more than once: ("
            << f0_val << ", " << f1_val << ")";
        found[f0_val] = true;
        ++n_found;

        Datum f1_expected = ExpectedValueF1(f0_val, false);
        absl::string_view f1_expected_val = varchar_to_string_view(f1_expected);
        ASSERT_EQ(f1_val, f1_expected_val);
    }

    ASSERT_FALSE(scanstate->next_tuple());
    ASSERT_FALSE(scanstate->next_tuple());
    ASSERT_EQ(n_found, n - n_erased);

    scanstate->close();

    TDB_TEST_END
}

TEST_F(BasicTestTableScan, TestScanAndRewindOnTableWithHoles) {
    TDB_TEST_BEGIN

    const size_t n = 5000;

    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(false, n, tabdesc);
    std::unique_ptr<TableScan> scan = TableScan::Create(tabdesc);
    ASSERT_NE(scan.get(), nullptr);

    // Now let's do a few edits to the file.
    // Note that we're expecting at least 10 pages here, because 5000 / (4096 /
    // 8) > 9.7 (could be more).
    // 1) empty the first page
    // 2) empty the 5th page in the middle
    // 3) empty the last.
    // 4) make a few unoccupied slots on each of the pages other than the pages
    // listed above.

    std::unique_ptr<File> f =
        g_fileman->Open(tabdesc->GetTableEntry()->tabfid());
    PageNumber pid = f->GetFirstPageNumber();
    size_t npages_with_unoccupied_slots = 0;
    size_t n_erased = 0;
    size_t npages;
    while (pid != INVALID_PID) {
        char *buf;
        ScopedBufferId bufid = g_bufman->PinPage(pid, &buf);
        if (npages == 0 || npages == 4) {
            memset(buf + sizeof(PageHeaderData), 0,
                   PAGE_SIZE - sizeof(PageHeaderData));
            VarlenDataPage dp(buf);
            n_erased += dp.GetRecordCount();
            VarlenDataPage::InitializePage(buf);
        } else {
            VarlenDataPage dp(buf);
            if (dp.GetMaxSlotId() > 2) {
                if (n & 1) {
                    dp.EraseRecord(dp.GetMinSlotId());
                    ++n_erased;
                } else {
                    dp.EraseRecord(dp.GetMaxSlotId());
                    ++n_erased;
                }

                if (dp.GetMaxSlotId() > 2) {
                    dp.EraseRecord((dp.GetMinSlotId() + dp.GetMaxSlotId()) / 2);
                    ++n_erased;
                }
                ++npages_with_unoccupied_slots;
            } else if (dp.GetMaxSlotId() == 2) {
                dp.EraseRecord(dp.GetMinSlotId());
                ++n_erased;
                ++npages_with_unoccupied_slots;
            }
        }
        g_bufman->MarkDirty(bufid);
        pid = ((PageHeaderData*) buf)->GetNextPageNumber();
        ++npages;
    }

    // We're expecting at least 10 pages in the file.
    ASSERT_GE(npages, 10);

    if (npages_with_unoccupied_slots < 8) {
        TestEnableLogging();
        LOG(kWarning, "We were xpecting a more compact heap file than we had. "
                      "We were only able to make %lu additional pages "
                      "with unoccupied slots other than the two empty pages.",
                      npages_with_unoccupied_slots);
        TestDisableLogging();
    }

    if (n - n_erased < 3 * n / 5) {
        TestEnableLogging();
        LOG(kWarning, "We are not expecting to have erased more than 2/5 of "
                      "records, but we removed %lu out of %lu.",
                      n_erased, n);
        TestDisableLogging();
    }

    // scan the file and check if we can successfully iterate over everything
    // that's left in the file.
    std::unique_ptr<PlanExecState> scanstate = scan->create_exec_state();
    scanstate->init();

    std::vector<bool> found;
    size_t round = 0;
    for (size_t rewind_pos : {(size_t) 0, (size_t) 1,
                              (n - n_erased)/2,
                              (n - n_erased)/4,
                              n - n_erased - 1,
                              n - n_erased, n - n_erased}) {
        size_t n_found = 0;
        found.resize(0);
        found.resize(n, false);
        for (size_t i = 0; i < rewind_pos; ++i) {
            ASSERT_TRUE(scanstate->next_tuple()) << i;

            std::vector<NullableDatumRef> rec = scanstate->get_record();
            ASSERT_EQ(rec.size(), (size_t) 2);

            ASSERT_FALSE(rec[0].isnull());
            uint32_t f0_val = rec[0].GetUInt32();
            ASSERT_FALSE(rec[1].isnull());
            absl::string_view f1_val = varchar_to_string_view(rec[1]);

            ASSERT_GE(f0_val, 0) << i;
            ASSERT_LT(f0_val, n) << i;

            ASSERT_FALSE(found[f0_val]) <<
                "the same record has appeared more than once: ("
                << f0_val << ", " << f1_val << ")";
            found[f0_val] = true;
            ++n_found;

            Datum f1_expected = ExpectedValueF1(f0_val, false);
            absl::string_view f1_expected_val = varchar_to_string_view(f1_expected);
            ASSERT_EQ(f1_val, f1_expected_val);
        }

        ASSERT_EQ(n_found, rewind_pos) << round;

        if (n - n_erased == rewind_pos) {
            ASSERT_FALSE(scanstate->next_tuple()) << round;
            ASSERT_FALSE(scanstate->next_tuple()) << round;
        }
        ASSERT_NO_ERROR(scanstate->rewind()) << round;
        ++round;
    }

    scanstate->close();

    TDB_TEST_END
}

}    // namespace taco
