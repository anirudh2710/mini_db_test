#include "base/TDBDBTest.h"

#include "catalog/CatCache.h"
#include "catalog/TableDesc.h"
#include "expr/BinaryOperator.h"
#include "expr/Literal.h"
#include "expr/Variable.h"
#include "index/btree/BTreeInternal.h"
#include "plan/IndexScan.h"
#include "plan/TempTable.h"
#include "execution/TableInsertState.h"
#include "utils/builtin_funcs.h"
#include "utils/typsupp/varchar.h"

namespace taco {

class BasicTestIndexScan: public TDBDBTest {
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

    Schema* GetTempTableSchema() {
        return Schema::Create({initoids::TYP_INT4,
                            initoids::TYP_VARCHAR},
                        {0, m_f1_typparam},
                        {isnullable, isnullable});
    }

    std::unique_ptr<taco::TempTable> CreateNewTempTablePlan(size_t num_records) {

        auto plan = TempTable::Create(GetTempTableSchema());
        for (size_t i = 0; i < num_records; ++i) {
            std::vector<Datum> data;
            data.emplace_back(ExpectedValueF0(i, isnullable));
            data.emplace_back(ExpectedValueF1(i, isnullable));
            EXPECT_NO_ERROR(plan->insert_record(std::move(data)));
        }
        return plan;
    }

    void
    CreateNewTable(size_t num_records,
                   std::shared_ptr<const TableDesc> &tabdesc) {
        g_db->CreateTable(
            "TableA",
            {initoids::TYP_INT4,
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

            auto prepare_recordbuf = [&](int32_t n) {
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

        g_db->CreateIndex("f0", tabid, IDXTYP(BTREE),
                    false, {0});
    }

    FunctionInfo m_varchar_infunc;
    static constexpr uint64_t m_f1_typparam = 2;
    static constexpr bool isnullable = false;
};

TEST_F(BasicTestIndexScan, TestIndexScanEmpty) {
    TDB_TEST_BEGIN

    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(0, tabdesc);

    Oid idxid = g_catcache->FindIndexByName("f0");
    auto idxdesc = g_catcache->FindIndexDesc(idxid);
    auto idx = BTree::Create(idxdesc);
    auto idxiter = idx->StartScan(nullptr, false, nullptr, false);

    std::unique_ptr<IndexScan> scan = IndexScan::Create(idxdesc, nullptr, false,
                                                        nullptr, false);
    ASSERT_TRUE(Schema::Identical(tabdesc->GetSchema(), scan->get_output_schema()));
    ASSERT_NE(scan.get(), nullptr)
        << "failed to create a index scan plan";

    std::unique_ptr<PlanExecState> scan_state = scan->create_exec_state();
    ASSERT_NE(scan.get(), nullptr)
        << "failed to create the index scan state";

    EXPECT_NO_ERROR(scan_state->init());
    EXPECT_FALSE(scan_state->next_tuple());
    EXPECT_NO_ERROR(scan_state->close());

    TDB_TEST_END
}

TEST_F(BasicTestIndexScan, TestIndexScanAll) {
    TDB_TEST_BEGIN

    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(100, tabdesc);

    Oid idxid = g_catcache->FindIndexByName("f0");
    auto idxdesc = g_catcache->FindIndexDesc(idxid);
    auto idx = BTree::Create(idxdesc);
    auto idxiter = idx->StartScan(nullptr, false, nullptr, false);

    std::unique_ptr<IndexScan> scan = IndexScan::Create(idxdesc, nullptr, false,
                                                        nullptr, false);
    ASSERT_TRUE(Schema::Compatible(tabdesc->GetSchema(), scan->get_output_schema()));
    ASSERT_NE(scan.get(), nullptr)
        << "failed to create a index scan plan";

    std::unique_ptr<PlanExecState> scan_state = scan->create_exec_state();
    ASSERT_NE(scan.get(), nullptr)
        << "failed to create the index scan state";

    EXPECT_NO_ERROR(scan_state->init());

    for (size_t i = 0; i < 100; i++) {
        EXPECT_TRUE(scan_state->next_tuple());
        std::vector<NullableDatumRef> rec = scan_state->get_record();
        EXPECT_EQ(rec.size(), 2);
        EXPECT_EQ(rec[0].GetUInt32(), i);
     }

    EXPECT_FALSE(scan_state->next_tuple());

    EXPECT_NO_ERROR(scan_state->close());

    TDB_TEST_END
}

TEST_F(BasicTestIndexScan, TestIndexScanSome) {
    TDB_TEST_BEGIN

    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(100, tabdesc);


    Oid idxid = g_catcache->FindIndexByName("f0");
    auto idxdesc = g_catcache->FindIndexDesc(idxid);

    std::vector<Datum> data_lower;
    data_lower.emplace_back(Datum::From(10));
    auto lower = IndexKey::Create(data_lower);
    std::vector<Datum> data_upper;
    data_upper.emplace_back(Datum::From(90));
    auto upper = IndexKey::Create(data_upper);


    std::unique_ptr<IndexScan> scan = IndexScan::Create(idxdesc, lower.get(), false,
                                                        upper.get(), true);
    ASSERT_NE(scan.get(), nullptr)
        << "failed to create a index scan plan";
    ASSERT_TRUE(Schema::Identical(tabdesc->GetSchema(), scan->get_output_schema()));

    std::unique_ptr<PlanExecState> scan_state = scan->create_exec_state();
    ASSERT_NE(scan.get(), nullptr)
        << "failed to create the index scan state";

    EXPECT_NO_ERROR(scan_state->init());

    for (size_t i = 10; i < 90; i++) {
        EXPECT_TRUE(scan_state->next_tuple());
        std::vector<NullableDatumRef> rec = scan_state->get_record();
        EXPECT_EQ(rec.size(), 2);
        EXPECT_EQ(rec[0].GetUInt32(), i);
    }

    EXPECT_FALSE(scan_state->next_tuple());

    EXPECT_NO_ERROR(scan_state->rewind());

    for (size_t i = 10; i < 90; i++) {
        EXPECT_TRUE(scan_state->next_tuple());
        std::vector<NullableDatumRef> rec = scan_state->get_record();
        EXPECT_EQ(rec.size(), 2);
        EXPECT_EQ(rec[0].GetUInt32(), i);
    }

    EXPECT_FALSE(scan_state->next_tuple());

    EXPECT_NO_ERROR(scan_state->close());

    TDB_TEST_END
}


}    // namespace taco
