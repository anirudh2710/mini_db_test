#include "base/TDBDBTest.h"

#include "catalog/CatCache.h"
#include "catalog/TableDesc.h"
#include "index/btree/BTreeInternal.h"
#include "plan/TableInsert.h"
#include "plan/TempTable.h"
#include "execution/TableInsertState.h"
#include "utils/builtin_funcs.h"
#include "utils/typsupp/varchar.h"

namespace taco {

class BasicTestTableInsert: public TDBDBTest {
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

TEST_F(BasicTestTableInsert, TestInsertEmptySubPlan) {
    TDB_TEST_BEGIN

    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(100, tabdesc);
    std::unique_ptr<TableInsert> insert = TableInsert::Create(CreateNewTempTablePlan(0), tabdesc);
    ASSERT_NE(insert.get(), nullptr)
        << "failed to create a table insert plan";

    std::unique_ptr<PlanExecState> insert_state = insert->create_exec_state();
    ASSERT_NE(insert_state.get(), nullptr)
        << "failed to create the table insert state";

    EXPECT_NO_ERROR(insert_state->init());
    EXPECT_TRUE(insert_state->next_tuple());
    EXPECT_FALSE(insert_state->next_tuple());
    EXPECT_EQ(insert_state->get_record().size(), 1);
    EXPECT_EQ(insert_state->get_record()[0].GetUInt64(), 0);

    EXPECT_FATAL_ERROR(insert_state->rewind());

    EXPECT_NO_ERROR(insert_state->close());

    // check the contents of the table
    Oid tabid = g_catcache->FindTableByName("TableA");
    tabdesc = g_catcache->FindTableDesc(tabid);
    std::unique_ptr<Table> tab = Table::Create(tabdesc);
    Table::Iterator iter = tab->StartScan();

    size_t i = 0;
    while (iter.Next()) i++;
    EXPECT_EQ(i, 100);

    // check the contents of the index
    Oid idxid = g_catcache->FindIndexByName("f0");
    auto idxdesc = g_catcache->FindIndexDesc(idxid);
    auto idx = BTree::Create(idxdesc);
    auto idxiter = idx->StartScan(nullptr, false, nullptr, false);

    i = 0;
    while (idxiter->Next()) ++i;
    EXPECT_EQ(i, 100);

    TDB_TEST_END
}

TEST_F(BasicTestTableInsert, TestInsertNormal) {
    TDB_TEST_BEGIN

    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(0, tabdesc);
    std::unique_ptr<TableInsert> insert = TableInsert::Create(CreateNewTempTablePlan(100), tabdesc);
    ASSERT_NE(insert.get(), nullptr)
        << "failed to create a table insert plan";

    std::unique_ptr<PlanExecState> insert_state = insert->create_exec_state();
    ASSERT_NE(insert_state.get(), nullptr)
        << "failed to create the table insert state";

    EXPECT_NO_ERROR(insert_state->init());
    EXPECT_TRUE(insert_state->next_tuple());
    EXPECT_FALSE(insert_state->next_tuple());
    EXPECT_EQ(insert_state->get_record().size(), 1);
    EXPECT_EQ(insert_state->get_record()[0].GetUInt32(), 100);

    EXPECT_FATAL_ERROR(insert_state->rewind());

    EXPECT_NO_ERROR(insert_state->close());

    // check the contents of the table
    Oid tabid = g_catcache->FindTableByName("TableA");
    tabdesc = g_catcache->FindTableDesc(tabid);
    std::unique_ptr<Table> tab = Table::Create(tabdesc);
    Table::Iterator iter = tab->StartScan();

    size_t i = 0;
    std::vector<uint32_t> res;
    std::vector<uint32_t> expected_res;
    while (iter.Next()) {
        const Record &rec = iter.GetCurrentRecord();
        uint32_t f0 = tabdesc->GetSchema()->GetField(0, rec.GetData()).GetUInt32();
        res.emplace_back(f0);

        expected_res.emplace_back(i);
        i++;
    }
    EXPECT_EQ(i, 100);
    EXPECT_THAT(res, testing::ContainerEq(expected_res));


    // check the contents of the index
    Oid idxid = g_catcache->FindIndexByName("f0");
    auto idxdesc = g_catcache->FindIndexDesc(idxid);
    auto idx = BTree::Create(idxdesc);
    auto idxiter = idx->StartScan(nullptr, false, nullptr, false);

    res.clear();
    while (idxiter->Next()) {
        const Record &rec = idxiter->GetCurrentItem();
        uint32_t f0 = idxdesc->GetKeySchema()->GetField(0, rec.GetData()).GetUInt32();
        res.emplace_back(f0);
    }
    EXPECT_EQ(i, 100);
    EXPECT_THAT(res, testing::ContainerEq(expected_res));

    TDB_TEST_END
}


}    // namespace taco
