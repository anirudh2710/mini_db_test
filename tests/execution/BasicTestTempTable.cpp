#include "base/TDBDBTest.h"

#include "catalog/CatCache.h"
#include "catalog/Schema.h"
#include "plan/TempTable.h"
#include "execution/TempTableState.h"
#include "utils/builtin_funcs.h"
#include "utils/typsupp/varchar.h"

namespace taco {

class BasicTestTempTable: public TDBDBTest {
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

    FunctionInfo m_varchar_infunc;
    static constexpr uint64_t m_f1_typparam = 3;
};

TEST_F(BasicTestTempTable, TestEmptyTempTable) {
    TDB_TEST_BEGIN

    std::unique_ptr<TempTable> plan;

    {
        auto schema =
        Schema::Create({initoids::TYP_INT4,
                        initoids::TYP_DOUBLE,
                        initoids::TYP_VARCHAR},
                       {0, 0, 40},
                       {false, false, true});

        plan = TempTable::Create(schema);
    }

    ASSERT_NE(plan.get(), nullptr);

    auto new_schema =
        Schema::Create({initoids::TYP_INT4,
                        initoids::TYP_DOUBLE,
                        initoids::TYP_VARCHAR},
                       {0, 0, 40},
                       {false, false, true});

    // Implicitly assuming memory safety here...
    EXPECT_TRUE(Schema::Identical(new_schema, plan->get_output_schema()));

    auto exec_state = plan->create_exec_state();
    ASSERT_NE(exec_state.get(), nullptr);

    EXPECT_NO_ERROR(exec_state->init());
    EXPECT_FALSE(exec_state->next_tuple());
    EXPECT_NO_ERROR(exec_state->close());

    TDB_TEST_END
}

TEST_F(BasicTestTempTable, TestTempTableWithRecords) {
    TDB_TEST_BEGIN

    auto schema =
        Schema::Create({initoids::TYP_INT4,
                        initoids::TYP_DOUBLE,
                        initoids::TYP_VARCHAR},
                       {0, 0, 40},
                       {false, false, true});

    auto plan = TempTable::Create(schema);
    ASSERT_NE(plan.get(), nullptr);
    for (size_t i = 0; i < 100; ++i) {
        std::vector<Datum> rec;
        rec.emplace_back(Datum::From(i));
        rec.emplace_back(Datum::From(((double)i) / 3));
    auto val = std::to_string(i * 2);
        Datum input_datum =
            Datum::FromVarlenAsStringView(val);
        rec.emplace_back(
            FunctionCallWithTypparam(m_varchar_infunc,
                                     m_f1_typparam,
                                     input_datum));
        EXPECT_NO_ERROR(plan->insert_record(std::move(rec)));
    }

    auto exec_state = plan->create_exec_state();
    ASSERT_NE(exec_state.get(), nullptr);

    EXPECT_NO_ERROR(exec_state->init());

    for (size_t i = 0; i < 100; ++i) {
        ASSERT_TRUE(exec_state->next_tuple());
        auto rec = exec_state->get_record();
        EXPECT_EQ(rec[0].GetInt32(), i);
        EXPECT_FLOAT_EQ(rec[1].GetDouble(), ((double)i) / 3);
        EXPECT_EQ(rec[2].GetVarlenAsStringView(),
                  std::to_string(i * 2));
    }

    ASSERT_FALSE(exec_state->next_tuple());

    EXPECT_NO_ERROR(exec_state->rewind());

    for (size_t i = 0; i < 100; ++i) {
        ASSERT_TRUE(exec_state->next_tuple());
        auto rec = exec_state->get_record();
        EXPECT_EQ(rec[0].GetInt32(), i);
        EXPECT_FLOAT_EQ(rec[1].GetDouble(), ((double)i) / 3);
        EXPECT_EQ(rec[2].GetVarlenAsStringView(),
                  std::to_string(i * 2));
    }

    ASSERT_FALSE(exec_state->next_tuple());

    EXPECT_NO_ERROR(exec_state->close());

    TDB_TEST_END
}

}    // namespace taco
