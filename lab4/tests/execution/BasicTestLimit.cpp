#include "base/TDBDBTest.h"

#include "catalog/CatCache.h"
#include "catalog/Schema.h"
#include "plan/Limit.h"
#include "plan/TempTable.h"
#include "execution/PlanExecState.h"
#include "utils/builtin_funcs.h"
#include "utils/typsupp/varchar.h"

namespace taco {

class BasicTestLimit: public TDBDBTest {
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

        m_schema = absl::WrapUnique(
            Schema::Create({initoids::TYP_INT4,
                            initoids::TYP_DOUBLE,
                            initoids::TYP_VARCHAR},
                            {0, 0, 40},
                            {false, false, true}));
    }

    void
    TearDown() override {
        TDBDBTest::TearDown();
    }

    FunctionInfo m_varchar_infunc;
    static constexpr uint64_t m_f1_typparam = 3;

    std::unique_ptr<Schema> m_schema;
};

TEST_F(BasicTestLimit, TestLimitEmptyInput) {
    TDB_TEST_BEGIN

    auto tmptable = TempTable::Create(m_schema.get());

    auto plan = Limit::Create(std::move(tmptable), 10);
    ASSERT_NE(plan.get(), nullptr);

    ASSERT_TRUE(Schema::Identical(plan->get_output_schema(), m_schema.get()));

    auto exec_state = plan->create_exec_state();
    ASSERT_NE(exec_state.get(), nullptr);

    EXPECT_NO_ERROR(exec_state->init());
    EXPECT_FALSE(exec_state->next_tuple());

    EXPECT_NO_ERROR(exec_state->close());

    TDB_TEST_END
}

TEST_F(BasicTestLimit, TestLimitNormal) {
    TDB_TEST_BEGIN

    auto tmptable = TempTable::Create(m_schema.get());
    ASSERT_NE(tmptable.get(), nullptr);
    double res = 0.0;
    for (size_t i = 0; i < 200; ++i) {
        std::vector<Datum> rec;
        rec.emplace_back(Datum::From(i));
        rec.emplace_back(Datum::From(((double)i) / 3));
        res += ((double)i) / 3 + 1.0;
    auto val = std::to_string(i * 2);
        Datum input_datum =
            Datum::FromVarlenAsStringView(val);
        rec.emplace_back(
            FunctionCallWithTypparam(m_varchar_infunc,
                                    m_f1_typparam,
                                    input_datum));
        EXPECT_NO_ERROR(tmptable->insert_record(std::move(rec)));
    }

    auto plan = Limit::Create(std::move(tmptable), 10);
    ASSERT_NE(plan.get(), nullptr);

    ASSERT_TRUE(Schema::Identical(plan->get_output_schema(), m_schema.get()));

    auto exec_state = plan->create_exec_state();
    ASSERT_NE(exec_state.get(), nullptr);

    EXPECT_NO_ERROR(exec_state->init());
    for (size_t i = 0; i < 10; ++i) {
        EXPECT_TRUE(exec_state->next_tuple());
        auto rec = exec_state->get_record();
        EXPECT_EQ(rec[0].GetInt32(), i);
        EXPECT_FLOAT_EQ(rec[1].GetDouble(), ((double)i) / 3);
        EXPECT_EQ(rec[2].GetVarlenAsStringView(),
                  std::to_string(i * 2));
    }
    EXPECT_FALSE(exec_state->next_tuple());

    EXPECT_NO_ERROR(exec_state->rewind());
    for (size_t i = 0; i < 10; ++i) {
        EXPECT_TRUE(exec_state->next_tuple());
        auto rec = exec_state->get_record();
        EXPECT_EQ(rec[0].GetInt32(), i);
        EXPECT_FLOAT_EQ(rec[1].GetDouble(), ((double)i) / 3);
        EXPECT_EQ(rec[2].GetVarlenAsStringView(),
                  std::to_string(i * 2));
    }

    EXPECT_FALSE(exec_state->next_tuple());
    EXPECT_NO_ERROR(exec_state->close());

    TDB_TEST_END
}

TEST_F(BasicTestLimit, TestLimitOverflow) {
    TDB_TEST_BEGIN

    auto tmptable = TempTable::Create(m_schema.get());
    ASSERT_NE(tmptable.get(), nullptr);
    double res = 0.0;
    for (size_t i = 0; i < 100; ++i) {
        std::vector<Datum> rec;
        rec.emplace_back(Datum::From(i));
        rec.emplace_back(Datum::From(((double)i) / 3));
        res += ((double)i) / 3 + 1.0;
    auto val = std::to_string(i * 2);
        Datum input_datum =
            Datum::FromVarlenAsStringView(val);
        rec.emplace_back(
            FunctionCallWithTypparam(m_varchar_infunc,
                                    m_f1_typparam,
                                    input_datum));
        EXPECT_NO_ERROR(tmptable->insert_record(std::move(rec)));
    }

    auto plan = Limit::Create(std::move(tmptable), 200);
    ASSERT_NE(plan.get(), nullptr);

    ASSERT_TRUE(Schema::Identical(plan->get_output_schema(), m_schema.get()));

    auto exec_state = plan->create_exec_state();
    ASSERT_NE(exec_state.get(), nullptr);

    EXPECT_NO_ERROR(exec_state->init());
    for (size_t i = 0; i < 100; ++i) {
        EXPECT_TRUE(exec_state->next_tuple());
        auto rec = exec_state->get_record();
        EXPECT_EQ(rec[0].GetInt32(), i);
        EXPECT_FLOAT_EQ(rec[1].GetDouble(), ((double)i) / 3);
        EXPECT_EQ(rec[2].GetVarlenAsStringView(),
                  std::to_string(i * 2));
    }
    EXPECT_FALSE(exec_state->next_tuple());
    EXPECT_FALSE(exec_state->next_tuple());

    EXPECT_NO_ERROR(exec_state->close());

    TDB_TEST_END
}

}
