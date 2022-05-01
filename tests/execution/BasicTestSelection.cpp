#include "base/TDBDBTest.h"

#include "catalog/CatCache.h"
#include "catalog/Schema.h"
#include "plan/Selection.h"
#include "plan/TempTable.h"
#include "execution/SelectionState.h"
#include "utils/builtin_funcs.h"
#include "utils/typsupp/varchar.h"
#include "expr/Literal.h"
#include "expr/BinaryOperator.h"
#include "expr/Variable.h"
#include "expr/AndOperator.h"
#include "expr/OrOperator.h"

namespace taco {

class BasicTestSelection: public TDBDBTest {
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

TEST_F(BasicTestSelection, TestSelectionEmptyChild) {
    TDB_TEST_BEGIN

    auto expr = Literal::Create(Datum::From(true), initoids::TYP_BOOL);
    auto tmptable = TempTable::Create(m_schema.get());
    ASSERT_NE(tmptable.get(), nullptr);
    auto plan = Selection::Create(std::move(tmptable), std::move(expr));
    ASSERT_NE(plan.get(), nullptr);

    auto exec_state = plan->create_exec_state();
    ASSERT_NE(exec_state.get(), nullptr);

    EXPECT_NO_ERROR(exec_state->init());
    EXPECT_FALSE(exec_state->next_tuple());
    EXPECT_NO_ERROR(exec_state->close());

    TDB_TEST_END
}

TEST_F(BasicTestSelection, TestSelectionEmptyResult) {
    TDB_TEST_BEGIN

    auto tmptable = TempTable::Create(m_schema.get());
    ASSERT_NE(tmptable.get(), nullptr);
    for (size_t i = 0; i < 200; ++i) {
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
        EXPECT_NO_ERROR(tmptable->insert_record(std::move(rec)));
    }

    auto lit = Literal::Create(Datum::From(100.0), initoids::TYP_DOUBLE);
    auto var = Variable::Create(m_schema.get(), 1);
    auto expr = BinaryOperator::Create(OPTYPE_GT, std::move(var), std::move(lit));

    auto plan = Selection::Create(std::move(tmptable), std::move(expr));
    ASSERT_NE(plan.get(), nullptr);

    auto exec_state = plan->create_exec_state();
    ASSERT_NE(exec_state.get(), nullptr);

    EXPECT_NO_ERROR(exec_state->init());
    EXPECT_FALSE(exec_state->next_tuple());
    EXPECT_NO_ERROR(exec_state->close());

    TDB_TEST_END
}

TEST_F(BasicTestSelection, TestSelectionExprFilter) {
    TDB_TEST_BEGIN

    auto tmptable = TempTable::Create(m_schema.get());
    ASSERT_NE(tmptable.get(), nullptr);
    for (size_t i = 0; i < 200; ++i) {
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
        EXPECT_NO_ERROR(tmptable->insert_record(std::move(rec)));
    }

    std::vector<Datum> rec;
    rec.emplace_back(Datum::From(132));
    rec.emplace_back(Datum::From(4.55));
    rec.emplace_back(Datum::FromNull());
    tmptable->insert_record(std::move(rec));

    auto lit1 = Literal::Create(Datum::From(100), initoids::TYP_INT4);
    auto var1 = Variable::Create(m_schema.get(), 0);
    auto left = BinaryOperator::Create(OPTYPE_GE, std::move(var1), std::move(lit1));

    auto lit2 = Literal::Create(Datum::From((double)4.0), initoids::TYP_DOUBLE);
    auto var2 = Variable::Create(m_schema.get(), 1);
    auto right = BinaryOperator::Create(OPTYPE_LT, std::move(var2), std::move(lit2));

    auto expr = OrOperator::Create(std::move(left), std::move(right));

    auto plan = Selection::Create(std::move(tmptable), std::move(expr));
    ASSERT_NE(plan.get(), nullptr);

    auto exec_state = plan->create_exec_state();
    EXPECT_NO_ERROR(exec_state->init());

    for (size_t i = 0; i < 200; ++i) {
        if (i >= 100 || ((double)i) / 3 < 4.0) {
            EXPECT_TRUE(exec_state->next_tuple());
            auto rec = exec_state->get_record();
            EXPECT_EQ(rec[0].GetInt32(), i);
            EXPECT_FLOAT_EQ(rec[1].GetDouble(), ((double)i) / 3);
            EXPECT_EQ(rec[2].GetVarlenAsStringView(),
                      std::to_string(i * 2));
        }
    }

    EXPECT_TRUE(exec_state->next_tuple());
    auto out = exec_state->get_record();
    EXPECT_EQ(out[0].GetInt32(), 132);
    EXPECT_FLOAT_EQ(out[1].GetDouble(), 4.55);
    EXPECT_TRUE(out[2].isnull());
    EXPECT_FALSE(exec_state->next_tuple());

    EXPECT_NO_ERROR(exec_state->rewind());
    {
        for (size_t i = 0; i < 200; ++i) {
            if (i >= 100 || ((double)i) / 3 < 4.0) {
                EXPECT_TRUE(exec_state->next_tuple());
                auto rec = exec_state->get_record();
                EXPECT_EQ(rec[0].GetInt32(), i);
                EXPECT_FLOAT_EQ(rec[1].GetDouble(), ((double)i) / 3);
                EXPECT_EQ(rec[2].GetVarlenAsStringView(),
                        std::to_string(i * 2));
            }
        }

        EXPECT_TRUE(exec_state->next_tuple());
        auto out = exec_state->get_record();
        EXPECT_EQ(out[0].GetInt32(), 132);
        EXPECT_FLOAT_EQ(out[1].GetDouble(), 4.55);
        EXPECT_TRUE(out[2].isnull());
        EXPECT_FALSE(exec_state->next_tuple());
    }

    EXPECT_NO_ERROR(exec_state->close());

    TDB_TEST_END
}

}
