#include "base/TDBDBTest.h"

#include "catalog/CatCache.h"
#include "catalog/Schema.h"
#include "plan/Aggregation.h"
#include "plan/TempTable.h"
#include "execution/PlanExecState.h"
#include "utils/builtin_funcs.h"
#include "utils/typsupp/varchar.h"
#include "expr/Literal.h"
#include "expr/BinaryOperator.h"
#include "expr/Variable.h"
#include "expr/AndOperator.h"
#include "expr/OrOperator.h"
#include "catalog/aggtyp.h"

namespace taco {

class BasicTestAggregation: public TDBDBTest {
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

TEST_F(BasicTestAggregation, TestAggregationEmptyInput) {
    TDB_TEST_BEGIN

    auto tmptable = TempTable::Create(m_schema.get());

    std::vector<std::unique_ptr<ExprNode>> exprs;
    exprs.emplace_back(Variable::Create(m_schema.get(), 0));
    auto plan = Aggregation::Create(std::move(tmptable), std::move(exprs), { AGGTYPE_COUNT });
    ASSERT_NE(plan.get(), nullptr);

    auto schema = Schema::Create({initoids::TYP_UINT8}, {0}, {true});
    ASSERT_TRUE(Schema::Identical(plan->get_output_schema(), schema));

    auto exec_state = plan->create_exec_state();
    ASSERT_NE(exec_state.get(), nullptr);

    EXPECT_NO_ERROR(exec_state->init());
    EXPECT_TRUE(exec_state->next_tuple());
    EXPECT_EQ(exec_state->get_record()[0].GetUInt64(), 0);

    EXPECT_NO_ERROR(exec_state->rewind());
    EXPECT_TRUE(exec_state->next_tuple());
    EXPECT_EQ(exec_state->get_record()[0].GetUInt64(), 0);

    EXPECT_NO_ERROR(exec_state->close());

    TDB_TEST_END
}

TEST_F(BasicTestAggregation, TestAggregationSingleColumn) {
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

    std::vector<std::unique_ptr<ExprNode>> exprs;
    auto lit = Literal::Create(Datum::From(1.0), initoids::TYP_DOUBLE);
    auto var = Variable::Create(m_schema.get(), 1);

    exprs.emplace_back(BinaryOperator::Create(OPTYPE_ADD, std::move(var), std::move(lit)));

    auto plan = Aggregation::Create(std::move(tmptable), std::move(exprs), { AGGTYPE_SUM });
    ASSERT_NE(plan.get(), nullptr);

    auto schema = Schema::Create({initoids::TYP_DOUBLE}, {0}, {true});
    ASSERT_TRUE(Schema::Identical(plan->get_output_schema(), schema));

    auto exec_state = plan->create_exec_state();
    ASSERT_NE(exec_state.get(), nullptr);

    EXPECT_NO_ERROR(exec_state->init());
    EXPECT_TRUE(exec_state->next_tuple());
    EXPECT_FLOAT_EQ(exec_state->get_record()[0].GetDouble(), res);

    EXPECT_NO_ERROR(exec_state->close());

    TDB_TEST_END
}

TEST_F(BasicTestAggregation, TestAggregationMultipleColumn) {
    TDB_TEST_BEGIN

    auto tmptable = TempTable::Create(m_schema.get());
    ASSERT_NE(tmptable.get(), nullptr);
    double res = 0.0;
    double sum = 0.0;
    for (size_t i = 0; i < 200; ++i) {
        std::vector<Datum> rec;
        rec.emplace_back(Datum::From(i));
        rec.emplace_back(Datum::From(((double)i) / 3));
        res += ((double)i) / 3 + 1.0;
        sum += i;
    auto val = std::to_string(i * 2);
        Datum input_datum =
            Datum::FromVarlenAsStringView(val);
        rec.emplace_back(
            FunctionCallWithTypparam(m_varchar_infunc,
                                    m_f1_typparam,
                                    input_datum));
        EXPECT_NO_ERROR(tmptable->insert_record(std::move(rec)));
    }

    std::vector<std::unique_ptr<ExprNode>> exprs;
    auto lit1 = Literal::Create(Datum::From(1.0), initoids::TYP_DOUBLE);
    auto var1 = Variable::Create(m_schema.get(), 1);
    exprs.emplace_back(BinaryOperator::Create(OPTYPE_ADD, std::move(var1), std::move(lit1)));

    exprs.emplace_back(Variable::Create(m_schema.get(), 0));
    exprs.emplace_back(Variable::Create(m_schema.get(), 2));

    auto plan = Aggregation::Create(std::move(tmptable), std::move(exprs),
                                    { AGGTYPE_SUM, AGGTYPE_AVG, AGGTYPE_COUNT });
    ASSERT_NE(plan.get(), nullptr);

    auto schema = Schema::Create({initoids::TYP_DOUBLE, initoids::TYP_DOUBLE, initoids::TYP_UINT8},
                                 {0, 0, 0}, {true, true, true});
    ASSERT_TRUE(Schema::Identical(plan->get_output_schema(), schema));

    auto exec_state = plan->create_exec_state();
    ASSERT_NE(exec_state.get(), nullptr);

    EXPECT_NO_ERROR(exec_state->init());
    EXPECT_TRUE(exec_state->next_tuple());
    EXPECT_FLOAT_EQ(exec_state->get_record()[0].GetDouble(), res);
    EXPECT_FLOAT_EQ(exec_state->get_record()[1].GetDouble(), (double)sum / 200);
    EXPECT_EQ(exec_state->get_record()[2].GetUInt64(), 200);

    EXPECT_NO_ERROR(exec_state->close());

    TDB_TEST_END
}

}
