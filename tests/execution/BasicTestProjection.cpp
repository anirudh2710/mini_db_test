#include "base/TDBDBTest.h"

#include "catalog/CatCache.h"
#include "catalog/TableDesc.h"
#include "plan/Projection.h"
#include "plan/TempTable.h"
#include "execution/ProjectionState.h"
#include "expr/BinaryOperator.h"
#include "expr/Variable.h"
#include "utils/builtin_funcs.h"
#include "utils/typsupp/varchar.h"

namespace taco {

class BasicTestProjection: public TDBDBTest {
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

    Schema* GetSchema() {
        return Schema::Create({initoids::TYP_INT4,
                            initoids::TYP_VARCHAR},
                        {0, 40},
                        {false, true});
    }

    std::unique_ptr<taco::TempTable> CreateNewTempTablePlan(bool isnullable, size_t num_records) {

        auto plan = TempTable::Create(GetSchema());
        for (size_t i = 0; i < num_records; ++i) {
            std::vector<Datum> data;
            data.emplace_back(ExpectedValueF0(i, isnullable));
            data.emplace_back(ExpectedValueF1(i, isnullable));
            EXPECT_NO_ERROR(plan->insert_record(std::move(data)));
        }
        return plan;
    }

    FunctionInfo m_varchar_infunc;
    static constexpr uint64_t m_f1_typparam = 2;
};

TEST_F(BasicTestProjection, TestFilterOneField) {
    TDB_TEST_BEGIN

    std::vector<std::unique_ptr<taco::ExprNode>> exprs;
    exprs.emplace_back(Variable::Create(GetSchema(), 0));
    std::unique_ptr<taco::PlanNode> proj = Projection::Create(CreateNewTempTablePlan(false, 100), std::move(exprs));

    ASSERT_NE(proj.get(), nullptr)
        << "failed to create a projection";

    std::unique_ptr<PlanExecState> proj_stat = proj->create_exec_state();
    ASSERT_NE(proj_stat.get(), nullptr)
        << "failed to create the projection state";

    EXPECT_NO_ERROR(proj_stat->init());

    for (size_t i = 0; i < 100; ++i) {
        EXPECT_TRUE(proj_stat->next_tuple())
            << "failed to get the next tuple";
        ASSERT_EQ(proj_stat->get_record().size(), 1)
            << "the number of fields in the record is not 1";
        ASSERT_EQ(proj_stat->get_record()[0].GetInt32(), ExpectedValueF0(i, false).GetInt32())
            << "the value of the field is not correct";
    }

    EXPECT_FALSE(proj_stat->next_tuple())
        << "there should be no more tuples";

    EXPECT_NO_ERROR(proj_stat->close());
    TDB_TEST_END
}

TEST_F(BasicTestProjection, TestFilterTwoField) {
    TDB_TEST_BEGIN

    std::vector<std::unique_ptr<taco::ExprNode>> exprs;
    exprs.emplace_back(Variable::Create(GetSchema(), 0));
    exprs.emplace_back(Variable::Create(GetSchema(), 1));
    std::unique_ptr<taco::PlanNode> proj = Projection::Create(CreateNewTempTablePlan(false, 100), std::move(exprs));

    ASSERT_NE(proj.get(), nullptr)
        << "failed to create a projection";

    std::unique_ptr<PlanExecState> proj_stat = proj->create_exec_state();
    ASSERT_NE(proj_stat.get(), nullptr)
        << "failed to create the projection state";

    EXPECT_NO_ERROR(proj_stat->init());

    for (size_t i = 0; i < 100; ++i) {
        EXPECT_TRUE(proj_stat->next_tuple());
        ASSERT_EQ(proj_stat->get_record().size(), 2);
        ASSERT_EQ(proj_stat->get_record()[0].GetInt32(), ExpectedValueF0(i, false).GetInt32());

        auto f1 = proj_stat->get_record()[1];
        absl::string_view f1_value = varchar_to_string_view(f1);
        Datum f1_expected = ExpectedValueF1(i, false);
        absl::string_view f1_expected_value = varchar_to_string_view(f1_expected);
        EXPECT_EQ(f1_value, f1_expected_value);
    }

    EXPECT_FALSE(proj_stat->next_tuple())
        << "there should be no more tuples";

    EXPECT_NO_ERROR(proj_stat->rewind());
    for (size_t i = 0; i < 100; ++i) {
        EXPECT_TRUE(proj_stat->next_tuple());
        ASSERT_EQ(proj_stat->get_record().size(), 2);
        ASSERT_EQ(proj_stat->get_record()[0].GetInt32(), ExpectedValueF0(i, false).GetInt32());

        auto f1 = proj_stat->get_record()[1];
        absl::string_view f1_value = varchar_to_string_view(f1);
        Datum f1_expected = ExpectedValueF1(i, false);
        absl::string_view f1_expected_value = varchar_to_string_view(f1_expected);
        EXPECT_EQ(f1_value, f1_expected_value);
    }

    EXPECT_FALSE(proj_stat->next_tuple())
        << "there should be no more tuples";

    EXPECT_NO_ERROR(proj_stat->close());
    TDB_TEST_END
}

TEST_F(BasicTestProjection, TestBinaryOperatorProjection) {
    TDB_TEST_BEGIN

    std::unique_ptr<ExprNode> var1 = Variable::Create(GetSchema(), 0);
    std::unique_ptr<ExprNode> var2 = Variable::Create(GetSchema(), 0);
    std::unique_ptr<ExprNode> add =
        BinaryOperator::Create(OPTYPE_ADD, std::move(var1), std::move(var2));

    std::vector<std::unique_ptr<taco::ExprNode>> exprs;
    exprs.emplace_back(std::move(add));
    std::unique_ptr<taco::PlanNode> proj =
         Projection::Create(CreateNewTempTablePlan(false, 100), std::move(exprs));

    ASSERT_NE(proj.get(), nullptr)
        << "failed to create a projection";

    std::unique_ptr<PlanExecState> proj_stat = proj->create_exec_state();
    ASSERT_NE(proj_stat.get(), nullptr)
        << "failed to create the projection state";

    EXPECT_NO_ERROR(proj_stat->init());

    for (size_t i = 0; i < 100; ++i) {
        EXPECT_TRUE(proj_stat->next_tuple());
        ASSERT_EQ(proj_stat->get_record().size(), 1);
        ASSERT_EQ(proj_stat->get_record()[0].GetInt32(), ExpectedValueF0(i, false).GetInt32()*2);
    }

    EXPECT_FALSE(proj_stat->next_tuple())
        << "there should be no more tuples";

    EXPECT_NO_ERROR(proj_stat->close());
    TDB_TEST_END
}

}    // namespace taco
