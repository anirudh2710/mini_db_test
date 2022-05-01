#include "base/TDBDBTest.h"

#include "catalog/TableDesc.h"
#include "catalog/CatCacheBase.h"
#include "catalog/Schema.h"
#include "catalog/aggtyp.h"
#include "plan/TableScan.h"
#include "plan/Selection.h"
#include "plan/Aggregation.h"
#include "plan/Projection.h"
#include "plan/Sort.h"
#include "plan/CartesianProduct.h"
#include "plan/Limit.h"
#include "execution/PlanExecState.h"
#include "expr/BinaryOperator.h"
#include "expr/Variable.h"
#include "expr/FuncCallOperator.h"
#include "expr/Literal.h"
#include "utils/builtin_funcs.h"
#include "utils/numbers.h"
#include "utils/typsupp/varchar.h"

namespace taco {

static constexpr uint32_t s_reorder_mask = 0x3f7u;

class BasicTestCompositePlan: public TDBDBTest {
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
    ExpectedValueF0(size_t n) {
        return Datum::From(n ^ s_reorder_mask);
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
    CreateNewTable(size_t num_records, absl::string_view table_name,
                   std::shared_ptr<const TableDesc> &tabdesc,
                   uint32_t base_for_f1 = 0) {
        g_db->CreateTable(
            table_name,
            {initoids::TYP_UINT4,
             initoids::TYP_VARCHAR},
            {0, m_f1_typparam},
            {"f0", "f1"},
            {false, true});

        Oid tabid = g_catcache->FindTableByName(table_name);
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
                data.emplace_back(ExpectedValueF0(n));
                data.emplace_back(ExpectedValueF1(n + base_for_f1, true));
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
    static constexpr uint64_t m_f1_typparam = 100;
};

TEST_F(BasicTestCompositePlan, TestAggregationQuery) {
    TDB_TEST_BEGIN

    /*
     * SELECT COUNT(*), COUNT(f1), SUM(f0), AVG(VARCHAR_LENGTH(f1))
     * FROM A
     * WHERE f0 >= 1000;
     */
    const uint32_t n = 2000;
    std::shared_ptr<const TableDesc> tabdescA;
    CreateNewTable(n, "A", tabdescA);

    // the expected answers
    uint64_t expected_count_all = 0;
    uint64_t expected_count_f1 = 0;
    uint64_t expected_sum_f0 = 0;
    uint64_t expected_sum_strlen_f1 = 0.0;

    for (uint32_t i = 0; i < n; ++i) {
        uint32_t f0 = (uint32_t)(i ^ s_reorder_mask);
        if (f0 >= 1000) {
            ++expected_count_all;
            expected_sum_f0 += f0;
            if (!(i & 2)) {
                std::string f1 = std::to_string(i * 10);
                if (f1.size() > m_f1_typparam) {
                    f1 = f1.substr(0, m_f1_typparam);
                }
                ++expected_count_f1;
                expected_sum_strlen_f1 += f1.length();
            }
        }
    }
    LOG(kInfo, "%u %u", expected_count_all,  expected_count_f1);
    double expected_avg_strlen_f1 =
        expected_sum_strlen_f1 * 1.0 / expected_count_f1;
    ASSERT_GT(expected_count_f1, 0)
        << "We expect at least one non-null f1 value in the query result.";

    // construct the query
    std::unique_ptr<PlanNode> p = TableScan::Create(tabdescA);
    std::unique_ptr<ExprNode> e = Variable::Create(p->get_output_schema(),
                                                   0);
    std::unique_ptr<ExprNode> e2 = Literal::Create(Datum::From((uint32_t) 1000),
                                                   initoids::TYP_UINT4);
    e = BinaryOperator::Create(OPTYPE_GE, std::move(e), std::move(e2));
    p = Selection::Create(std::move(p), std::move(e));

    std::vector<AggType> aggtyps;
    std::vector<std::unique_ptr<ExprNode>> agg_args;
    aggtyps.reserve(4);
    agg_args.reserve(4);

    // We can use COUNT(0) in place of COUNT(*) here
    aggtyps.emplace_back(AGGTYPE_COUNT);
    agg_args.emplace_back(Literal::Create(Datum::From(0), initoids::TYP_INT4));

    // COUNT(f1)
    aggtyps.emplace_back(AGGTYPE_COUNT);
    agg_args.emplace_back(Variable::Create(p->get_output_schema(), 1));

    // SUM(f0)
    aggtyps.emplace_back(AGGTYPE_SUM);
    agg_args.emplace_back(Variable::Create(p->get_output_schema(), 0));

    // AVG(VARCHAR_LENGTH(f1))
    Oid vcharlen_funcid = g_catcache->FindFunctionByName("varchar_length");
    ASSERT_NE(vcharlen_funcid, InvalidOid);
    aggtyps.emplace_back(AGGTYPE_AVG);
    agg_args.emplace_back(FuncCallOperator::Create(
        vcharlen_funcid,
        Variable::Create(p->get_output_schema(), 1)));

    p = Aggregation::Create(std::move(p), std::move(agg_args),
                            aggtyps);

    // check if the schema is ok
    const Schema *outsch = p->get_output_schema();
    ASSERT_EQ(outsch->GetNumFields(), (FieldId) 4);
    ASSERT_EQ(outsch->GetFieldTypeId(0), initoids::TYP_UINT8);
    ASSERT_EQ(outsch->GetFieldTypeId(1), initoids::TYP_UINT8);
    ASSERT_EQ(outsch->GetFieldTypeId(2), initoids::TYP_UINT8);
    ASSERT_EQ(outsch->GetFieldTypeId(3), initoids::TYP_DOUBLE);

    // run the query
    std::unique_ptr<PlanExecState> s = p->create_exec_state();
    ASSERT_NO_ERROR(s->init());
    ASSERT_TRUE(s->next_tuple());

    std::vector<NullableDatumRef> rec;
    ASSERT_NO_ERROR(rec = s->get_record());
    ASSERT_EQ(rec.size(), 4u);

    // check the results
    ASSERT_FALSE(rec[0].isnull());
    auto count_all = rec[0].GetUInt64();
    EXPECT_EQ(count_all, expected_count_all);

    ASSERT_FALSE(rec[1].isnull());
    auto count_f1 = rec[1].GetUInt64();
    EXPECT_EQ(count_f1, expected_count_f1);

    ASSERT_FALSE(rec[2].isnull());
    auto sum_f0 = rec[2].GetUInt64();
    EXPECT_EQ(sum_f0, expected_sum_f0);


    ASSERT_FALSE(rec[3].isnull());
    auto avg_strlen_f1 = rec[3].GetDouble();
    EXPECT_DOUBLE_EQ(avg_strlen_f1, expected_avg_strlen_f1);

    // should have only one row in aggregation
    ASSERT_FALSE(s->next_tuple());
    ASSERT_NO_ERROR(s->close());

    TDB_TEST_END
}

TEST_F(BasicTestCompositePlan, TestJoinQuery) {
    TDB_TEST_BEGIN

    /*
     * SELECT A.f0, A.f1, B.f1
     * FROM A, B
     * WHERE A.f0 = B.f0
     * ORDER BY A.f1 DESC
     * LIMIT 100;
     */
    const uint32_t n = 2000;
    const uint32_t limit = 100;
    std::shared_ptr<const TableDesc> tabdescA;
    std::shared_ptr<const TableDesc> tabdescB;
    CreateNewTable(n, "A", tabdescA);
    CreateNewTable(n, "B", tabdescB, 10000);

    // the expected answers
    std::vector<std::string> f1_vals;
    f1_vals.reserve(n * 0.5 + 1);
    for (uint32_t i = 0; i < n; ++i) {
        if (i & 2) {
            // null value
            continue;
        }
        f1_vals.push_back(std::to_string(i * 10));
    }
    ASSERT_GE(f1_vals.size(), limit);
    std::sort(f1_vals.begin(), f1_vals.end(), std::greater<std::string>());

    std::vector<uint32_t> expected_f0;
    expected_f0.reserve(limit);
    for (uint32_t i = 0; i < limit; ++i) {
        uint32_t f1;
        ASSERT_TRUE(SimpleAtoiWrapper(f1_vals[i], &f1));
        ASSERT_EQ(f1 % 10, 0);
        f1 = (f1 / 10) ^ s_reorder_mask;
        expected_f0.push_back(f1);
    }

    // construct the query
    std::unique_ptr<PlanNode> p = TableScan::Create(tabdescA);
    std::unique_ptr<PlanNode> p2 = TableScan::Create(tabdescB);
    p = CartesianProduct::Create(std::move(p), std::move(p2));
    std::unique_ptr<ExprNode> e = Variable::Create(p->get_output_schema(),
                                                   0);
    std::unique_ptr<ExprNode> e2 = Variable::Create(p->get_output_schema(),
                                                    2);
    e = BinaryOperator::Create(OPTYPE_EQ, std::move(e), std::move(e2));
    p = Selection::Create(std::move(p), std::move(e));

    std::vector<std::unique_ptr<ExprNode>> exprs;
    exprs.reserve(3);
    exprs.emplace_back(Variable::Create(p->get_output_schema(), 0));
    exprs.emplace_back(Variable::Create(p->get_output_schema(), 1));
    exprs.emplace_back(Variable::Create(p->get_output_schema(), 3));
    p = Projection::Create(std::move(p), std::move(exprs));

    exprs.reserve(1);
    exprs.emplace_back(Variable::Create(p->get_output_schema(), 1));
    p = Sort::Create(std::move(p), std::move(exprs), {true});
    p = Limit::Create(std::move(p), limit);

    // check if the schema is ok
    const Schema *outsch = p->get_output_schema();
    ASSERT_EQ(outsch->GetNumFields(), (FieldId) 3);
    ASSERT_EQ(outsch->GetFieldTypeId(0), initoids::TYP_UINT4);
    ASSERT_EQ(outsch->GetFieldTypeId(1), initoids::TYP_VARCHAR);
    ASSERT_EQ(outsch->GetFieldTypeId(2), initoids::TYP_VARCHAR);

    // run the query
    std::unique_ptr<PlanExecState> s = p->create_exec_state();
    ASSERT_NO_ERROR(s->init());

    for (uint32_t i = 0; i < limit; ++i) {
        ASSERT_TRUE(s->next_tuple());
        std::vector<NullableDatumRef> rec;
        ASSERT_NO_ERROR(rec = s->get_record());
        ASSERT_EQ(rec.size(), 3u);

        ASSERT_FALSE(rec[0].isnull());
        auto f0 = rec[0].GetUInt32();
        ASSERT_EQ(f0, expected_f0[i]);
        auto ii = f0 ^ s_reorder_mask;

        std::string expected_A_f1 = std::to_string(ii * 10);
        ASSERT_FALSE(rec[1].isnull());
        absl::string_view A_f1 = varchar_to_string_view(rec[1]);
        ASSERT_EQ(A_f1, expected_A_f1);

        std::string expected_B_f1 = std::to_string((ii + 10000) * 10);
        ASSERT_FALSE(rec[2].isnull());
        absl::string_view B_f1 = varchar_to_string_view(rec[2]);
        ASSERT_EQ(B_f1, expected_B_f1);
    }

    ASSERT_FALSE(s->next_tuple());
    ASSERT_NO_ERROR(s->close());

    TDB_TEST_END
}

}  // namespace taco

