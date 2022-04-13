#include "base/TDBDBTest.h"

#include "catalog/CatCache.h"
#include "catalog/TableDesc.h"
#include "plan/TableScan.h"
#include "plan/Sort.h"
#include "execution/PlanExecState.h"
#include "utils/builtin_funcs.h"
#include "utils/typsupp/varchar.h"
#include "expr/Variable.h"

namespace taco {

static constexpr size_t s_reorder_mask = 0x7edull;

class BasicTestSort: public TDBDBTest {
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
        return Datum::From((n / 10) ^ s_reorder_mask);
    }

    Datum
    ExpectedValueF1(size_t n, bool isnullable) {
        if (isnullable && (n & 3)) {
            return Datum::FromNull();
        }
        char str[2];
        str[0] = 'A';
        str[1] = '0' + (n % 10);
        Datum input_datum = Datum::FromVarlenBytes(str, 2);
        return FunctionCallWithTypparam(m_varchar_infunc,
                                        m_f1_typparam,
                                        input_datum);
    }

    void
    CreateNewTable(bool isnullable, size_t num_records,
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
    }

    FunctionInfo m_varchar_infunc;
    static constexpr uint64_t m_f1_typparam = 2;
};

TEST_F(BasicTestSort, TestSortEmpty) {
    TDB_TEST_BEGIN

    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(false, 0, tabdesc);
    std::unique_ptr<TableScan> p0 = TableScan::Create(tabdesc);
    ASSERT_NE(p0.get(), nullptr);

    std::unique_ptr<ExprNode> col0 = Variable::Create(p0->get_output_schema(),
                                                      0);
    ASSERT_NE(col0.get(), nullptr);
    std::unique_ptr<ExprNode> col1 = Variable::Create(p0->get_output_schema(),
                                                      1);
    ASSERT_NE(col1.get(), nullptr);

    std::vector<std::unique_ptr<ExprNode>> sort_exprs;
    sort_exprs.emplace_back(std::move(col0));
    sort_exprs.emplace_back(std::move(col1));
    std::unique_ptr<PlanNode> p1 = Sort::Create(std::move(p0),
                                                std::move(sort_exprs),
                                                {false, false});
    ASSERT_NE(p1.get(), nullptr);

    std::unique_ptr<PlanExecState> s = p1->create_exec_state();
    ASSERT_NE(s.get(), nullptr);

    ASSERT_NO_ERROR(s->init());
    EXPECT_FALSE(s->next_tuple());
    EXPECT_FALSE(s->next_tuple());
    ASSERT_NO_ERROR(s->close());

    TDB_TEST_END
}

TEST_F(BasicTestSort, TestSortSingleColumnAsc) {
    TDB_TEST_BEGIN

    const size_t n = 40960;
    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(false, n, tabdesc);
    std::unique_ptr<TableScan> p0 = TableScan::Create(tabdesc);
    ASSERT_NE(p0.get(), nullptr);

    std::unique_ptr<ExprNode> col0 = Variable::Create(p0->get_output_schema(),
                                                      0);
    ASSERT_NE(col0.get(), nullptr);

    std::vector<std::unique_ptr<ExprNode>> sort_exprs;
    sort_exprs.emplace_back(std::move(col0));
    std::unique_ptr<PlanNode> p1 = Sort::Create(std::move(p0),
                                                std::move(sort_exprs),
                                                {false});
    ASSERT_NE(p1.get(), nullptr);

    std::unique_ptr<PlanExecState> s = p1->create_exec_state();
    ASSERT_NE(s.get(), nullptr);

    ASSERT_NO_ERROR(s->init());
    std::vector<bool> found(10, false);
    uint32_t nfound = 0;
    for (size_t i = 0; i < n; ++i) {
        ASSERT_TRUE(s->next_tuple());
        std::vector<NullableDatumRef> rec = s->get_record();
        ASSERT_EQ(rec.size(), 2);
        ASSERT_FALSE(rec[0].isnull());
        uint32_t val = rec[0].GetUInt32();
        ASSERT_EQ(val, i / 10) << i;

        ASSERT_FALSE(rec[1].isnull());
        absl::string_view val1  = varchar_to_string_view(rec[1]);
        ASSERT_EQ(val1[0], 'A') << i;

        int last_digit = (int)(val1[1] - '0');
        ASSERT_FALSE(found[last_digit])
            << "The same record appeared more than once: (" << val
            << ", " << val1 << ")";
        found[last_digit] = true;
        ++nfound;

        if ((i + 1) % 10 == 0) {
            ASSERT_EQ(nfound, 10u) << "missing some record";
            found.clear();
            found.resize(10, false);
            nfound = 0;
        }
    }

    ASSERT_FALSE(s->next_tuple());
    ASSERT_FALSE(s->next_tuple());

    ASSERT_NO_ERROR(s->close());

    TDB_TEST_END
}

TEST_F(BasicTestSort, TestSortSingleColumnDesc) {
    TDB_TEST_BEGIN

    const size_t n = 40960;
    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(false, n, tabdesc);
    std::unique_ptr<TableScan> p0 = TableScan::Create(tabdesc);
    ASSERT_NE(p0.get(), nullptr);

    std::unique_ptr<ExprNode> col0 = Variable::Create(p0->get_output_schema(),
                                                      0);
    ASSERT_NE(col0.get(), nullptr);

    std::vector<std::unique_ptr<ExprNode>> sort_exprs;
    sort_exprs.emplace_back(std::move(col0));
    std::unique_ptr<PlanNode> p1 = Sort::Create(std::move(p0),
                                                std::move(sort_exprs),
                                                {true});
    ASSERT_NE(p1.get(), nullptr);

    std::unique_ptr<PlanExecState> s = p1->create_exec_state();
    ASSERT_NE(s.get(), nullptr);

    ASSERT_NO_ERROR(s->init());
    std::vector<bool> found(10, false);
    uint32_t nfound = 0;
    for (size_t i = 0; i < n; ++i) {
        ASSERT_TRUE(s->next_tuple());
        std::vector<NullableDatumRef> rec = s->get_record();
        ASSERT_EQ(rec.size(), 2);
        ASSERT_FALSE(rec[0].isnull());
        uint32_t val = rec[0].GetUInt32();
        ASSERT_EQ(val, n / 10 - (i / 10) - 1) << i;

        ASSERT_FALSE(rec[1].isnull());
        absl::string_view val1  = varchar_to_string_view(rec[1]);
        ASSERT_EQ(val1[0], 'A') << i;

        int last_digit = (int)(val1[1] - '0');
        ASSERT_FALSE(found[last_digit])
            << "The same record appeared more than once: (" << val
            << ", " << val1 << ")";
        found[last_digit] = true;
        ++nfound;

        if ((i + 1) % 10 == 0) {
            ASSERT_EQ(nfound, 10u) << "missing some record";
            found.clear();
            found.resize(10, false);
            nfound = 0;
        }
    }

    ASSERT_FALSE(s->next_tuple());
    ASSERT_FALSE(s->next_tuple());

    ASSERT_NO_ERROR(s->close());

    TDB_TEST_END
}

TEST_F(BasicTestSort, TestSortTwoColumnsAscAsc) {
    TDB_TEST_BEGIN

    const size_t n = 40960;
    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(false, n, tabdesc);
    std::unique_ptr<TableScan> p0 = TableScan::Create(tabdesc);
    ASSERT_NE(p0.get(), nullptr);

    std::unique_ptr<ExprNode> col0 = Variable::Create(p0->get_output_schema(),
                                                      0);
    ASSERT_NE(col0.get(), nullptr);
    std::unique_ptr<ExprNode> col1 = Variable::Create(p0->get_output_schema(),
                                                      1);
    ASSERT_NE(col1.get(), nullptr);

    std::vector<std::unique_ptr<ExprNode>> sort_exprs;
    sort_exprs.emplace_back(std::move(col1));
    sort_exprs.emplace_back(std::move(col0));
    std::unique_ptr<PlanNode> p1 = Sort::Create(std::move(p0),
                                                std::move(sort_exprs),
                                                {false, false});
    ASSERT_NE(p1.get(), nullptr);

    std::unique_ptr<PlanExecState> s = p1->create_exec_state();
    ASSERT_NE(s.get(), nullptr);

    ASSERT_NO_ERROR(s->init());
    for (size_t i = 0; i < n; ++i) {
        ASSERT_TRUE(s->next_tuple());
        std::vector<NullableDatumRef> rec = s->get_record();
        ASSERT_EQ(rec.size(), 2);
        ASSERT_FALSE(rec[0].isnull());
        uint32_t val = rec[0].GetUInt32();
        ASSERT_EQ(val, i % (n / 10)) << i;

        ASSERT_FALSE(rec[1].isnull());
        absl::string_view val1  = varchar_to_string_view(rec[1]);
        ASSERT_EQ(val1[0], 'A') << i;

        int last_digit = (int)(val1[1] - '0');
        ASSERT_EQ(last_digit, (int)(i / (n / 10)));
    }

    ASSERT_FALSE(s->next_tuple());
    ASSERT_FALSE(s->next_tuple());

    ASSERT_NO_ERROR(s->close());

    TDB_TEST_END
}

TEST_F(BasicTestSort, TestSortTwoColumnsDescAsc) {
    TDB_TEST_BEGIN

    const size_t n = 40960;
    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(false, n, tabdesc);
    std::unique_ptr<TableScan> p0 = TableScan::Create(tabdesc);
    ASSERT_NE(p0.get(), nullptr);

    std::unique_ptr<ExprNode> col0 = Variable::Create(p0->get_output_schema(),
                                                      0);
    ASSERT_NE(col0.get(), nullptr);
    std::unique_ptr<ExprNode> col1 = Variable::Create(p0->get_output_schema(),
                                                      1);
    ASSERT_NE(col1.get(), nullptr);

    std::vector<std::unique_ptr<ExprNode>> sort_exprs;
    sort_exprs.emplace_back(std::move(col1));
    sort_exprs.emplace_back(std::move(col0));
    std::unique_ptr<PlanNode> p1 = Sort::Create(std::move(p0),
                                                std::move(sort_exprs),
                                                {true, false});
    ASSERT_NE(p1.get(), nullptr);

    std::unique_ptr<PlanExecState> s = p1->create_exec_state();
    ASSERT_NE(s.get(), nullptr);

    ASSERT_NO_ERROR(s->init());
    size_t round = 0;
    for (size_t rewind_pos : {(size_t) 0,
                              (size_t) 1, n / 4, n / 2, n - 1, n, n}) {
        for (size_t i = 0; i < rewind_pos; ++i) {
            ASSERT_TRUE(s->next_tuple());
            std::vector<NullableDatumRef> rec = s->get_record();
            ASSERT_EQ(rec.size(), 2);
            ASSERT_FALSE(rec[0].isnull());
            uint32_t val = rec[0].GetUInt32();
            ASSERT_EQ(val, i % (n / 10)) << i << ' ' << round;

            ASSERT_FALSE(rec[1].isnull());
            absl::string_view val1  = varchar_to_string_view(rec[1]);
            ASSERT_EQ(val1[0], 'A') << i;

            int last_digit = (int)(val1[1] - '0');
            ASSERT_EQ(last_digit, 9 - (int)(i / (n / 10)));
        }

        if (rewind_pos == n) {
            ASSERT_FALSE(s->next_tuple());
            ASSERT_FALSE(s->next_tuple());
        }
        ASSERT_NO_ERROR(s->rewind());
        ++round;
    }

    ASSERT_NO_ERROR(s->close());

    TDB_TEST_END
}

TEST_F(BasicTestSort, TestSortNullableColumnAsc) {
    TDB_TEST_BEGIN

    const size_t n = 128;
    std::shared_ptr<const TableDesc> tabdesc;
    CreateNewTable(true, 0, tabdesc);
    std::unique_ptr<TableScan> p0 = TableScan::Create(tabdesc);

    // create a small test case here
    std::unique_ptr<Table> tab = Table::Create(tabdesc);
    std::vector<Datum> data;
    data.reserve(2);
    maxaligned_char_buf buf;
    buf.reserve(128);
    for (size_t i = 0; i < n; ++i) {
        data.clear();
        if (i & 1) {
            data.emplace_back(Datum::FromNull());
        } else {
            size_t x = ((i / 2) ^ s_reorder_mask) & (n / 2 - 1);
            data.emplace_back(Datum::From(x));
        }
        data.emplace_back(ExpectedValueF1(i, false));
        buf.clear();
        ASSERT_NE(tabdesc->GetSchema()->WritePayloadToBuffer(data, buf), -1);
        Record rec(buf);
        tab->InsertRecord(rec);
    }

    ASSERT_NE(p0.get(), nullptr);

    std::unique_ptr<ExprNode> col0 = Variable::Create(p0->get_output_schema(),
                                                      0);
    ASSERT_NE(col0.get(), nullptr);

    std::vector<std::unique_ptr<ExprNode>> sort_exprs;
    sort_exprs.emplace_back(std::move(col0));
    std::unique_ptr<PlanNode> p1 = Sort::Create(std::move(p0),
                                                std::move(sort_exprs),
                                                {false});
    ASSERT_NE(p1.get(), nullptr);

    std::unique_ptr<PlanExecState> s = p1->create_exec_state();
    ASSERT_NE(s.get(), nullptr);

    ASSERT_NO_ERROR(s->init());
    for (size_t i = 0; i < n; ++i) {
        ASSERT_TRUE(s->next_tuple());
        std::vector<NullableDatumRef> rec = s->get_record();
        ASSERT_EQ(rec.size(), 2);

        if (i < n / 2) {
            ASSERT_TRUE(rec[0].isnull()) << i;
        } else {
            ASSERT_FALSE(rec[0].isnull());
            uint32_t val = rec[0].GetUInt32();
            ASSERT_EQ(val, i - n / 2) << i;
        }

        ASSERT_FALSE(rec[1].isnull());
        absl::string_view val1  = varchar_to_string_view(rec[1]);
        ASSERT_EQ(val1[0], 'A') << i;
    }

    ASSERT_FALSE(s->next_tuple());
    ASSERT_FALSE(s->next_tuple());

    ASSERT_NO_ERROR(s->close());

    TDB_TEST_END
}

}    // namespace taco
