#include "base/TDBDBTest.h"

#include "catalog/Schema.h"
#include "plan/TempTable.h"
#include "plan/CartesianProduct.h"
#include "execution/CartesianProductState.h"

namespace taco {

static constexpr uint32_t s_reorder_mask = 0xf73cda37u;
static const char *s_f1 = "f1";
static const char *s_f2 = "f2";

class BasicTestCartesianProduct: public TDBDBTest {
protected:
    std::unique_ptr<TempTable>
    CreateTempTable(uint32_t n, Oid typid, absl::string_view field_name) {
        auto schema = Schema::Create({typid}, {0}, {false},
                                     {cast_as_string(field_name)});
        std::unique_ptr<TempTable> plan = TempTable::Create(schema);
        for (size_t i = 0; i < n; ++i) {
            std::vector<Datum> rec;
            rec.emplace_back(Datum::From((uint64_t)(i ^ s_reorder_mask)));
            plan->insert_record(std::move(rec));
        }
        return plan;
    }
};

TEST_F(BasicTestCartesianProduct, TestManyToOne) {
    TDB_TEST_BEGIN

    const uint32_t n1 = 10;
    const uint32_t n2 = 1;
    std::unique_ptr<TempTable> p1 =
        CreateTempTable(n1, initoids::TYP_UINT4, s_f1);
    std::unique_ptr<TempTable> p2 =
        CreateTempTable(n2, initoids::TYP_UINT8, s_f2);
    std::unique_ptr<PlanNode> p3 = CartesianProduct::Create(std::move(p1),
                                                          std::move(p2));

    const Schema *output_sch = p3->get_output_schema();
    ASSERT_EQ(output_sch->GetNumFields(), 2);
    ASSERT_EQ(output_sch->GetFieldTypeId(0), initoids::TYP_UINT4);
    ASSERT_EQ(output_sch->GetFieldTypeId(1), initoids::TYP_UINT8);

    std::unique_ptr<PlanExecState> s = p3->create_exec_state();
    s->init();

    const uint32_t n = n1 * n2;
    std::vector<bool> found;
    size_t round = 0;
    for (uint32_t rewind_pos : { (uint32_t) 0, (uint32_t) 1,
                                 n / 5, n - 1, n, n}) {
        found.clear();
        found.resize(n, false);
        uint32_t nfound = 0;
        for (uint32_t i = 0; i < rewind_pos; ++i) {
            ASSERT_TRUE(s->next_tuple()) << round;
            std::vector<NullableDatumRef> rec = s->get_record();
            ASSERT_EQ(rec.size(), 2);
            uint32_t k1 = rec[0].GetUInt32() ^ s_reorder_mask;
            ASSERT_GE(k1, 0);
            ASSERT_LT(k1, n1);

            uint32_t k2 = rec[1].GetUInt64() ^ s_reorder_mask;
            ASSERT_GE(k2, 0);
            ASSERT_LT(k2, n2);

            uint32_t k = k1 * n2 + k2;
            ASSERT_FALSE(found[k]) << "result ("
                << rec[0].GetUInt32()
                << ", "
                << rec[1].GetUInt64()
                <<") appeared more than once";
            found[k] = true;
            ++nfound;
        }

        if (rewind_pos == n) {
            ASSERT_FALSE(s->next_tuple());
            ASSERT_FALSE(s->next_tuple());
        }
        ASSERT_EQ(nfound, rewind_pos);
        s->rewind();
        ++round;
    }
    s->close();
    TDB_TEST_END
}

TEST_F(BasicTestCartesianProduct, TestOneToMany) {
    TDB_TEST_BEGIN

    const uint32_t n1 = 1;
    const uint32_t n2 = 10;
    std::unique_ptr<TempTable> p1 =
        CreateTempTable(n1, initoids::TYP_UINT4, s_f1);
    std::unique_ptr<TempTable> p2 =
        CreateTempTable(n2, initoids::TYP_UINT8, s_f2);
    std::unique_ptr<PlanNode> p3 = CartesianProduct::Create(std::move(p1),
                                                          std::move(p2));

    const Schema *output_sch = p3->get_output_schema();
    ASSERT_EQ(output_sch->GetNumFields(), 2);
    ASSERT_EQ(output_sch->GetFieldTypeId(0), initoids::TYP_UINT4);
    ASSERT_EQ(output_sch->GetFieldTypeId(1), initoids::TYP_UINT8);

    std::unique_ptr<PlanExecState> s = p3->create_exec_state();
    s->init();

    const uint32_t n = n1 * n2;
    std::vector<bool> found;
    size_t round = 0;
    for (uint32_t rewind_pos : { (uint32_t) 0, (uint32_t) 1,
                                 n / 5, n - 1, n, n}) {
        found.clear();
        found.resize(n, false);
        uint32_t nfound = 0;
        for (uint32_t i = 0; i < rewind_pos; ++i) {
            ASSERT_TRUE(s->next_tuple()) << round;
            std::vector<NullableDatumRef> rec = s->get_record();
            ASSERT_EQ(rec.size(), 2);
            uint32_t k1 = rec[0].GetUInt32() ^ s_reorder_mask;
            ASSERT_GE(k1, 0);
            ASSERT_LT(k1, n1);

            uint32_t k2 = rec[1].GetUInt64() ^ s_reorder_mask;
            ASSERT_GE(k2, 0);
            ASSERT_LT(k2, n2);

            uint32_t k = k1 * n2 + k2;
            ASSERT_FALSE(found[k]) << "result ("
                << rec[0].GetUInt32()
                << ", "
                << rec[1].GetUInt64()
                <<") appeared more than once";
            found[k] = true;
            ++nfound;
        }

        if (rewind_pos == n) {
            ASSERT_FALSE(s->next_tuple());
            ASSERT_FALSE(s->next_tuple());
        }
        ASSERT_EQ(nfound, rewind_pos);
        s->rewind();
        ++round;
    }
    s->close();
    TDB_TEST_END
}

TEST_F(BasicTestCartesianProduct, TestManyToMany) {
    TDB_TEST_BEGIN

    const uint32_t n1 = 10;
    const uint32_t n2 = 10;
    std::unique_ptr<TempTable> p1 =
        CreateTempTable(n1, initoids::TYP_UINT4, s_f1);
    std::unique_ptr<TempTable> p2 =
        CreateTempTable(n2, initoids::TYP_UINT8, s_f2);
    std::unique_ptr<PlanNode> p3 = CartesianProduct::Create(std::move(p1),
                                                          std::move(p2));

    const Schema *output_sch = p3->get_output_schema();
    ASSERT_EQ(output_sch->GetNumFields(), 2);
    ASSERT_EQ(output_sch->GetFieldTypeId(0), initoids::TYP_UINT4);
    ASSERT_EQ(output_sch->GetFieldTypeId(1), initoids::TYP_UINT8);

    std::unique_ptr<PlanExecState> s = p3->create_exec_state();
    s->init();

    const uint32_t n = n1 * n2;
    std::vector<bool> found;
    size_t round = 0;
    for (uint32_t rewind_pos : { (uint32_t) 0, (uint32_t) 1,
                                 n / 5, n - 1, n, n}) {
        found.clear();
        found.resize(n, false);
        uint32_t nfound = 0;
        for (uint32_t i = 0; i < rewind_pos; ++i) {
            ASSERT_TRUE(s->next_tuple()) << round;
            std::vector<NullableDatumRef> rec = s->get_record();
            ASSERT_EQ(rec.size(), 2);
            uint32_t k1 = rec[0].GetUInt32() ^ s_reorder_mask;
            ASSERT_GE(k1, 0);
            ASSERT_LT(k1, n1);

            uint32_t k2 = rec[1].GetUInt64() ^ s_reorder_mask;
            ASSERT_GE(k2, 0);
            ASSERT_LT(k2, n2);

            uint32_t k = k1 * n2 + k2;
            ASSERT_FALSE(found[k]) << "result ("
                << rec[0].GetUInt32()
                << ", "
                << rec[1].GetUInt64()
                <<") appeared more than once";
            found[k] = true;
            ++nfound;
        }

        if (rewind_pos == n) {
            ASSERT_FALSE(s->next_tuple());
            ASSERT_FALSE(s->next_tuple());
        }
        ASSERT_EQ(nfound, rewind_pos);
        s->rewind();
        ++round;
    }
    s->close();
    TDB_TEST_END
}

TEST_F(BasicTestCartesianProduct, TestEmptyResult) {
    TDB_TEST_BEGIN

    uint32_t n1 = 0;
    uint32_t n2 = 10;
    std::unique_ptr<TempTable> p1 =
        CreateTempTable(n1, initoids::TYP_UINT4, s_f1);
    std::unique_ptr<TempTable> p2 =
        CreateTempTable(n2, initoids::TYP_UINT8, s_f2);
    std::unique_ptr<PlanNode> p3 = CartesianProduct::Create(std::move(p1),
                                                          std::move(p2));
    std::unique_ptr<PlanExecState> s = p3->create_exec_state();
    s->init();
    ASSERT_FALSE(s->next_tuple());
    ASSERT_FALSE(s->next_tuple());
    s->rewind();
    ASSERT_FALSE(s->next_tuple());
    ASSERT_FALSE(s->next_tuple());
    s->close();

    n1 = 10;
    n2 = 0;
    p1 = CreateTempTable(n1, initoids::TYP_UINT4, s_f1);
    p2 = CreateTempTable(n2, initoids::TYP_UINT8, s_f2);
    p3 = CartesianProduct::Create(std::move(p1), std::move(p2));

    s = p3->create_exec_state();
    s->init();
    ASSERT_FALSE(s->next_tuple());
    ASSERT_FALSE(s->next_tuple());
    s->rewind();
    ASSERT_FALSE(s->next_tuple());
    ASSERT_FALSE(s->next_tuple());
    s->close();

    n1 = 0;
    n2 = 0;
    p1 = CreateTempTable(n1, initoids::TYP_UINT4, s_f1);
    p2 = CreateTempTable(n2, initoids::TYP_UINT8, s_f2);
    p3 = CartesianProduct::Create(std::move(p1), std::move(p2));

    s = p3->create_exec_state();
    s->init();
    ASSERT_FALSE(s->next_tuple());
    ASSERT_FALSE(s->next_tuple());
    s->rewind();
    ASSERT_FALSE(s->next_tuple());
    ASSERT_FALSE(s->next_tuple());
    s->close();

    TDB_TEST_END
}

}   // namespace taco

