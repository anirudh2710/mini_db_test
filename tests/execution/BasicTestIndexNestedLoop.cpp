#include "base/TDBDBTest.h"

#include "catalog/CatCache.h"
#include "plan/TableScan.h"
#include "plan/IndexNestedLoop.h"
#include "execution/PlanExecState.h"
#include "expr/Variable.h"

namespace taco {

class BasicTestIndexNestedLoop: public TDBDBTest {
protected:
    void
    SetUp() override {
        TDBDBTest::SetUp();

        g_db->CreateTable(
            "t1",
            { initoids::TYP_INT4,
              initoids::TYP_INT4},
            { 0, 0 },
            { "f1", "f2" },
            { false, false });
        Oid tabid = g_catcache->FindTableByName("t1");
        ASSERT_NE(tabid, InvalidOid);
        auto t1_tabdesc = g_catcache->FindTableDesc(tabid);

        g_db->CreateTable(
            "t2",
            { initoids::TYP_INT4,
              initoids::TYP_INT4},
            { 0, 0 },
            { "f1", "f2" },
            { false, false });
        Oid tabid2 = g_catcache->FindTableByName("t2");
        ASSERT_NE(tabid2, InvalidOid);
        auto t2_tabdesc = g_catcache->FindTableDesc(tabid2);

        g_db->CreateIndex(
            "t2_f1",
            tabid2,
            IDXTYP(BTREE),
            false,
            {0});
        Oid idxid2 = g_catcache->FindIndexByName("t2_f1");
        ASSERT_NE(idxid2, InvalidOid);
        auto t2_idxdesc = g_catcache->FindIndexDesc(idxid2);

        m_t1 = Table::Create(std::move(t1_tabdesc));
        m_t2 = Table::Create(std::move(t2_tabdesc));
        m_t2_idx = Index::Create(std::move(t2_idxdesc));
    }

    void
    TearDown() override {
        m_t1 = nullptr;
        m_t2 = nullptr;
        m_t2_idx = nullptr;
        TDBDBTest::TearDown();
    }

    bool
    InsertRecord(std::unique_ptr<Table> &t, int32_t key, int32_t id) {
        std::vector<Datum> d;
        d.emplace_back(Datum::From(key));
        d.emplace_back(Datum::From(id));
        m_recbuf.clear();
        if (t->GetTableDesc()->GetSchema()->WritePayloadToBuffer(d, m_recbuf)
            == -1) {
            return false;
        }
        Record rec(m_recbuf);
        t->InsertRecord(rec);
        if (!rec.GetRecordId().IsValid()) {
            return false;
        }

        if (t == m_t2) {
            if (!m_t2_idx->InsertRecord(rec, t->GetTableDesc()->GetSchema())) {
                return false;
            }
        }

        return true;
    }

    std::unique_ptr<PlanNode>
    CreateT1JoinT2() {
        auto outer = TableScan::Create(g_catcache->FindTableDesc(
            m_t1->GetTableDesc()->GetTableEntry()->tabid()));

        auto inner_idxdesc = g_catcache->FindIndexDesc(
            m_t2_idx->GetIndexDesc()->GetIndexEntry()->idxid());

        std::vector<std::unique_ptr<ExprNode>> joinexprs_outer;
        joinexprs_outer.emplace_back(
            Variable::Create(outer->get_output_schema(), 0)
        );
        std::unique_ptr<ExprNode> upper_expr =
            Variable::Create(outer->get_output_schema(), 0);

        std::unique_ptr<IndexNestedLoop> inl =
            IndexNestedLoop::Create(std::move(outer),
                                    std::move(inner_idxdesc),
                                    std::move(joinexprs_outer),
                                    std::move(upper_expr),
                                    false,
                                    false);
        return inl;
    }

    void
    CheckRecordValues(absl::string_view callsite_file,
                      uint64_t callsite_lineno,
                      const std::vector<NullableDatumRef> &data,
                      int e1, int e2, int e3, int e4) {
        ASSERT_EQ(data.size(), 4)
            << "Note: called from " << callsite_file << ":" << callsite_lineno;
        ASSERT_FALSE(data[0].isnull())
            << "Note: called from " << callsite_file << ":" << callsite_lineno;
        ASSERT_EQ(data[0].GetInt32(), e1)
            << "Note: called from " << callsite_file << ":" << callsite_lineno;
        ASSERT_FALSE(data[1].isnull())
            << "Note: called from " << callsite_file << ":" << callsite_lineno;
        ASSERT_EQ(data[1].GetInt32(), e2)
            << "Note: called from " << callsite_file << ":" << callsite_lineno;
        ASSERT_FALSE(data[2].isnull())
            << "Note: called from " << callsite_file << ":" << callsite_lineno;
        ASSERT_EQ(data[2].GetInt32(), e3)
            << "Note: called from " << callsite_file << ":" << callsite_lineno;
        ASSERT_FALSE(data[3].isnull())
            << "Note: called from " << callsite_file << ":" << callsite_lineno;
        ASSERT_EQ(data[3].GetInt32(), e4)
            << "Note: called from " << callsite_file << ":" << callsite_lineno;
    }

    std::unique_ptr<Table> m_t1;
    std::unique_ptr<Table> m_t2;
    std::unique_ptr<Index> m_t2_idx;
    maxaligned_char_buf m_recbuf;
};

#define CheckRecordValues(...) \
    CheckRecordValues(__FILE__, __LINE__, __VA_ARGS__)

TEST_F(BasicTestIndexNestedLoop, TestEmptyJoinEmpty) {
    TDB_TEST_BEGIN

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    Datum sp = Datum::FromNull(), sp2 = Datum::FromNull();
    ASSERT_NO_ERROR(sp = es->save_position());
    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());
    ASSERT_NO_ERROR(sp2 = es->save_position());

    ASSERT_NO_ERROR(es->rewind());
    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_FALSE(es->rewind(sp2));
    ASSERT_FALSE(es->rewind(sp));
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestEmptyJoinNonEmpty) {
    TDB_TEST_BEGIN

    InsertRecord(m_t2, 0, 0);
    InsertRecord(m_t2, 1, 1);

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    Datum sp = Datum::FromNull(), sp2 = Datum::FromNull();
    ASSERT_NO_ERROR(sp = es->save_position());
    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());
    ASSERT_NO_ERROR(sp2 = es->save_position());

    ASSERT_NO_ERROR(es->rewind());
    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_FALSE(es->rewind(sp2));
    ASSERT_FALSE(es->rewind(sp));
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestNonEmptyJoinEmpty) {
    TDB_TEST_BEGIN

    InsertRecord(m_t1, 0, 0);
    InsertRecord(m_t1, 1, 1);

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    Datum sp = Datum::FromNull(), sp2 = Datum::FromNull();
    ASSERT_NO_ERROR(sp = es->save_position());
    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());
    ASSERT_NO_ERROR(sp2 = es->save_position());

    ASSERT_NO_ERROR(es->rewind());
    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_FALSE(es->rewind(sp2));
    ASSERT_FALSE(es->rewind(sp));
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestOneToOne1) {
    TDB_TEST_BEGIN

    std::vector<int> t1_keys = {
        2, 3, 4, 8, 10, 15
    };

    std::vector<int> t2_keys = {
        1, 2, 4, 7, 100
    };

    std::vector<NullableDatumRef> data;

    for (size_t i = 0; i < t1_keys.size(); ++i) {
        InsertRecord(m_t1, t1_keys[i], i);
    }

    for (size_t i = 0; i < t2_keys.size(); ++i) {
        InsertRecord(m_t2, t2_keys[i], i);
    }

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 0, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 2);

    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestOneToOne2) {
    TDB_TEST_BEGIN

    std::vector<int> t1_keys = {
        1, 2, 4, 7, 100
    };

    std::vector<int> t2_keys = {
        2, 3, 4, 8, 10, 15
    };

    std::vector<NullableDatumRef> data;

    for (size_t i = 0; i < t1_keys.size(); ++i) {
        InsertRecord(m_t1, t1_keys[i], i);
    }

    for (size_t i = 0; i < t2_keys.size(); ++i) {
        InsertRecord(m_t2, t2_keys[i], i);
    }

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 0);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 2);

    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestOneToOne3) {
    TDB_TEST_BEGIN

    std::vector<int> t1_keys = {
        1, 2, 4, 7, 100, 150
    };

    std::vector<int> t2_keys = {
        2, 3, 4, 8, 10, 15, 150
    };

    std::vector<NullableDatumRef> data;

    for (size_t i = 0; i < t1_keys.size(); ++i) {
        InsertRecord(m_t1, t1_keys[i], i);
    }

    for (size_t i = 0; i < t2_keys.size(); ++i) {
        InsertRecord(m_t2, t2_keys[i], i);
    }

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 0);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 2);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 5, 150, 6);

    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestOneToMany1) {
    TDB_TEST_BEGIN

    std::vector<int> t1_keys = {
        1, 2, 4, 7, 100, 150
    };

    std::vector<int> t2_keys = {
        2, 2, 3, 4, 4, 8, 10, 15, 150, 150
    };

    std::vector<NullableDatumRef> data;

    for (size_t i = 0; i < t1_keys.size(); ++i) {
        InsertRecord(m_t1, t1_keys[i], i);
    }

    for (size_t i = 0; i < t2_keys.size(); ++i) {
        InsertRecord(m_t2, t2_keys[i], i);
    }

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 0);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 3);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 4);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 5, 150, 8);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 5, 150, 9);

    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestOneToMany2) {
    TDB_TEST_BEGIN

    std::vector<int> t1_keys = {
        1, 2, 4, 7, 100, 150
    };

    std::vector<int> t2_keys = {
        2, 2, 3, 4, 4, 8, 10, 100, 100
    };

    std::vector<NullableDatumRef> data;

    for (size_t i = 0; i < t1_keys.size(); ++i) {
        InsertRecord(m_t1, t1_keys[i], i);
    }

    for (size_t i = 0; i < t2_keys.size(); ++i) {
        InsertRecord(m_t2, t2_keys[i], i);
    }

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 0);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 3);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 4);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 4, 100, 7);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 4, 100, 8);

    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestOneToMany3) {
    TDB_TEST_BEGIN

    std::vector<int> t1_keys = {
        1, 2, 4, 7, 100
    };

    std::vector<int> t2_keys = {
        2, 2, 3, 4, 4, 8, 10
    };

    std::vector<NullableDatumRef> data;

    for (size_t i = 0; i < t1_keys.size(); ++i) {
        InsertRecord(m_t1, t1_keys[i], i);
    }

    for (size_t i = 0; i < t2_keys.size(); ++i) {
        InsertRecord(m_t2, t2_keys[i], i);
    }

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 0);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 3);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 4);

    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestOneToMany4) {
    TDB_TEST_BEGIN

    std::vector<int> t1_keys = {
        2, 3, 4, 7, 100, 150
    };

    std::vector<int> t2_keys = {
        1, 2, 2, 4, 4, 8, 10, 100, 100
    };

    std::vector<NullableDatumRef> data;

    for (size_t i = 0; i < t1_keys.size(); ++i) {
        InsertRecord(m_t1, t1_keys[i], i);
    }

    for (size_t i = 0; i < t2_keys.size(); ++i) {
        InsertRecord(m_t2, t2_keys[i], i);
    }

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 0, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 0, 2, 2);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 3);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 4);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 4, 100, 7);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 4, 100, 8);

    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestManyToOne1) {
    TDB_TEST_BEGIN

    std::vector<int> t1_keys = {
        2, 2, 3, 4, 4, 8, 10, 15, 150, 150
    };

    std::vector<int> t2_keys = {
        1, 2, 4, 7, 100, 150
    };

    std::vector<NullableDatumRef> data;

    for (size_t i = 0; i < t1_keys.size(); ++i) {
        InsertRecord(m_t1, t1_keys[i], i);
    }

    for (size_t i = 0; i < t2_keys.size(); ++i) {
        InsertRecord(m_t2, t2_keys[i], i);
    }

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 0, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 3, 4, 2);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 4, 4, 2);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 8, 150, 5);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 9, 150, 5);

    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestManyToOne2) {
    TDB_TEST_BEGIN

    std::vector<int> t1_keys = {
        2, 2, 3, 4, 4, 8, 10, 100, 100
    };

    std::vector<int> t2_keys = {
        1, 2, 4, 7, 100, 150
    };

    std::vector<NullableDatumRef> data;

    for (size_t i = 0; i < t1_keys.size(); ++i) {
        InsertRecord(m_t1, t1_keys[i], i);
    }

    for (size_t i = 0; i < t2_keys.size(); ++i) {
        InsertRecord(m_t2, t2_keys[i], i);
    }

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 0, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 3, 4, 2);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 4, 4, 2);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 7, 100, 4);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 8, 100, 4);

    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestManyToOne3) {
    TDB_TEST_BEGIN

    std::vector<int> t1_keys = {
        2, 2, 3, 4, 4, 8, 10
    };

    std::vector<int> t2_keys = {
        1, 2, 4, 7, 100
    };

    std::vector<NullableDatumRef> data;

    for (size_t i = 0; i < t1_keys.size(); ++i) {
        InsertRecord(m_t1, t1_keys[i], i);
    }

    for (size_t i = 0; i < t2_keys.size(); ++i) {
        InsertRecord(m_t2, t2_keys[i], i);
    }

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 0, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 3, 4, 2);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 4, 4, 2);

    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestManyToOne4) {
    TDB_TEST_BEGIN

    std::vector<int> t1_keys = {
        1, 2, 2, 4, 4, 8, 10, 100, 100
    };

    std::vector<int> t2_keys = {
        2, 3, 4, 7, 100, 150
    };

    std::vector<NullableDatumRef> data;

    for (size_t i = 0; i < t1_keys.size(); ++i) {
        InsertRecord(m_t1, t1_keys[i], i);
    }

    for (size_t i = 0; i < t2_keys.size(); ++i) {
        InsertRecord(m_t2, t2_keys[i], i);
    }

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 0);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 2, 2, 0);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 3, 4, 2);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 4, 4, 2);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 7, 100, 4);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 8, 100, 4);

    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestManyToMany1) {
    TDB_TEST_BEGIN

    std::vector<int> t1_keys = {
        1, 2, 2, 4, 4, 8, 10, 100, 100
    };

    std::vector<int> t2_keys = {
        2, 2, 3, 4, 4, 7, 100, 100, 150
    };

    std::vector<NullableDatumRef> data;

    for (size_t i = 0; i < t1_keys.size(); ++i) {
        InsertRecord(m_t1, t1_keys[i], i);
    }

    for (size_t i = 0; i < t2_keys.size(); ++i) {
        InsertRecord(m_t2, t2_keys[i], i);
    }

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 0);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 2, 2, 0);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 2, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 3, 4, 3);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 3, 4, 4);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 4, 4, 3);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 4, 4, 4);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 7, 100, 6);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 7, 100, 7);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 8, 100, 6);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 8, 100, 7);

    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestManyToMany2) {
    TDB_TEST_BEGIN

    std::vector<int> t1_keys = {
        2, 2, 3, 4, 4, 8, 10, 150, 150, 200
    };

    std::vector<int> t2_keys = {
        1, 2, 2, 4, 4, 7, 100, 100, 150, 150
    };

    std::vector<NullableDatumRef> data;

    for (size_t i = 0; i < t1_keys.size(); ++i) {
        InsertRecord(m_t1, t1_keys[i], i);
    }

    for (size_t i = 0; i < t2_keys.size(); ++i) {
        InsertRecord(m_t2, t2_keys[i], i);
    }

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 0, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 0, 2, 2);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 2);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 3, 4, 3);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 3, 4, 4);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 4, 4, 3);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 4, 4, 4);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 7, 150, 8);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 7, 150, 9);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 8, 150, 8);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 8, 150, 9);

    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestOneToOneWithRewind) {
    TDB_TEST_BEGIN

    std::vector<int> t1_keys = {
        1, 2, 4, 7, 100
    };

    std::vector<int> t2_keys = {
        2, 3, 4, 8, 10, 15
    };

    std::vector<NullableDatumRef> data;

    for (size_t i = 0; i < t1_keys.size(); ++i) {
        InsertRecord(m_t1, t1_keys[i], i);
    }

    for (size_t i = 0; i < t2_keys.size(); ++i) {
        InsertRecord(m_t2, t2_keys[i], i);
    }

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    Datum sp = Datum::FromNull(),
          sp2 = Datum::FromNull(),
          sp3 = Datum::FromNull();
    ASSERT_NO_ERROR(sp = es->save_position());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 0);
    ASSERT_NO_ERROR(sp2 = es->save_position());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 2);
    ASSERT_NO_ERROR(sp3 = es->save_position());

    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    // rewinds back to the first record
    ASSERT_TRUE(es->rewind(sp2));
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 0);
    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 2);
    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    // rewinds back to the beginning
    ASSERT_FALSE(es->rewind(sp));
    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 0);
    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 2);
    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    // rewinds back to the second tuple
    ASSERT_TRUE(es->rewind(sp3));
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 2);
    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestOneToManyWithRewind) {
    TDB_TEST_BEGIN

    std::vector<int> t1_keys = {
        1, 2, 4, 7, 100, 150
    };

    std::vector<int> t2_keys = {
        2, 2, 3, 4, 4, 8, 10, 15, 150, 150
    };

    std::vector<NullableDatumRef> data;

    for (size_t i = 0; i < t1_keys.size(); ++i) {
        InsertRecord(m_t1, t1_keys[i], i);
    }

    for (size_t i = 0; i < t2_keys.size(); ++i) {
        InsertRecord(m_t2, t2_keys[i], i);
    }

    Datum sp = Datum::FromNull(),
          sp2 = Datum::FromNull();

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 0);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 1);
    ASSERT_NO_ERROR(sp = es->save_position());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 3);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 4);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 5, 150, 8);
    ASSERT_NO_ERROR(sp2 = es->save_position());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 5, 150, 9);

    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    // rewind to the second join result
    ASSERT_TRUE(es->rewind(sp));
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 1);
    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 2, 4, 3);
    ASSERT_TRUE(es->next_tuple());

    // rewind to the fifth join result (actually skipping the fourth)
    ASSERT_TRUE(es->rewind(sp2));
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 5, 150, 8);
    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 5, 150, 9);
    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestManyToOneWithRewind) {
    TDB_TEST_BEGIN

    std::vector<int> t1_keys = {
        2, 2, 3, 4, 4, 8, 10, 15, 150, 150
    };

    std::vector<int> t2_keys = {
        1, 2, 4, 7, 100, 150
    };

    std::vector<NullableDatumRef> data;

    for (size_t i = 0; i < t1_keys.size(); ++i) {
        InsertRecord(m_t1, t1_keys[i], i);
    }

    for (size_t i = 0; i < t2_keys.size(); ++i) {
        InsertRecord(m_t2, t2_keys[i], i);
    }

    Datum sp = Datum::FromNull(),
          sp2 = Datum::FromNull();

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 0, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 3, 4, 2);
    ASSERT_NO_ERROR(sp = es->save_position());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 4, 4, 2);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 8, 150, 5);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 9, 150, 5);
    ASSERT_NO_ERROR(sp2 = es->save_position());

    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());


    // rewind to the third record
    ASSERT_TRUE(es->rewind(sp));
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 3, 4, 2);
    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 4, 4, 2);
    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 8, 150, 5);
    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 9, 150, 5);
    ASSERT_FALSE(es->next_tuple());

    // rewind to the last record
    ASSERT_TRUE(es->rewind(sp2));
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 150, 9, 150, 5);
    ASSERT_FALSE(es->next_tuple());

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

TEST_F(BasicTestIndexNestedLoop, TestManyToManyWithRewind) {
    TDB_TEST_BEGIN

    std::vector<int> t1_keys = {
        1, 2, 2, 4, 4, 8, 10, 100, 100
    };

    std::vector<int> t2_keys = {
        2, 2, 3, 4, 4, 7, 100, 100, 150
    };

    std::vector<NullableDatumRef> data;

    for (size_t i = 0; i < t1_keys.size(); ++i) {
        InsertRecord(m_t1, t1_keys[i], i);
    }

    for (size_t i = 0; i < t2_keys.size(); ++i) {
        InsertRecord(m_t2, t2_keys[i], i);
    }

    Datum sp = Datum::FromNull(),
          sp2 = Datum::FromNull(),
          sp3 = Datum::FromNull();

    auto inl = CreateT1JoinT2();
    auto es = inl->create_exec_state();
    ASSERT_NE(es, nullptr);
    ASSERT_NO_ERROR(es->init());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 0);
    ASSERT_NO_ERROR(sp = es->save_position());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 2, 2, 0);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 2, 2, 1);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 3, 4, 3);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 3, 4, 4);
    ASSERT_NO_ERROR(sp2 = es->save_position());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 4, 4, 3);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 4, 4, 4);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 7, 100, 6);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 7, 100, 7);

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 8, 100, 6);
    ASSERT_NO_ERROR(sp3 = es->save_position());

    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 8, 100, 7);

    ASSERT_FALSE(es->next_tuple());
    ASSERT_FALSE(es->next_tuple());

    // rewind to the 11th record
    ASSERT_TRUE(es->rewind(sp3));
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 8, 100, 6);
    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 100, 8, 100, 7);
    ASSERT_FALSE(es->next_tuple());

    // rewind to the 6th record
    ASSERT_TRUE(es->rewind(sp2));
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 3, 4, 4);
    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 4, 4, 3);

    // rewind to the 1st record
    ASSERT_TRUE(es->rewind(sp));
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 0);
    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 1, 2, 1);
    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 2, 2, 0);
    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 2, 2, 2, 1);
    ASSERT_TRUE(es->next_tuple());
    ASSERT_NO_ERROR(data = es->get_record());
    CheckRecordValues(data, 4, 3, 4, 3);

    ASSERT_NO_ERROR(es->close());
    TDB_TEST_END
}

}   // namespace taco

