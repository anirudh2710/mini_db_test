#ifndef TESTS_INDEX_BTREE_TESTBTREE_H
#define TESTS_INDEX_BTREE_TESTBTREE_H

#include "base/TDBDBTest.h"

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>

#include "index/idxtyps.h"
#include "index/btree/BTreeInternal.h"
#include "catalog/CatCache.h"
#include "catalog/IndexDesc.h"
#include "catalog/TableDesc.h"
#include "catalog/systables/initoids.h"
#include "storage/BufferManager.h"
#include "storage/FileManager.h"
#include "storage/VarlenDataPage.h"

// defined in index/btree/BTreeDelete.cpp
ABSL_DECLARE_FLAG(bool, btree_enable_rebalancing_pages);

ABSL_FLAG(bool, test_btree_catcache_use_volatiletree, true,
          "whether to use VolatileTree instead of B-tree in btree tests");
ABSL_FLAG(int64_t, reorder_mask, 0xf3127abf,
          "the xor mask used for generating keys");
ABSL_FLAG(uint32_t, test_btree_seed, 0xef220108u,
        "The seed used in the random number generator of btree test.");

namespace taco {

// XXX the return type deduction wasn't available until c++14.
// We can use decltype but it's a hassle to do so.
// But in any case, we should definitely make this macro generic and combine
// these two into one signle macro/template to allow us to test private member
// functions in friend classes in other tests in the future.
#define DEFINE_PRIVATE_MEMFN_ACCESSOR(typname, funcname) \
    template <class... Args> \
    typname funcname(std::unique_ptr<BTree> &bt, Args&& ...args) { \
        return bt->funcname(std::forward<Args>(args)...); \
    }

#define DEFINE_PRIVATE_MEMFN_ACCESSOR_VOID(funcname) \
    template <class... Args> \
    void funcname(std::unique_ptr<BTree> &bt, Args&& ...args) { \
        bt->funcname(std::forward<Args>(args)...); \
    }

static const char *s_tab = "A";
static const char *s_idx_1 = "A_f1";
static const char *s_idx_2 = "A_f2";
static const char *s_idx_1_2 = "A_f1_f2";
static const char *s_idx_2_1 = "A_f2_f1";
static const char *s_idx_1_uniq = "A_f1_uniq";
static const char *s_idx_2_uniq = "A_f2_uniq";
static const char *s_tab_f1 = "f1";
static const char *s_tab_f2 = "f2";
static constexpr int64_t s_max_key = 0xffffffffl;
static constexpr const size_t s_f1_len = 8;
static constexpr const size_t s_f2_len = 384;

class TestBTree : public TDBDBTest {
protected:
    TestBTree():
        m_d{{Datum::FromNull()}, {Datum::FromNull()}} {}

    std::shared_ptr<const TableDesc> m_tabdesc;
    std::shared_ptr<const IndexDesc> m_idxdesc_1;
    std::shared_ptr<const IndexDesc> m_idxdesc_2;
    std::shared_ptr<const IndexDesc> m_idxdesc_1_2;
    std::shared_ptr<const IndexDesc> m_idxdesc_2_1;
    std::shared_ptr<const IndexDesc> m_idxdesc_1_uniq;
    std::shared_ptr<const IndexDesc> m_idxdesc_2_uniq;

    // page header = 24 + 16 = 40
    // available space = 4056

    // payload = 8, leaf reclen = 16, internal reclen = 24, payload = 8
    // fan-out: leaf = 202 (with 16 left), internal = 145 (with 4 left),
    std::unique_ptr<BTree> m_idx_1;

    // max payload = 384, leaf reclen = 392, internal reclen = 400
    // fan-out: leaf = 10 (with 96 left), internal = 10 (with 400 left)
    std::unique_ptr<BTree> m_idx_2;

    // TBD
    std::unique_ptr<BTree> m_idx_1_2;
    std::unique_ptr<BTree> m_idx_2_1;
    std::unique_ptr<BTree> m_idx_1_uniq;
    std::unique_ptr<BTree> m_idx_2_uniq;

    int64_t m_reorder_mask;

    std::string m_strbuf;

    unique_malloced_ptr m_index_key;

    Datum m_d[2];

    int64_t
    get_ikey(int64_t i) {
        return i ^ m_reorder_mask;
    }

    absl::string_view
    get_skey(int64_t i, size_t len = 380) {
        snprintf(&m_strbuf[0], 11, "0x%08lx", get_ikey(i));
        m_strbuf[10] = 'A';
        return absl::string_view(m_strbuf.c_str(), len);
    }

    IndexKey *
    get_key_f1(int64_t i) {
        m_d[0] = Datum::From(get_ikey(i));
        IndexKey::Construct(m_index_key.get(), m_d, 1);
        return (IndexKey*) m_index_key.get();
    }

    IndexKey *
    get_key_f2(int64_t i, size_t len = 380) {
        // this assumes varchar is a plain byte array
        m_d[0] = Datum::FromVarlenAsStringView(get_skey(i, len));
        IndexKey::Construct(m_index_key.get(), m_d, 1);
        return (IndexKey*) m_index_key.get();
    }

    IndexKey *
    get_key_f1_f2(int64_t i, int64_t j, size_t len = 380) {
        m_d[0] = Datum::From(get_ikey(i));
        m_d[1] = Datum::FromVarlenAsStringView(get_skey(j, len));
        IndexKey::Construct(m_index_key.get(), m_d, 2);
        return (IndexKey*) m_index_key.get();
    }

    /*!
     * This generates a key of (f2, f1), but the indexes f1_i and f2_j are
     * passed in the inversed order!!
     */
    IndexKey *
    get_key_f2_f1(int64_t f1_i, int64_t f2_j, size_t len = 380) {
        m_d[0] = Datum::FromVarlenAsStringView(get_skey(f2_j, len));
        m_d[1] = Datum::From(get_ikey(f1_i));
        IndexKey::Construct(m_index_key.get(), m_d, 2);
        return (IndexKey*) m_index_key.get();
    }

    int64_t
    read_f1_payload(const char *payload) {
        return m_idxdesc_1->GetKeySchema()
            ->GetField(0, payload).GetInt64();
    }

    absl::string_view
    read_f2_payload(const char *payload) {
        return m_idxdesc_2->GetKeySchema()
            ->GetField(0, payload).GetVarlenAsStringView();
    }

    std::pair<int64_t, absl::string_view>
    read_f1_f2_payload(const char *payload) {
        Datum f2 = m_idxdesc_1_2->GetKeySchema()->GetField(1, payload);
        return std::make_pair(
            m_idxdesc_1_2->GetKeySchema()->GetField(0, payload).GetInt64(),
            f2.isnull() ? "" : f2.GetVarlenAsStringView());
    }

    void
    LoadDB() {
        Oid tabid = g_catcache->FindTableByName(s_tab);
        ASSERT(tabid != InvalidOid);
        m_tabdesc = g_catcache->FindTableDesc(tabid);

        Oid idxid;

        idxid = g_catcache->FindIndexByName(s_idx_1);
        ASSERT(idxid != InvalidOid);
        m_idxdesc_1 = g_catcache->FindIndexDesc(idxid);
        m_idx_1 = BTree::Create(m_idxdesc_1);

        idxid = g_catcache->FindIndexByName(s_idx_2);
        ASSERT(idxid != InvalidOid);
        m_idxdesc_2 = g_catcache->FindIndexDesc(idxid);
        m_idx_2 = BTree::Create(m_idxdesc_2);

        idxid = g_catcache->FindIndexByName(s_idx_1_2);
        ASSERT(idxid != InvalidOid);
        m_idxdesc_1_2 = g_catcache->FindIndexDesc(idxid);
        m_idx_1_2 = BTree::Create(m_idxdesc_1_2);

        idxid = g_catcache->FindIndexByName(s_idx_2_1);
        ASSERT(idxid != InvalidOid);
        m_idxdesc_2_1 = g_catcache->FindIndexDesc(idxid);
        m_idx_2_1 = BTree::Create(m_idxdesc_2_1);

        idxid = g_catcache->FindIndexByName(s_idx_1_uniq);
        ASSERT(idxid != InvalidOid);
        m_idxdesc_1_uniq = g_catcache->FindIndexDesc(idxid);
        m_idx_1_uniq = BTree::Create(m_idxdesc_1_uniq);

        idxid = g_catcache->FindIndexByName(s_idx_2_uniq);
        ASSERT(idxid != InvalidOid);
        m_idxdesc_2_uniq = g_catcache->FindIndexDesc(idxid);
        m_idx_2_uniq = BTree::Create(m_idxdesc_2_uniq);
    }

    void
    CreateDB() {
        // CREATE TABLE A (
        //      f1          INT8 NOT NULL,
        //      f2          VARCHAR(380)
        // )
        //
        // We won't actually insert anything into table A during the tests.
        // This only registers the table in the catalog.
        g_db->CreateTable(s_tab, {initoids::TYP_INT8,
                                  initoids::TYP_VARCHAR},
                                 {0, 380},
                                 {s_tab_f1, s_tab_f2},
                                 {false, true});

        Oid tabid = g_catcache->FindTableByName(s_tab);
        ASSERT(tabid != InvalidOid);
        m_tabdesc = g_catcache->FindTableDesc(tabid);

        Oid idxid;

        // CREATE INDEX A_f1 on A using btree(f1);
        g_db->CreateIndex(s_idx_1, tabid, IDXTYP(BTREE),
                          false, {0});
        idxid = g_catcache->FindIndexByName(s_idx_1);
        ASSERT(idxid != InvalidOid);
        m_idxdesc_1 = g_catcache->FindIndexDesc(idxid);
        m_idx_1 = BTree::Create(m_idxdesc_1);

        // CREATE INDEX A_f2 on A using btree(f2);
        g_db->CreateIndex(s_idx_2, tabid, IDXTYP(BTREE),
                          false, {1});
        idxid = g_catcache->FindIndexByName(s_idx_2);
        ASSERT(idxid != InvalidOid);
        m_idxdesc_2 = g_catcache->FindIndexDesc(idxid);
        m_idx_2 = BTree::Create(m_idxdesc_2);

        // CREATE INDEX A_f1_f2 on A using btree(f1, f2);
        g_db->CreateIndex(s_idx_1_2, tabid, IDXTYP(BTREE),
                          false, {0, 1});
        idxid = g_catcache->FindIndexByName(s_idx_1_2);
        ASSERT(idxid != InvalidOid);
        m_idxdesc_1_2 = g_catcache->FindIndexDesc(idxid);
        m_idx_1_2 = BTree::Create(m_idxdesc_1_2);

        // CREATE INDEX A_f2_f1 on A using btree(f2, f1);
        g_db->CreateIndex(s_idx_2_1, tabid, IDXTYP(BTREE),
                          false, {1, 0});
        idxid = g_catcache->FindIndexByName(s_idx_2_1);
        ASSERT(idxid != InvalidOid);
        m_idxdesc_2_1 = g_catcache->FindIndexDesc(idxid);
        m_idx_2_1 = BTree::Create(m_idxdesc_2_1);

        // CREATE UNIQUE INDEX A_f2_uniq on A using btree(f2);
        g_db->CreateIndex(s_idx_1_uniq, tabid, IDXTYP(BTREE),
                          true, {0});
        idxid = g_catcache->FindIndexByName(s_idx_1_uniq);
        ASSERT(idxid != InvalidOid);
        m_idxdesc_1_uniq = g_catcache->FindIndexDesc(idxid);
        m_idx_1_uniq = BTree::Create(m_idxdesc_1_uniq);

        // CREATE UNIQUE INDEX A_f2_uniq on A using btree(f2);
        g_db->CreateIndex(s_idx_2_uniq, tabid, IDXTYP(BTREE),
                          true, {1});
        idxid = g_catcache->FindIndexByName(s_idx_2_uniq);
        ASSERT(idxid != InvalidOid);
        m_idxdesc_2_uniq = g_catcache->FindIndexDesc(idxid);
        m_idx_2_uniq = BTree::Create(m_idxdesc_2_uniq);
    }


    void
    SetUp() override {
        TDB_TEST_BEGIN

        m_reorder_mask =
            absl::GetFlag(FLAGS_reorder_mask);
        m_strbuf.resize(380, 'A');
        m_index_key = unique_aligned_alloc(8, IndexKey::ComputeStructSize(2));

        g_test_catcache_use_volatiletree =
            absl::GetFlag(FLAGS_test_btree_catcache_use_volatiletree);

        // disable rebalancing pages by default
        absl::SetFlag(&FLAGS_btree_enable_rebalancing_pages, false);

        TDBDBTest::SetUp();

        if (g_test_existing_db_path.empty()) {
            CreateDB();
        } else {
            LoadDB();
        }

        TDB_TEST_END
    }

    void
    TearDown() override {
        TDBDBTest::TearDown();
    }

    void
    EnableRebalancingPages(bool enable = true) {
        absl::SetFlag(&FLAGS_btree_enable_rebalancing_pages, enable);
    }

    size_t
    GetBufferPoolSize() {
        return DEFAULT_BUFPOOL_SIZE;
    }

    /*!
     * This forces the buffer manager to flush all pages by pinning enough
     * amount of pages in the buffer pool at the same time. Currently this only
     * works when there's no buffer frame pinned, and it is the caller's
     * responsibility to ensure that.
     */
    void
    ForceFlushBuffer() {
        auto f = g_fileman->Open(m_tabdesc->GetTableEntry()->tabfid());
        PageNumber pid = f->GetFirstPageNumber();

        std::vector<ScopedBufferId> bufids;
        size_t sz = GetBufferPoolSize();
        bufids.reserve(sz);
        for (size_t i = 0; i < sz; ++i) {
            if (pid == INVALID_PID) {
                pid = f->AllocatePage();
            }
            char *buf;
            bufids.emplace_back(ScopedBufferId(g_bufman->PinPage(pid, &buf)));
        }
    }

    DEFINE_PRIVATE_MEMFN_ACCESSOR(BufferId, CreateNewBTreePage)
    DEFINE_PRIVATE_MEMFN_ACCESSOR(BufferId, GetBTreeMetaPage)
    DEFINE_PRIVATE_MEMFN_ACCESSOR_VOID(CreateLeafRecord)
    DEFINE_PRIVATE_MEMFN_ACCESSOR_VOID(CreateInternalRecord)
    DEFINE_PRIVATE_MEMFN_ACCESSOR(int, BTreeTupleCompare)
    DEFINE_PRIVATE_MEMFN_ACCESSOR(SlotId, BinarySearchOnPage)
    DEFINE_PRIVATE_MEMFN_ACCESSOR(BufferId, FindLeafPage)
    DEFINE_PRIVATE_MEMFN_ACCESSOR(SlotId, FindInsertionSlotIdOnLeaf)
    DEFINE_PRIVATE_MEMFN_ACCESSOR_VOID(InsertRecordOnPage)
    DEFINE_PRIVATE_MEMFN_ACCESSOR_VOID(CreateNewRoot)
    DEFINE_PRIVATE_MEMFN_ACCESSOR(maxaligned_char_buf, SplitPage)
    DEFINE_PRIVATE_MEMFN_ACCESSOR(SlotId, FindDeletionSlotIdOnLeaf)
    DEFINE_PRIVATE_MEMFN_ACCESSOR_VOID(DeleteSlotOnPage)
    DEFINE_PRIVATE_MEMFN_ACCESSOR_VOID(HandleMinPageUsage)
    DEFINE_PRIVATE_MEMFN_ACCESSOR(bool, TryMergeOrRebalancePages)
    DEFINE_PRIVATE_MEMFN_ACCESSOR(bool, MergePages)
    DEFINE_PRIVATE_MEMFN_ACCESSOR(bool, RebalancePages)
};

}    // namespace taco

#endif        // TESTS_INDEX_BTREE_TESTBTREE_H
