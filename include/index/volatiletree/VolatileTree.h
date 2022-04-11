#ifndef INDEX_VOLATILETREE_VOLATILETREE_H
#define INDEX_VOLATILETREE_VOLATILETREE_H

#include "tdb.h"

#include <map>

#include "index/Index.h"

namespace taco {

/*!
 * Initializes the internal state of the in-memory tree index.
 */
void InitVolatileTree();

/*!
 * Cleans up the internal state of the in-memory tree index.
 */
void CleanVolatileTree();

struct VolatileTreeFuncs {
    /*!
     * This is a copy of the index's key schema to be used by std::multimap's
     * comparator to dissemble a record payload.
     */
    Schema m_key_schema;
    std::vector<FunctionInfo> m_lt_funcs;
    std::vector<FunctionInfo> m_eq_funcs;
};

struct VolatileTreeLess {
    VolatileTreeLess(std::shared_ptr<VolatileTreeFuncs> funcs):
        m_funcs(std::move(funcs)) {}

    bool operator()(char *buf1, char *buf2);

private:
    std::shared_ptr<VolatileTreeFuncs> m_funcs;
};

struct VolatileTreeBase {
    // key: some tuplebuf, value: record id
    typedef std::multimap<char*, RecordId, VolatileTreeLess> tree_type;

    VolatileTreeBase(std::shared_ptr<VolatileTreeFuncs> funcs);

    tree_type m_tree;

    std::shared_ptr<VolatileTreeFuncs> m_funcs;

    std::vector<maxaligned_char_buf> m_tuplebufs;
};

/*!
 * An in-memory tree index that is stored only in memory. It needs to be
 * rebuilt on every restart.
 */
class VolatileTree: public Index {
public:
    using tree_type = VolatileTreeBase::tree_type;

    class Iterator;

    static void Initialize(const IndexDesc *idxdesc);

    static std::unique_ptr<VolatileTree> Create(
        std::shared_ptr<const IndexDesc> idxdesc);

private:
    VolatileTree(std::shared_ptr<const IndexDesc> idxdesc,
                 std::shared_ptr<VolatileTreeBase> base);

    std::shared_ptr<VolatileTreeBase> m_base;

public:
    virtual ~VolatileTree();

    bool InsertKey(const IndexKey *key, RecordId rid) override;

    bool DeleteKey(const IndexKey *key, RecordId &rid) override;

    std::unique_ptr<Index::Iterator> StartScan(const IndexKey *lower,
                                               bool lower_isstrict,
                                               const IndexKey *upper,
                                               bool upper_isstrict) override;
public:
    typedef typename std::decay<
        decltype(((VolatileTreeBase*)nullptr)->m_tree.begin())>::type
        map_iterator;

    class Iterator: public Index::Iterator {
    public:
        bool Next() override;

        bool IsAtValidItem() override;

        const Record &
        GetCurrentItem() override {
            return m_rec;
        }

        RecordId
        GetCurrentRecordId() override {
            return m_rec.GetRecordId();
        }

        void
        EndScan() override {
        }

    private:
        Iterator(VolatileTree *index, VolatileTree::map_iterator iter,
                 const IndexKey *upper, bool upper_isstrict);

        VolatileTree::map_iterator m_iter;

        Record m_rec;

        UniqueIndexKey m_upper;

        std::vector<Datum> m_upper_data_buffer;

        bool m_is_first;

        bool m_upper_isstrict;

        friend class VolatileTree;
    };

    friend class Iterator;
};

}   // namespace taco

#endif  // INDEX_VOLATILETREE_VOLATILETREE_H
