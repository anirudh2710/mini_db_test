#ifndef INDEX_INDEX_H
#define INDEX_INDEX_H

#include "tdb.h"

#include "index/BulkLoadIterator.h"
#include "index/IndexKey.h"
#include "index/idxtyps.h"
#include "catalog/IndexDesc.h"
#include "storage/Record.h"

namespace taco {

/*!
 * An interface class for index implementations. In Taco-DB, we only implement
 * secondary indexes, where each indexed item consists of a key of some
 * specified schema, and a record ID value.
 */
class Index {
public:
    class Iterator;

    /*!
     * Initializes an index described by an index descriptor. Currently we
     * support a in-memory volatile tree index (based on std::multimap), and a
     * B-tree index.  This function will call the `Initialize()` function of
     * the specified index type (`idxdesc->GetIndexEntry()->idxtyp()`). Except
     * for the in-memory volatile tree index, the index should be stored
     * entirely in the file as specified by the index file id
     * (`idxdesc->GetIndexDesc()->idxfid()`).
     *
     * Different from the Table class, the index descriptor passed to this
     * function is always complete (even during catalog initialization). Hence,
     * we can use any information stored in the index descriptor.
     */
    static void Initialize(const IndexDesc *idxdesc);

    /*!
     * Creates an index object over an index file described by the index
     * descriptor, which has already been initialized.
     */
    static std::unique_ptr<Index> Create(
        std::shared_ptr<const IndexDesc> idxdesc);

    /*!
     * Index object destructor.
     */
    virtual ~Index();

    /*!
     * Returns the index descriptor of this index.
     */
    const IndexDesc*
    GetIndexDesc() const {
        return m_idxdesc.get();
    }

    /*!
     * Returns the key schema of this index.
     */
    const Schema*
    GetKeySchema() const {
        return m_idxdesc->GetKeySchema();
    }

    /*!
     * Loads all the (key, record id) pairs provided by the iterator \p iter
     * into an empty index. It is undefined if the index is not empty, and the
     * specific index implementation is allowed to (but not required to) throw
     * an error when called on an non-empty index.
     *
     * Upon successful return, the iterator \iter is ended by calling
     * `iter->EndScan()`.
     *
     * The default implementation calls the InsertKey() function over all the
     * pairs returned by \p iter. The specific index implementation should
     * override it with a more efficient way if there's any.
     */
    virtual void BulkLoad(BulkLoadIterator &iter);

    /*!
     * Inserts the (key, rid) pair into the index.
     *
     * Returns true if the insertion succeeds. Returns false if the index was
     * declared as a unique index and a duplicate key is found, or for a
     * non-unique index, the (key, rid) pair already exists in the index.
     */
    virtual bool InsertKey(const IndexKey *key, RecordId rid) = 0;

    /*!
     * Inserts the (key, rid) extracted from the table record into the index.
     * All fields in the \p rec must be valid. The table schema \p tabschema
     * (not index key schema) may be null and this function will look for it
     * from the catalog. The caller may also provide a non-null table schema
     * to avoid that lookup.
     *
     * See InsertKey() for specification.
     */
    bool InsertRecord(const Record& rec, const Schema *tabschema = nullptr);

    /*!
     * Deletes any matching \p key (if \p rid is invalid), or the matching (\p
     * key, \p rid) pair (if rid is valid) from the index.
     *
     * Returns true if the deletion succeeds. Upon successful return, update \p
     * rid to the deleted indexed item's record id. Returns false if the \p key
     * (if \p rid is invalid) or (\p key, \p rid) pair (if \p rid is valid) is
     * not found in the index, in which case, rid is set to invalid.
     */
    virtual bool DeleteKey(const IndexKey *key, RecordId &rid) = 0;

    /*!
     * Deletes the (key, rid) extracted from the table record from the index.
     * The data in \p rec must be valid but `rec.GetRecordId()` may be invalid.
     * The table schema \p tabschema (not index key schema) may be null and
     * this function will look for it from the catalog. The caller may also
     * provide a non-null table schema to avoid that lookup.

     * See DeleteKey() for specification.
     */
    bool DeleteRecord(Record& rec, const Schema *tabschema = nullptr);

    /*!
     * Returns a forward-iterator for all the indexed items in the specified
     * range defined by the arguments. A non-null \p lower defines the lower
     * bound and the iterator should return the indexed items with keys that
     * are >= (\p lower_isstrict == false), or > (\p lower_isstrict == true) \p
     * lower.  A non-null \p upper defines the upper bound, and the iterator
     * should not return the indexed items whose keys are >= (\p upper_isstrict
     * == true), or > (\p upper_isstrict = false). If \p lower (and/or \p
     * upper) is null, there is no lower (upper resp.) bound for the returned
     * indexed items, in which case, \p lower_isstrict and \p upper_isstrict
     * are ignored.
     *
     * \p lower and \p upper are allowed to have fewer keys than the index
     * schema does, the comparison between the key of an indexed item and \p
     * lower or \p upper should be done only on the prefix of
     * `lower->GetNumKeys()` or `upper->GetNumKeys()` keys.
     *
     * Not all combinations of arguments can be supported by the underlying
     * index and it should throw an error if the underlying index cannot
     * reasonably support that. Tree indexes (e.g., B-tree) should support
     * range search and prefix searches, while hash indexes may just support
     * equality searches.
     */
    virtual std::unique_ptr<Iterator> StartScan(const IndexKey *lower,
                                                bool lower_isstrict,
                                                const IndexKey *upper,
                                                bool upper_isstrict) = 0;

protected:
    Index(std::shared_ptr<const IndexDesc> idxdesc);

private:
    /*!
     * Extract the keys from \p recbuf into \p m_key. The fetched datum
     * are stored in \p data. If \p tabschema is non-null, it will be used
     * to get the fields. Otherwise, we will find one from the catalog cache.
     *
     * \p data must empty upon entry.
     */
    void PrepareKey(std::vector<Datum> &data,
                    const char *recbuf,
                    const Schema *tabschema);

    std::shared_ptr<const IndexDesc> m_idxdesc;

    /*!
     * A buffer enough for holding an Indexkey for the number of fields in
     * the key schema. This might be nullptr and will only be allocated in
     * the first call to InsertRecord() or DeleteRecord().
     */
    unique_malloced_ptr m_key;
public:
    /*!
     * A forward-iterator for the items in the index. To move to the next item,
     * one should call `Next()` and check if it returns `true`. It cannot go
     * back to a previously returned item once another `Next()` is called.
     */
    class Iterator {
    public:
        Iterator() = default;
        Iterator(const Iterator&) = delete;
        Iterator& operator=(const Iterator&) = delete;

        /*!
         * Default move constructor.
         */
        Iterator(Iterator&& it) = default;

        /*!
         * Default move assignment.
         */
        Iterator &operator=(Iterator &&it) = default;

        virtual ~Iterator() = default;

        /*!
         * Returns the underlying index.
         */
        constexpr const Index*
        GetIndex() const {
            return m_index;
        }

        /*!
         * Moves the iterator to the next indexed item if any. Returns true
         * if there's one, or false if there's not.
         */
        virtual bool Next() = 0;

        /*!
         * Returns whether the iterator is currently at a valid indexed item.
         */
        virtual bool IsAtValidItem() = 0;

        /*!
         * Returns the current indexed item the iterator is currently at, where
         * the `GetData()` and `GetLength()` pair describes a valid buffer that
         * contains the key, and `GetRecordId()` is set to the record ID of the
         * heap record (not the index record!).
         */
        virtual const Record &GetCurrentItem() = 0;

        /*!
         * Returns the record id of the indexed item the iterator is currently
         * at.
         */
        virtual RecordId GetCurrentRecordId() = 0;

        /*!
         * Ends the index scan.
         */
        virtual void EndScan() = 0;

    protected:
        Iterator(Index *index):
            m_index(index) {}

        /*!
         * Static casts the `m_index` pointer to T* type. This should only be
         * used when the caller is sure about the index type, e.g., a subclass
         * of Iterator that invoked the Index(Index*) constructor.
         */
        template<class T>
        constexpr T*
        GetIndexAs() const {
            return (T*) m_index;
        }

    private:
        Index *m_index;
    };
};

}   // namespace taco

#endif      // INDEX_INDEX_H
