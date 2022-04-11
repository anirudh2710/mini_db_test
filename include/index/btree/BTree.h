#ifndef INDEX_BTREE_BTREE_H
#define INDEX_BTREE_BTREE_H

#include "tdb.h"

#include "catalog/IndexDesc.h"
#include "index/BulkLoadIterator.h"
#include "index/Index.h"
#include "index/IndexKey.h"
#include "storage/Record.h"
#include "storage/FileManager.h"
#include "storage/VarlenDatapage.h"

namespace taco {

/*!
 * Each PathItem is a (PageNumber, SlotId) pair of an B-tree internal record.
 * It is used for storing the page number and slot id of each B-tree internal
 * record whose child page pid you followed when you call BTree::FindLeafPage()
 * to locate a leaf page for a (key, recid) pair.
 */
typedef RecordId PathItem;

/*!
 * A B-tree stored in the persistent files. Logically the tree is a map of key,
 * record ID pairs that has one-to-one correspondence to the heap file records
 * in some table. The key is a record payload that only consists of the key
 * columns as defined by the key schema, extracted from a heap tuple, and the
 * record ID is that of the heap tuple in the table (which you may think of as
 * a pointer into the table).
 *
 * Internally, B-tree considers each (key, recid) pair as the key as a whole.
 * Meaning, two pairs with the same key but different record ids are considered
 * different by the B-tree. Duplicate (key, recid) pair must not appear at the
 * same time in a B-tree index. This ensures all the pairs are unique within
 * the index and we can uniquely identify each indexed item in the tree (which
 * makes updating and iteration meaningful).
 *
 * The tree consists of several levels of internal pages and a level of leaf
 * pages. Each page is managed as a VarlenDataPage. The pages on the same level
 * are linked as a double-linked list through the their page headers
 * `BTreePageHeaderData`. Note that this header is stored in the user data area
 * of the VarlenDataPage rather than the beginning of the page.
 *
 * For the leaf pages, each (key, recid) pair is stored as a single record,
 * which consists of a header `BTreeLeafRecordHeaderData` at the beginning and
 * the actual key payload immediately following the header. We store the recid
 * in the header instead of the payload. All the records are sorted by key and
 * record id lexicographically (see BTreeTupleCompare() for details).
 *
 * For internal pages, it stores the the child page links and separator keys as
 * internal records. Each internal records consists of a header
 * `BTreeInternalRecordHeaderData` at the beginning and the separator key's
 * payload immediately follows, with the exception of the very first internal
 * record on an internal page (see below). Note that, we store the child page
 * number and the heap record id in the header rather than in the payload. The
 * first internal record on an internal page only has a header
 * `BTreeInternalPayloadRecordHeaderData` without any payload following that,
 * and its heap record id is undefined (meaning we should never use its value).
 * The first internal record is assumed to be smaller than any other internal
 * record and/or (key, recid) pairs. All the remaining records are sorted by
 * the key and heap record id lexicographically (i.e., its key is negative
 * infinity in all fields). For an internal record with key k_i, heap record id
 * recid_i, and child page number c_i, let k_{i+1}, recid_{i+1} be the key and
 * the heap record id in the internal record after it (if any), the subtree
 * c_i points to should contain and may only contain (key, recid) pairs such
 * that (k_{i}, recid_{i}) <= (key, recid) < (k_{i+1}, recid_{i+1}). This (key,
 * recid) pair range is said to be covered by the subtree.
 *
 */
struct BTree: public Index {
public:
    class Iterator;

    static void Initialize(const IndexDesc *idxdesc);

    static std::unique_ptr<BTree> Create(
        std::shared_ptr<const IndexDesc> idxdesc);

private:
    BTree(std::shared_ptr<const IndexDesc> idxdesc,
          std::unique_ptr<File> file,
          std::vector<FunctionInfo> lt_funcs,
          std::vector<FunctionInfo> eq_funcs);

    std::unique_ptr<File> m_file;

    std::vector<FunctionInfo> m_lt_funcs;

    std::vector<FunctionInfo> m_eq_funcs;

public:
    virtual ~BTree();

    void BulkLoad(BulkLoadIterator &iter) override;

    bool InsertKey(const IndexKey *key, RecordId recid) override;

    bool DeleteKey(const IndexKey *key, RecordId &recid) override;

    std::unique_ptr<Index::Iterator> StartScan(const IndexKey *lower,
                                               bool lower_isstrict,
                                               const IndexKey *upper,
                                               bool upper_isstrict) override;

    /*!
     * Returns whether this tree is empty.
     */
    bool IsEmpty();

    /*!
     * Returns the tree height. It is 1 + the level of the root page.
     */
    uint32_t GetTreeHeight();

    class Iterator: public Index::Iterator {
    public:
        ~Iterator() {
            EndScan();
        }

        bool Next() override;

        bool IsAtValidItem() override;

        const Record& GetCurrentItem() override;

        RecordId GetCurrentRecordId() override;

        void EndScan() override;

    private:
        Iterator(BTree *btree,
                 ScopedBufferId bufid, SlotId one_before_matching_sid,
                 const IndexKey *key, bool upper_isstrict);

        ScopedBufferId m_bufid;

        SlotId m_sid;

        Record m_rec;

        UniqueIndexKey m_upper;

        std::vector<Datum> m_upper_data_buffer;

        bool m_upper_isstrict;

        friend class BTree;
    };

private:
    // in index/btree/BTreeUtils.cpp

    /*!
     * Allocates and initializes a new BTree internal or leaf page. Returns
     * the buffer frame id where the new page is pinned.
     */
    BufferId CreateNewBTreePage(bool isroot,
                                bool isleaf,
                                PageNumber prev_pid,
                                PageNumber next_pid);

    /*!
     * Pins the BTree meta page and returns the buffer ID of the buffer frame
     * where it is pinned.
     */
    BufferId GetBTreeMetaPage();

    /*!
     * Creates a new leaf record with the given \p key and heap \recid in \p
     * recbuf. The record buffer is cleared upon entry and contains the
     * newly created record upon return.
     */
    void CreateLeafRecord(const IndexKey *key,
                          const RecordId &recid,
                          maxaligned_char_buf &recbuf);

    /*!
     * Creates a new internal record with separator key and heap record id from
     * \p child_recbuf, and the child page number \p child_pid in \p recbuf.
     * \p child_isleaf must be true if \p child_recbuf is a leaf record.
     * Conversely, it must be false if \p child_recbuf is an internal record.
     * The record buffer \p recbuf is cleared upon entry and contains the newly
     * created record upon return.
     */
    void CreateInternalRecord(const Record &child_recbuf,
                              PageNumber child_pid,
                              bool child_isleaf,
                              maxaligned_char_buf &recbuf);

    /*!
     * Compares a \p (key, recid) pair to a leaf or internal record in the
     * B-tree. The key is first compared against the key stored in the \p
     * tuplebuf lexicographically by fields. If there's any NULL field, it is
     * considered as smaller than any non-NULL value. If the \p key compares
     * equal to the key stored in \p tuplebuf, it then compares \p recid
     * against the heap record id stored in \p tuplebuf.
     *
     * Returns -1 if \p (key, recid) is smaller than the key and record id pair
     * stored in \p tuplebuf; 0 if they equal; or 1 if the former is larger
     * than the latter.
     *
     * Note that TupleCompare() treats a key that is exactly a prefix of the
     * fields stored in a record payload as equal, whereas BTreeTupleCompare()
     * should consider that key is smaller than the key stored in the record
     * payload. Hence, BTreeTupleCompare() needs to consider this case after
     * calling TupleCompare().
     */
    int BTreeTupleCompare(const IndexKey *key,
                          const RecordId &recid,
                          const char *tuplebuf,
                          bool isleaf) const;

    /*!
     * Uses binary search to find the last slot whose key-recid pair is
     * smaller than or equal to the `(key, recid)` pair. The comparison of the
     * key-recid pair is defined by `BTreeTupleCompare()`, except that the
     * first slot on an internal page is treated as negative infinity (which is
     * smaller than any key).
     *
     * For an internal page, there must be such a slot as the key in first slot
     * on the page is treated as negative infinity (i.e., it is smaller than any
     * key). For a leaf page, there may not be such a slot, in which case it
     * should return INVALID_SID (which is MinSlotId - 1).
     *
     * Note that it is undefined \p find_next_key is true when \p buf points to
     * an internal page.
     */


    SlotId BinarySearchOnPage(char *buf,
                              const IndexKey *key,
                              const RecordId &recid);

    /*!
     * Finds a leaf page from the root such that:
     *
     *  1) for a null \p key, the found leaf page is the left-most page on the
     *  leaf level;
     *
     *  2) for a non-null \p key, the found leaf page whose key range covers
     *  the \p (key, recid) pair (note that there's only one such page on the
     *  leaf level).
     *
     * Returns the buffer id of the buffer frame where the found leaf page is
     * pinned. Note that the returned leaf page is the only page that should be
     * pinned by `FindLeafPage()` upon return.
     *
     * If \p p_search_path is not null, this function stores the record id of
     * all the internal records in `*p_search_path`. Otherwise, it should not
     * dereferences \p p_search_path.
     */
    BufferId FindLeafPage(const IndexKey *key,
                          const RecordId &recid,
                          std::vector<PathItem> *p_search_path);

    /*!
     * Finds a leaf page from a page pinned in buffer frame \p bufid whose key
     * range covers the \p (key, recid) pair such that
     *
     *  1) for a null \p key, the found leaf page is the left-most page on the
     *  leaf level;
     *
     *  2) for a non-null \p key, the found leaf page whose key range covers
     *  the \p (key, recid) pair (note that there's only one such page on the
     *  leaf level).
     *
     * Returns the buffer id of the buffer frame where the found leaf page is
     * pinned, with its parent page unpinned.
     *
     * If \p p_search_path is not null, this function stores the record id of
     * all the internal records in `*p_search_path`. Otherwise, it should not
     * dereferences \p p_search_path.
     *
     * It is undefined if the initial page pinned in \p bufid does not have a
     * key range that covers the \p (key, recid) pair.
     */
    BufferId FindLeafPage(BufferId bufid,
                          const IndexKey *key,
                          const RecordId &recid,
                          std::vector<PathItem> *p_search_path);

    /*!
     * Finds the slot id where the inserting \p (key, recid) pair at that slot
     * will keep the page's key and record id pairs unique and sorted.
     *
     * If this a unique index, it should further ensure that no key (instead of
     * pairs) on this page is equal to the insertion \p key. For this purpose,
     * NULL values do not compare equal.
     */
    SlotId FindInsertionSlotIdOnLeaf(const IndexKey *key,
                                     const RecordId &recid,
                                     BufferId bufid);

    /*!
     * Inserts a record in \p recbuf onto a leaf page already pinned in the
     * buffer frame \p bufid whose key range covers the \p (key, recid) pair,
     * at slot \p insertion_sid. \p search_path contains a stack of record ids
     * of the internal records when we initially searched for the insertion
     * point.
     *
     * Note that, InsertRecordOnPage() may need to split the page that it is
     * inserting into, which may cause split of its parent or ancestor pages
     * recursively. Upon return, the page it is inserting into should be
     * unpinned and any additional pin obtained during the recursive split must
     * also be dropped.
     */
    void InsertRecordOnPage(BufferId bufid,
                            SlotId insertion_sid,
                            maxaligned_char_buf &&recbuf,
                            std::vector<PathItem> &&search_path);

    /*!
     * Creates a new root page and updates the B-Tree meta page. The old root
     * page (which has already been split) is initially pinned in \p bufid and
     * the record buffer \p recbuf contains an internal record that points to
     * the right sibling of the old root page. Upon return the old root page is
     * unpinned.
     */
    void CreateNewRoot(BufferId bufid,
                       maxaligned_char_buf &&recbuf);

    /*!
     * Splits a page pinned in \p bufid that is too full to insert an
     * additional record in \p recbuf at slot \p insertion_sid, into two
     * sibling pages such that
     *
     *  1) the two pages have exactly all the records on the original page plus
     *  the new record in \p recbuf;
     *
     *  2) all the (key, recid) pairs are still sorted and the old page is the
     *  left sibling page;
     *
     *  3) and the choice of split point minimizes the difference between the
     *  page usages of the two pages.
     *
     * Returns an internal record that points to the new (right) page and its
     * key and heap record id are correctly set such that all key and record id
     * pairs on the old (left) page are strictly smaller than the internal
     * record, and all key and record id pairs on the new (right) page are
     * larger than or equal to the internal record.
     *
     * Note that the buffer pin on the left page should be kept, while the pin
     * on the right page should be dropped, upon successful return.
     */
    maxaligned_char_buf SplitPage(BufferId bufid,
                                  SlotId insertion_sid,
                                  maxaligned_char_buf &&recbuf);

    /*!
     * Finds the slot to delete for the `(key, recid)` pair. Upon entry, \p the
     * page pinned in buffer frame \p bufid should be 1) either the page whose
     * key range covers `(key, recid)` if recid is valid; 2) or a page that is
     * the left-most page on the leaf level whose key range possibly overlaps
     * with some pair with the given \p key if recid is invalid.
     *
     * This function may need to move to the right page of the original page to
     * find the slot to delete in some cases, in which case, the search path at
     * the lowest internal page level may also need to be updated. However, it
     * should simply add 1 to the slot id. CheckMinPageUsage() is be
     * responsible for fetching the sibling parent page if necessary. The bufid
     * should be updated if this function moves to the right page and it should
     * properly unpin the previous leaf page. It is also responsible for
     * unpinning the page should there be any uncaught error.
     *
     * Returns the slot id of the leaf record to be deleted if it has a
     * matching key when recid is invalid, or (key, recid) pair when recid is
     * valid. Otherwise, returns INVALID_SID.
     */
    SlotId FindDeletionSlotIdOnLeaf(const IndexKey *key,
                                    const RecordId &recid,
                                    BufferId &bufid,
                                    std::vector<PathItem> &search_path);

    /*!
     * Deletes a slot from a page. If slot \p sid is the last slot on the root
     * page, it should update the header flags to indicate this becomes a leaf
     * page as well.
     */
    void DeleteSlotOnPage(BufferId bufid, SlotId sid);

    /*!
     * Checks if a page pinned in buffer frame \p bufid needs to be merged or
     * rebalanced with a sibling page sharing the same parent. \p search_path
     * is the record ids of the internal records that leads to the leaf page
     * where we deleted a tuple (with the exception of the last record id in
     * search path which can have a slot id that is 1 larger than the maximum
     * on the page, see FindDeletionSlotIdOnLeaf()).
     *
     * The buffer and any buffer additionally obtained in this function should
     * be unpinned upon return.
     */
    void HandleMinPageUsage(BufferId bufid,
                            std::vector<PathItem> &&search_path);

    /*!
     * Try to merge or reblanace two sibling pages. Returns true if any of
     * these succeed. Upon successful return, all but the parent buffer pin are
     * dropped. If the function returns false, it does not unpin any page.
     */
    bool TryMergeOrRebalancePages(BufferId lbufid,
                                  BufferId rbufid,
                                  BufferId pbufid,
                                  SlotId lsid);


    /*!
     * Merges two sibling pages if there're enough space. Returns true if it
     * successfully merged the two pages, or false if it cannot merge the two
     * pages. Unpon successful return, it should free the right page and
     * unpin the left page. Otherwise, it should leave all three pages pinned.
     *
     * Note that if MergePages() determines that it cannot merge the pages, it
     * should not physically modify any of the three pages (even if the page
     * still has the same set of records logically after such modification) and
     * should not mark these buffered pages as dirty.
     */
    bool MergePages(BufferId lbufid,
                    BufferId rbufid,
                    BufferId pbufid,
                    SlotId lsid);

    /*!
     * Rebalances two sibling pages if it is possible and it should choose a
     * way that minimizes the differences between the page usages of the two
     * pages. Returns true if it is successful, or false if it cannot rebalance
     * the two pages. Upon successful return, it should unpin both the left and
     * the right pages. Otherwise, it should leave all three pages pinned.
     *
     * Note that if RebalancePages() determines that it cannot rebalance the
     * pages, it should not physically modify any of the three pages (even if
     * the page still has the same set of records logically after such
     * modification) and should not mark these buffered pages as dirty.
     */
    bool RebalancePages(BufferId lbufid,
                        BufferId rbufid,
                        BufferId pbufid,
                        SlotId lsid);

    // declare the gtest fixture as a friend class
    friend class TestBTree;
    friend class Iterator;
};

}   // namespace taco

#endif      // INDEX_BTREE_BTREE_H
