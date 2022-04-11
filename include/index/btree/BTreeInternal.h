#ifndef INDEX_BTREE_BTREEINTERNAL_H
#define INDEX_BTREE_BTREEINTERNAL_H

#include "tdb.h"

#include "index/btree/BTree.h"
#include "storage/Record.h"
#include "storage/VarlenDataPage.h"

namespace taco {

/*!
 * This is the record number that is guaranteed to be larger than any valid
 * record ID.
 */
static const RecordId INFINITY_RECORDID = { RESERVED_PID, MaxSlotId };

/*!
 * The B-tree meta page.
 */
struct BTreeMetaPageData {
    PageHeaderData  m_ph;

    /*!
     * The page number of the B-Tree root page. This should never be
     * INVALID_PID even if the tree is empty.
     */
    PageNumber      m_root_pid;

    /*!
     * Initializes the B-tree meta page.
     */
    static void Initialize(char *btmeta_buf, PageNumber root_pid);
};

constexpr uint16_t BTREE_PAGE_ISROOT = 1u;
constexpr uint16_t BTREE_PAGE_ISLEAF = 2u;

/*!
 * The B-Tree page header for both internal pages and the leaf pages.
 */
struct BTreePageHeaderData {

    /*!
     * Flags, see BTREE_PAGE_xxx above.
     */
    uint16_t        m_flags;

    /*!
     * The total length of index records on this page. This is used for
     * estimating when the page is half full.
     */
    FieldOffset     m_totrlen;

    /*!
     * The previous page's page number on this level, or INVALID_PID if
     * this page is the first page on this level.
     */
    PageNumber      m_prev_pid;

    /*!
     * The next page's page number on this level, or INVALID_PID if this
     * page is the last page on this level.
     */
    PageNumber      m_next_pid;

    /*!
     * Currently unused.
     */
    uint32_t        m_reserved;

    constexpr bool
    IsRootPage() const {
        return m_flags & BTREE_PAGE_ISROOT;
    }

    constexpr bool
    IsLeafPage() const {
        return m_flags & BTREE_PAGE_ISLEAF;
    }

    /*!
     * Initializes the B-tree internal/leaf page as a data page, and then
     * initializes the BTreePageHeaderData (as its user data area).
     */
    static void Initialize(char *pagebuf,
                           uint16_t flags,
                           PageNumber prev_pid,
                           PageNumber next_pid);
};

constexpr size_t BTreePageHeaderSize = sizeof(BTreePageHeaderData);

/*!
 * The header of a B-Tree internal page record.
 */
struct BTreeInternalRecordHeaderData {
    /*!
     * The page number of the page this internal record points to.
     */
    PageNumber      m_child_pid;

    /*!
     * Currently unused.
     */
    uint32_t        m_reserved;

    /*!
     * The record ID for making keys distinct.
     */
    RecordId        m_heap_recid;

    char*
    GetPayload() const {
        return const_cast<char*>(reinterpret_cast<const char*>(this)) +
            MAXALIGN(sizeof(BTreeInternalRecordHeaderData));
    }
};

constexpr size_t BTreeInternalRecordHeaderSize =
    MAXALIGN(sizeof(BTreeInternalRecordHeaderData));

/*!
 * The header of a B-Tree leaf page record.
 */
struct BTreeLeafRecordHeaderData {
    /*!
     * The record ID of the record this leaf record points to.
     */
    RecordId        m_recid;

    char*
    GetPayload() const {
        return const_cast<char*>(reinterpret_cast<const char*>(this)) +
            MAXALIGN(sizeof(BTreeLeafRecordHeaderData));
    }
};

constexpr size_t BTreeLeafRecordHeaderSize =
    MAXALIGN(sizeof(BTreeLeafRecordHeaderData));

static inline char*
BTreeRecordGetPayload(char* recbuf, bool isleaf) {
    if (isleaf) {
        return ((BTreeLeafRecordHeaderData*) recbuf)->GetPayload();
    } else {
        return ((BTreeInternalRecordHeaderData*) recbuf)->GetPayload();
    }
}

static inline const char*
BTreeRecordGetPayload(const char* recbuf, bool isleaf) {
    return BTreeRecordGetPayload(const_cast<char*>(recbuf), isleaf);
}

static inline const RecordId&
BTreeRecordGetHeapRecordId(const char *recbuf, bool isleaf) {
    if (isleaf) {
        return ((const BTreeLeafRecordHeaderData*) recbuf)->m_recid;
    } else {
        return ((const BTreeInternalRecordHeaderData*) recbuf)->m_heap_recid;
    }
}

/*!
 * Returns the page usage of a B-Tree page given its number of records
 * and its total length of all the records.
 */
static inline FieldOffset
BTreeComputePageUsage(SlotId num_recs, FieldOffset totrlen) {
    return ((FieldOffset) PAGE_SIZE) -
        VarlenDataPage::ComputeFreeSpace(BTreePageHeaderSize,
            num_recs, totrlen);
}

// This should be enough for fitting at least 3 internal records on an internal
// pages, or 2 leaf records on a leaf page, for 4KB pages.
//
// XXX This is not computed from VarlenDataPage impl because it's inconvenient
// to mark VarlenDataPage::ComputeFreeSpace() as constexpr in C++11 where
// constexpr function can only have one return statement (plus a few
// declarations).
// This should be computed as VarlenDataPage::BTreeComputePageUsage(3, 3 *
// BTreeInternalRecordHeaderData::HeaderSize) / 2, if we switch to C++14 or
// later C++ standard. (Note we do not need to store the first key on an
// internal page, hence / 2).
constexpr FieldOffset BTREE_MAX_RECORD_SIZE = 2000;

constexpr FieldOffset BTREE_MIN_PAGE_USAGE = (FieldOffset) (PAGE_SIZE * 0.4);

}   // namespace taco

#endif      // INDEX_BTREE_BTREEINTERNAL_H
