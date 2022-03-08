#ifndef STORAGE_FILEMANAGER_PRIVATE_H
#define STORAGE_FILEMANAGER_PRIVATE_H

#include "FileManager.h"

namespace taco {

constexpr uint64_t DBFILE_MAGIC = 0xdefabc1221cbafedul;
constexpr PageNumber PENDING_CREATION_PID = RESERVED_PID;

/*!
 * The FM meta page is always the first page in the file. It is not visible
 * outside the FileManager anyway, so it is safe to reuse INVALID_PID as its
 * page ID here.
 */
constexpr PageNumber FM_META_PID = INVALID_PID;

//! Size of allocation in pages.
constexpr PageNumber PageGroupSize = 64;

// FormatMeta() assumes there are at least 3 pages in the first page group.
static_assert(PageGroupSize >= 3);

//! Maximum size of a single data file.
constexpr size_t MaxFileSize = (size_t) 64 * 1024 * 1024 * 1024;

//! Maximum number of pages in a single data file
constexpr PageNumber MaxNumPagesPerFile =  MaxFileSize / PAGE_SIZE;

//! Maximum number of data files
constexpr uint64_t MaxNumDataFiles =
    ((((uint64_t) MaxPageNumber) + 1) + MaxNumPagesPerFile - 1) /
    MaxNumPagesPerFile;

// There're many reasons why we don't want too many physical files:
// 1. We don't want an excessive amount of files in a single directory.
// 2. We preallocate the main data file vector, and we don't want that to be
// huge.
static_assert(MaxNumDataFiles <= 4096);

/*!
 * This is a truncated and private version of PageHeaderData. Nothing other
 * than the flags are needed in a meta file so we can use this to save space
 * in the file directories. This is 4-bytes exactly and that means we can store
 * 1023 PageNumber in a page directory, following the header.
 *
 * Note: this must be kept in sync with PageHeaderData as a prefix of that.
 */
struct MetaPageHeaderData {
    uint16_t m_flags;
    uint16_t m_reserved;
};
static_assert(sizeof(MetaPageHeaderData) == 4);

static_assert(FileIdBits <= 19, "we support at most 2^19 files at this time");

constexpr FileId
FileIdGetDir1Offset(FileId fid) {
    return fid / 1023;
}

constexpr FileId
FileIdGetDir2Offset(FileId fid) {
    return fid % 1023;
}

constexpr uint64_t
PageNumberGetDataFileId(PageNumber pid) {
    return pid / MaxNumPagesPerFile;
}

constexpr PageNumber
PageNumberGetDataFilePageId(PageNumber pid) {
    return pid % MaxNumPagesPerFile;
}

constexpr PageNumber
DataFileIdAndPageIdGetPageNumber(uint64_t fsfileid, PageNumber fspid) {
    return fsfileid * MaxNumPagesPerFile + fspid;
}

/*!
 * The length of the file name of main data files. Each name is a hexadecimal
 * nummber of this length. That means we can have at most 4096 data files in a
 * single directory. This is aribitrarily chosen, but should be more than
 * enough.
 */
#define DataFileNameLength 3
static_assert(MaxNumDataFiles <= 1 << (DataFileNameLength * 4));

struct FreePageList {
    atomic<PageNumber>      m_next_fp_in_last_pg;
    atomic<PageNumber>      m_last_pg;

    // list head of the free page list
    atomic<PageNumber>      m_fp_list;
};

struct FMMetaPageData {
    MetaPageHeaderData  m_ph;
    uint64_t        m_magic;
    PageNumber      m_fdir1_pid;
    FreePageList    m_fpl;

    /*!
     * \p m_last_free_fid serves as a hint to locate the last allocated free
     * file id. Usually, the next free file ID should be just next to it.
     * Stale value of \p m_last_free_fid should not render the reader's
     * implementation to be incorrect, rather it should only affect the
     * efficiency.
     */
    atomic<FileId>  m_last_free_fid;
};

struct FileDirectoryData {
    MetaPageHeaderData  m_ph;
    atomic<PageNumber>  m_pid[1023];
};
static_assert(sizeof(FileDirectoryData) <= PAGE_SIZE);

struct RegularFileMetaPageData {
    PageHeaderData          m_ph;

    /*!
     * The first page ID of this file. The first page of a regular file never
     * changes and may never be deallocated.
     */
    PageNumber              m_first_pid;

    /*!
     * The last page ID of this file. This is merely a hint, in that this may
     * not be the latest tail of the list when there are concurrent page
     * allocations. The user of \p m_last_pid must be prepared to move to next
     * until it finds the actual last page.
     */
    atomic<PageNumber>      m_last_pid;

    //! The free page list of this file.
    FreePageList            m_fpl;
};

}

#endif      // STORAGE_FILEMANAGER_PRIVATE_H
