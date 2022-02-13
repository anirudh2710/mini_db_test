#ifndef STORAGE_FILEMANAGER_H
#define STORAGE_FILEMANAGER_H

#include "tdb.h"

#include <mutex>

#include "utils/Latch.h"

namespace taco {

/*!
 * \p PageHeaderData defines the header of every virtual file data page. Any
 * class other than the FileManager shall **NEVER** modify the PageHeaderData
 * but may inspect its content using the member functions.
 *
 * Impl. note: currently sizeof(PageHeaderData) == 16, but do not assume that
 * any place other than the FileManager.
 */
class PageHeaderData {
public:
    constexpr bool
    IsAllocated() const {
        return m_flags & (FLAG_META_PAGE | FLAG_VFILE_PAGE);
    }

    constexpr bool
    IsMetaPage() const {
        return m_flags & FLAG_META_PAGE;
    }

    constexpr bool
    IsVFilePage() const {
        return m_flags & FLAG_VFILE_PAGE;
    }

    constexpr bool
    IsFMMetaPage() const {
        return (m_flags & (FLAG_META_PAGE | FLAG_VFILE_PAGE)) ==
               FLAG_META_PAGE;
    }

    constexpr bool
    IsVFileMetaPage() const {
        return (m_flags & (FLAG_META_PAGE | FLAG_VFILE_PAGE)) ==
               (FLAG_META_PAGE | FLAG_VFILE_PAGE);
    }

    constexpr bool
    IsVFileDataPage() const {
        return (m_flags & (FLAG_META_PAGE | FLAG_VFILE_PAGE)) ==
               FLAG_VFILE_PAGE;
    }

    /*!
     * Returns the next page number.
     *
     * You may ignore any concurrency control related description.
     */
    PageNumber
    GetNextPageNumber() const {
        return m_next_pid.load(memory_order_acquire);
    }

    /*!
     * Returns the previous page number.
     *
     * You may ignore any concurrency control related description.
     */
    PageNumber
    GetPrevPageNumber() const {
        return m_prev_pid.load(memory_order_relaxed);
    }

    constexpr FileId
    GetFileId() const {
        return m_fid;
    }


private:
    uint16_t            m_flags;
    uint16_t            m_reserved;
    FileId              m_fid;
    atomic<PageNumber>  m_prev_pid;
    atomic<PageNumber>  m_next_pid;

    static constexpr uint16_t FLAG_META_PAGE = 0x1;
    static constexpr uint16_t FLAG_VFILE_PAGE = 0x2;

    friend class FileManager;
    friend class File;
};

// We require PageHeaderData to be in standard-layout and trivial (for it's
// written to a disk page as binary), i.e., a POD prior to C++20.
static_assert(std::is_standard_layout<PageHeaderData>::value &&
              std::is_trivial<PageHeaderData>::value);

constexpr FileId WAL_FILEID_MASK = ((FileId) 1) << 31;
constexpr FileId TMP_FILEID_MASK = ((FileId) 1) << 30;
constexpr int FileIdBits = 19;
constexpr FileId MinRegularFileId = 1;
constexpr FileId MaxRegularFileId = (((FileId) 1) << (FileIdBits + 1)) - 1;
constexpr FileId INVALID_FID = 0;


constexpr FileId NEW_REGULAR_FID = INVALID_FID;
constexpr FileId NEW_TMP_FID = TMP_FILEID_MASK;

}   // namespace taco

#endif      // STORAGE_FILEMANAGER_H
