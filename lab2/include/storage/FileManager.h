#ifndef STORAGE_FILEMANAGER_H
#define STORAGE_FILEMANAGER_H

#include "tdb.h"

#include <mutex>

#include "storage/BufferManager.h"
#include "storage/FSFile.h"

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

class FMMetaPageData;
class File;
class FreePageList;
class FileManager;

}   // namespace taco

// no namepsace for unmangled names, use taco_ prefix instead.
// The functions may be wrapped with --wrap flag to GNU ld in tests.
extern "C" {

void taco_fileman_readpage_impl(::taco::FileManager *fileman,
                                ::taco::PageNumber pid,
                                char *buf);

void __wrap_taco_fileman_readpage_impl(::taco::FileManager *fileman,
                                       ::taco::PageNumber pid,
                                       char *buf);

void __real_taco_fileman_readpage_impl(::taco::FileManager *fileman,
                                       ::taco::PageNumber pid,
                                       char *buf);

void taco_fileman_writepage_impl(::taco::FileManager *fileman,
                                 ::taco::PageNumber pid,
                                 const char *buf);

void __wrap_taco_fileman_writepage_impl(::taco::FileManager *fileman,
                                        ::taco::PageNumber pid,
                                        const char *buf);

void __real_taco_fileman_writepage_impl(::taco::FileManager *fileman,
                                        ::taco::PageNumber pid,
                                        const char *buf);
}
namespace taco {
/*!
 * \p FileManager exposes a virtual file interface based on \p FSFile.
 *
 * There are three types of files
 *  -Regular file (main data file). These have persistent file ID numbers in
 *  the range of [\p MinRegularFileId, \p MaxRegularFileId]. A regular file is
 *  an non-empty unordered list of pages. The first page of a regular file
 *  never changes once created or deallocated unless the file gets deleted. In
 *  contrast to the common File abstraction found in a file system, the page
 *  numbers are not consecutive nor monotonic (e.g., a file may have a list of
 *  pages with PID = [5, 1, 2, 4, 3]). As a result, one may only enumerate the
 *  pages by iteratively call `PageHeaderData::GetNextPageNumber()' (or
 *  `PageHeaderdata::GetPrevPageNumber()'), or implement their own
 *  indexing/ordering scheme.
 *
 *  -Temporary files. These are files to be deleted upon work done or program
 *  exit (though the program may fail to clear these files due to errors). They
 *  are assigned unique file IDs at runtime with bit TMP_FILEID_MASK set but
 *  the file ID may never be INVALID_FID (0) after zeroing its mask bit.
 *
 *  -Write-Ahead Logging file. These have persistent file ID numbers with
 *  WAL_FILEID_MASK set. Assignment policy TBD.
 *
 */
class FileManager {
public:
    FileManager();

    ~FileManager();

    void CloseAll();

    /*!
     * Initializes the FileManager with a specified data directory path \p
     * datadir. Any member function except the destructor and CloseAll() may
     * not be called before Init() is successfully called.
     *
     * If \p create == true, \p Init() will try to create and format a new
     * directory at the specified path. In this case, the initial size of the
     * first main data file will be \p init_size after it is rounded to the
     * smallest value that is multiples of the page allocation group size and
     * fits the necessary metadata.
     *
     * If \p create == true, it tries to initializes itself by reading data
     * files found at the specified path. It is an error if the metadata could
     * not be read or is obviously corrupt. \p init_size is ignored in this
     * case.
     *
     * This function either succeed or throws a fatal error.
     */
    void Init(std::string datadir, size_t init_size, bool create);

    /*!
     * Creates or opens a virtual file. If \p fid is
     *  - NEW_REGULAR_FID, it creates a new regular file.
     *
     *  - a valid regular file ID. it opens the file if it exists. It is an
     *  error if the file does not exist.
     *
     *  - NEW_TMP_FID, it creates a new temporary file.
     *
     *  - any valid temporary file ID, it opens the temporary file. It is an
     *  error if the file does not exist.
     *
     *  - any valid WAL file ID, it creates or opens the WAL file.
     *
     * This function never returns a null pointer but may log an error or a
     * fatal error if it fails.
     */
    std::unique_ptr<File> Open(FileId fid);

    /*!
     * Reading a page \p pid in the main data file into \p buf. It is a fatal
     * error if the specified page does not exist. This is mainly for
     * BufferManager use and all others should use File::ReadPage().
     *
     * Note: reading from WAL or TMP files must go through File::ReadPage().
     */
    inline void
    ReadPage(PageNumber pid, char *buf) {
        taco_fileman_readpage_impl(this, pid, buf);
    }

    /*!
     * Writing a buffered page in \p buf to the page \p pid in the main data
     * file. It is a fatal error if the specified page does not exist.  This is
     * mainly for BufferManager use and all others should use
     * File::WritePage().
     *
     * Note: writing to WAL or TMP files should go through File::WritePage().
     */
    inline void
    WritePage(PageNumber pid, const char *buf) {
        taco_fileman_writepage_impl(this, pid, buf);
    }

    /*!
     * Flushes all buffered writes to the main data file to disk.
     *
     * Note: to flush a WAL or TMP file, use File::Flush().
     */
    void Flush();

    /*!
     * Returns the path to the specified main data file.
     */
    std::string GetMainDataFilePath(uint64_t DataFileId) const;

private:
    void FormatMeta();

    void LoadMeta();

    std::unique_ptr<File> CreateTmpFile();

    std::unique_ptr<File> OpenTmpFile(FileId fid);

    std::unique_ptr<File> CreateRegularFile();

    std::unique_ptr<File> OpenRegularFile(FileId fid);

    FileId FindFreeFileId();

    void AllocateL2FileDirectory(BufferId fdir1_bufid, size_t fdir1_off);

    PageNumber AllocatePageGroup();

    void InitFreePageList(FreePageList *fpl, PageNumber last_pg);

    PageNumber GetNextFreePageNumber(FreePageList *fpl, BufferId meta_bufid);

    void AddToFreePageList(FreePageList *fpl,
                           BufferId pg_bufid,
                           BufferId meta_bufid);

    std::mutex                                      m_fsfile_extension_latch;

    std::mutex                                      m_fdir1_latch;

    std::mutex                                      m_meta_latch;

    std::string                                     m_datadir;

    std::string                                     m_maindir;

    std::string                                     m_tmpdir;

    std::string                                     m_waldir;

    std::vector<std::unique_ptr<FSFile>>            m_mainfiles;

    FMMetaPageData                                  *m_meta;

    friend class File;

    friend void ::taco_fileman_readpage_impl(::taco::FileManager*,
                                             ::taco::PageNumber,
                                             char*);
    friend void ::taco_fileman_writepage_impl(::taco::FileManager*,
                                              ::taco::PageNumber,
                                              const char*);
    friend void ::__wrap_taco_fileman_readpage_impl(::taco::FileManager*,
                                                    ::taco::PageNumber,
                                                    char*);
    friend void ::__wrap_taco_fileman_writepage_impl(::taco::FileManager*,
                                                     ::taco::PageNumber,
                                                     const char*);
};

/*!
 * Represents an open virtual file managed by the FileManager.
 */
class File {
public:
    /*!
     * Destructor. Automatically calls Close().
     */
    ~File();

    /*!
     * Closes the file. It never fails.
     */
    void Close();

    constexpr bool
    IsOpen() const noexcept {
        return m_fileid != INVALID_FID;
    }

    void ReadPage(PageNumber pid, char *buf);

    void WritePage(PageNumber pid, const char *buf);

    /*!
     * Allocates a new page in the file and returns it page number. The new
     * page is not pinned in the buffer maanger.
     */
    PageNumber
    AllocatePage() {
        return (PageNumber) AllocatePageImpl(false, LatchMode::SH);
    }

    /*!
     * Allocates a new page in the file and returns a valid buffer ID where
     * is pinned. You may ignore the latch \p mode parameter.
     *
     * It is an error if this overload is called on a TMP or WAL file.
     */
    BufferId
    AllocatePage(LatchMode mode) {
        return (BufferId) AllocatePageImpl(true, mode);
    }

    /*!
     * Returns a page that belongs to this file to its free page list. The
     * parameter \p bufid should be a valid buffer ID where the page to return
     * is pinned.
     *
     * After the call, the pin on the page is dropped.
     *
     * It is an error if it is called on a TMP or WAL file.
     */
    void FreePage(BufferId bufid);

    /*!
     * Same as FreePage(BufferId) but prevents double free.
     */
    void FreePage(ScopedBufferId &bufid) {
        FreePage(bufid.Release());
    }

    /*!
     * Returns the first page's PID. The first page's PID never changes so
     * it is not possible that there is any page before the returned page.
     */
    PageNumber GetFirstPageNumber();

    /*!
     * Returns the last page's PID. Note that concurrent threads may still
     * append new pages into this file, so the returned PID might not be the
     * last by the time the page is read.
     */
    PageNumber GetLastPageNumber();

    /*!
     * Not supported.
     */
    void Flush();

    constexpr FileId
    GetFileId() const {
        return m_fileid;
    }

private:
    File():
        m_fileid(INVALID_FID),
        m_meta_pid(INVALID_PID) {}

    /*!
     * The actual implementation of AllocatePage().
     */
    uint64_t AllocatePageImpl(bool need_latch, LatchMode mode);

    FileId          m_fileid;
    PageNumber      m_meta_pid;

    friend class FileManager;
};

}   // namespace taco

#endif      // STORAGE_FILEMANAGER_H
