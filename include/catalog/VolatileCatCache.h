#ifndef CATALOG_VOLATILE_CATCACHE_H
#define CATALOG_VOLATILE_CATCACHE_H

#include "tdb.h"

#include "catalog/CatCacheBase.h"
#include "storage/Record.h"

namespace taco {

class VolatileCatCache: public CatCacheBase<VolatileCatCache> {
public:
    /*!
     * Not implemented. Volatile catlog cache is never saved on disk so it
     * can't be initialized from an existing data directory.
     */
    void InitializeFromExistingData() {
        LOG(kError,
            "voatile catlog cannot be initialized from an existing data "
            "directory");
    }

private:
    struct InmemFile {
        std::vector<bool>       m_valid;
        std::vector<size_t>     m_offsets;
        std::vector<char>       m_bytes;
    };

    /*!
     * An opaque handle for iterating a catalog file. It must be automatically
     * ended if it goes out of scope.
     */
    struct CatFileIterator {
        FileId      m_fid;
        uint16_t    m_nextidx;
        uint16_t    m_endidx;
    };

    /*!
     * Creates a new catalog file and returns its file ID. If \p
     * format_heapfile is true, the opened file will be accessed through the
     * following record and iterator iterfaces. Otherwise, it is accessed as
     * an unformatted file consisting of pages, which should initially have
     * just one page. The second argument is unused by VolatileCatCache and
     * is only kept for compatibility.
     *
     * The FileID does not need to be unique from regular files in the file
     * manager, but the first file ever allocated through this function is
     * assumed to have file ID 1.
     *
     * Note this file ID is not allocated by the file manager in
     * VolatileCatCache.
     */
    FileId CreateCatalogFile(bool format_heapfile, FieldOffset /*unused*/ = 0);

    /*!
     * An opaque handle for a catalog file. It must be automatically closed
     * if it goes out of scope. In VolatileCatCache, the files do not need
     * to be closed, so we just use the file ID as the handle.
     */
    typedef FileId FileHandle;

    /*!
     * Opens a catalog file for access. The second argument \p tabdesc is
     * always needed except for variable-length build during initialization
     * and the db meta page.
     *
     * VolatileCatCache does not use the provided \p tabdesc.
     */
    FileHandle OpenCatalogFile(FileId fid, const TableDesc *tabdesc);

    /*!
     * Closes a catalog file pointed by the file handle. This does nothing
     * except for setting fh to INVALID_FID in VolatileCatCache.
     */
    void CloseCatalogFile(FileHandle &fh);

    /*!
     * An opaque handle for a data page in an unformatted catalog file. It
     * must be automatically release if it goes out of scope.
     */
    typedef int PageHandle;

    /*!
     * Returns a handle to the first page in the unformatted catalog file with
     * it pinned in the memory. In VolatileCatCache, one may only invoke this
     * on the DB meta file. And the returned page buffer is not in buffer
     * manager, rather it's in our own internal state.
     */
    PageHandle GetFirstPage(FileHandle &fh, char **pagebuf);

    /*!
     * Marks a page pointed by the page handle as dirty. This does nothing in
     * VolatileCatCache.
     */
    void MarkPageDirty(PageHandle &pghandle);

    /*!
     * Releases the pin over the page pointed by the page handle. This does
     * nothing in VolatileCatCache.
     */
    void ReleasePage(PageHandle &pghandle);

    /*!
     * Appends a record to the catalog file specified by the file ID. This also
     * updates the `rec.GetRecordID()' to the record ID of the newly inserted
     * record.
     */
    void AppendRecord(FileHandle &fh, Record &rec);

    /*!
     * Creates an iterator over the catalog file specified by the file ID.
     */
    CatFileIterator IterateCatEntry(FileHandle &fh);

    /*!
     * Creates an iterator that starts at \p rid. The iterator may or may not
     * return additional records after the first one if \p rid exists.
     */
    CatFileIterator IterateCatEntryFrom(FileHandle &fh, RecordId rid);

    /*!
     * Tries to move the iterator to the next row and returns whether such
     * a row exists.
     */
    bool NextCatEntry(CatFileIterator &iter);

    /*!
     * Returns the current catalog entry pointed by the iterator as a buffer
     * pointer. It is undefined if a NextCatEntry has not been called or a
     * previous call returns `false'.
     */
    const char *GetCurrentCatEntry(CatFileIterator &iter);

    /*!
     * Returns the current catalog entry's record ID.  It is undefined if a
     * NextCatEntry has not been called or a previous call returns `false'.
     */
    RecordId GetCurrentCatEntryRecordId(CatFileIterator &iter);

    /*!
     * Updates the current catalog entry pointed by the iterator with the
     * record \p rec.  This invalidates the current position of the iterator so
     * it is undefined to call either `GetCurrentCatEntry' or
     * `UpdateCurrentCatEntry' after this call. However, one may still call
     * `NextCatEntry' on it to resume the iteration. There is no guarantee that
     * the same entry will not be returned again though.
     *
     * This is intended to be used for updating either a single entry (and then
     * ending the iteration), or for performing initialization in the catalog
     * initializer.
     */
    void UpdateCurrentCatEntry(CatFileIterator &iter, Record &rec);

    /*!
     * Releses any resource associated with the catalog file iterator. There is
     * no resource associated with an iterator in VolatileCatCache, so this
     * does nothing except for invalidating the iterator.
     */
    void EndIterateCatEntry(CatFileIterator &iter);

    /*!
     * This vector stores all our in-memory only files. The index into
     * this file is file ID - 2.
     */
    std::vector<std::unique_ptr<InmemFile>> m_systables;

    /*!
     * The pointer to the in-memory only dbmeta page.
     */
    unique_malloced_ptr m_dbmeta_page;

    friend class CatCacheBase<VolatileCatCache>;
};

/*!
 * Declare explicit instantiation of the base class of VolatileCatCache, which
 * will be defined in catalog/VolatileCatCache.cpp.
 */
extern template class CatCacheBase<VolatileCatCache>;

}    // namespace taco

#endif      // CATALOG_VOLATILE_CATCACHE_H
