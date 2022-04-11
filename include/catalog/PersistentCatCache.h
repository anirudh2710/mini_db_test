#ifndef CATALOG_PERSISTENTCATCACHE_H
#define CATALOG_PERSISTENTCATCACHE_H

#include "tdb.h"

#include "catalog/CatCacheBase.h"
#include "storage/BufferManager.h"
#include "storage/FileManager.h"
#include "storage/Record.h"
#include "storage/Table.h"
#include "index/BulkLoadIterator.h"

namespace taco {

class PersistentCatCache: public CatCacheBase<PersistentCatCache> {
public:
    PersistentCatCache();

private:
    /*!
     * Creates a new catalog file and returns its file ID. If \p
     * format_heapfile is true, the opened file will be accessed through the
     * following record and iterator iterfaces. Otherwise, it is accessed as
     * an unformatted file consisting of pages, which should initially have
     * just one page.
     *
     * The first file ever allocated through this function is
     * assumed to have file ID 1.
     */
    FileId CreateCatalogFile(bool format_heapfile);

    /*!
     * An opaque handle for a catalog file. It must be automatically closed if
     * it goes out of scope. In PersistentCatCache, we only keep a file ID, and
     * we don't actually open the file, see `GetFirstPage()' for explanation.
     * This allows us to do nothing for closing the file.
     */
    struct FileHandle {
        // the file ID of the table
        FileId                  m_fid;

        // the heap file, this might be nullptr initially
        std::unique_ptr<Table>  m_table;
    };

    /*!
     * Opens a catalog file for access. The second argument \p tabdesc is
     * always needed except for variable-length build during initialization
     * and the db meta page.
     */
    FileHandle OpenCatalogFile(FileId fid, const TableDesc *tabdesc);

    /*!
     * Closes a catalog file pointed by the file handle.
     */
    void CloseCatalogFile(FileHandle &fh);

    /*!
     * An opaque handle for a data page in an unformatted catalog file. It
     * must be automatically release if it goes out of scope.
     */
    typedef ScopedBufferId PageHandle;

    /*!
     * Returns a handle to the first page in the unformatted catalog file with
     * it pinned in the memory.
     *
     * The only valid value of the file handle is the file ID of the DB meta
     * file, so we cache its first page number (NOTE which is assumed to never
     * change with the current implementation of FileManager). This returns a
     * ScopedBufferId as a page handle by pinning the first page in the buffer
     * manager. If `GetFirstPage()' is called with any other file ID, it throws
     * a fatal error.
     */
    PageHandle GetFirstPage(FileHandle &fh, char **pagebuf);

    /*!
     * Marks a page pointed by the page handle as dirty.
     */
    void MarkPageDirty(PageHandle &pghandle);

    /*!
     * Releases the pin over the page pointed by the page handle.
     */
    void ReleasePage(PageHandle &pghandle);

    /*!
     * Appends a record to the catalog file specified by the file ID. This also
     * updates the `rec.GetRecordID()' to the record ID of the newly inserted
     * record.
     */
    void AppendRecord(FileHandle &fh, Record &rec);

    /*!
     * An opaque handle for iterating a catalog file. It must be automatically
     * ended if it goes out of scope.
     */
    typedef Table::Iterator CatFileIterator;

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
     * Releses any resource associated with the catalog file iterator.
     */
    void EndIterateCatEntry(CatFileIterator &iter);

    /*!
     * Returns a BulkLoadIterator for bulk loading a systable index.
     */
    BulkLoadIterator *GetBulkLoadIterator(
        FileHandle &fh,
        std::shared_ptr<const TableDesc> tabdesc,
        std::shared_ptr<const IndexDesc> idxdesc);

    /*!
     * Creates the Table object in the file handle using a fake table
     * descriptor. \p reclen is only required when we are building with
     * fixed-length data pages only.
     */
    void _CreateTableIfNullForFileHandle(FileHandle &fh, FieldOffset reclen);

    /*!
     * The page number of the DB meta page, which is the first page of the DB
     * meta file.
     */
    PageNumber  m_dbmetapg_pid;

    friend class CatCacheBase<PersistentCatCache>;
};

/*!
 * Declar explicit instantiation of the base class of PersistentCatCache, which
 * will be defined in catalog/PersistentCatCache.cpp.
 */
extern template class CatCacheBase<PersistentCatCache>;

}   // namespace taco

#endif      // CATALOG_PERSISTENTCATCACHE_H
