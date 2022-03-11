// storage/Table.h
#pragma once

#include "tdb.h"

#include <unordered_set>

#include "catalog/TableDesc.h"
#include "storage/BufferManager.h"
#include "storage/FileManager.h"
#include "storage/Record.h"

namespace taco {

class File;

/*!
 * Table implements a heap file over the virtual file provided by the
 * FileManager. A heap file manages a list of data pages, and supports
 * inserting, deleting, updating and forward-iterating the records stored in
 * the file. A Table object and its iterators should use the buffer manager to
 * access its pages, rather than directly going through the File interface.
 *
 * Note:
 * 1. You may assume tabisvarlen in the table entry of the table descriptor
 * passed to the Table class is always `true`.
 * 2. Table may have a non-trivial constructor and/or a non-trivial destructor,
 * but they must not throw any error.
 * 3. A Table object should not store any heap file's meta data or content
 * locally, so that two Table objects created on the same heap file should be
 * able to see the effect of each other as long as there's no data race.
 */
class Table {
public:
    class Iterator;

    /*!
     * Inititalizes a table on a newly created virtual file. For all user tables,
     * the file should have been inserted into the catalog cache and the caller
     * should supply a table descriptor returned by the catalog cache.
     *
     * For bootstrapping and initialization of the catalog cache, it may pass a
     * fake table descriptor which at minimum has the following set:
     *  - `tabdesc->GetTableEntry()->tabissys()`
     *  - `tabdesc->GetTableEntry()->tabfid()`
     *  - `tabdesc->GetTableEntry()->tabisvarlen`
     *
     * All other members may or may not be valid (nullptr), so you may only
     * access the above members of the table descriptor in `Table::Initialize()`.
     *
     * It is undefined if `Initialize()` is called more than once on a file, or
     * the file has been modified since it is created.
     */
    static void Initialize(const TableDesc *tabdesc);

    /*!
     * Constructs a table object on a heap file that was previously initialized
     * by `Table::Initialize()`. For all user tables, the caller should pass a
     * table descriptor returned by the catalog cache.
     *
     * For bootstrapping and initialization of the catalog cache, it may pass a
     * fake table descriptor which at minimum needs to have the following set:
     *  - `tabdesc->GetTableEntry()->tabissys()`
     *  - `tabdesc->GetTableEntry()->tabfid()`
     *  - `tabdesc->GetTableEntry()->tabisvarlen`

     * All other members may or may not be valid (nullptr), so you may only
     * access the above members of the table descriptor in `Table::Create()`.
     *
     * It is undefined if the file referenced by the table descriptor was not
     * previously initialized by `Initialize()`.
     */
    static std::unique_ptr<Table> Create(
        std::shared_ptr<const TableDesc> tabdesc);

private:
    /*!
     * Constructor. It should be private and it must not throw any error.
     *
     * TODO you may add any arguments to the constructor that you need
     */
    Table(std::shared_ptr<const TableDesc> tabdesc);

public:

    /*!
     * Destructor. It must not throw any error.
     */
    ~Table();

    /*!
     * Returns the table descriptor of this table.
     */
    const TableDesc*
    GetTableDesc() const {
        return m_tabdesc.get();
    }

    /*!
     * Inserts the record into table. It may insert a the record on an existing
     * page or create a new page to insert the record if necessary.  Upon
     * successful return, \p rec.GetRecordId() should be set to the record ID
     * of the newly inserted record.  Otherwise, rec.GetRecordId() is
     * undefined.
     *
     * Note: for the heap file project, you may always insert to the end of the
     * file without reusing empty spaces in the middle of the file.
     *
     * It is an error if the record cannot be inserted. It is undefined if the
     * record \p rec does not have its data pointer and length correctly set.
     */
    void InsertRecord(Record& rec);

    /*!
     * Erases the record specified by the record ID. This function will obtain
     * an additional page pin on the target page, even if the caller already
     * has one in an iterator, but it must release the additional pin upon
     * exit. If a page becomes empty, EraseRecord() must call File::FreePage()
     * to return the page to the file manager.
     *
     * It is an error if it cannot erase the record (e.g., because it's not
     * found), or \p rid does not belong to this table.
     */
    void EraseRecord(RecordId rid);

    /*!
     * Updates the record specified by the record ID. Upon return, the
     * `rec.GetRecordID()' is updated to the new record ID of the updated
     * record (which may be different from \p rid). This function will obtain
     * an additional page pin on the target page, even if the caller already
     * has one in an iterator, but it must release the additional pin upon
     * exit. If a page becomes empty, UpdateRecord() does not have to free
     * the page in the file manager.
     *
     * Note that the function must perform in-place update whenever the new
     * record's length is no larger than the old record's length at \p rid.
     *
     * It is an error if the update cannot be completed, in which case,
     * the old record at \p rid must not be erased. It is also an error
     * if \p rid does not belong to this table.
     */
    void UpdateRecord(RecordId rid, Record &rec);

    /*!
     * Starts a scan of the table from the beginning.
     */
    Iterator StartScan();

    /*!
     * Starts a scan of the table so that the first call to `Iterator::Next()`
     * will return the first record with an ID greater than or equal to \p rid.
     */
    Iterator StartScanFrom(RecordId rid);

private:

    /*!
     * The descriptor of the table.
     */
    std::shared_ptr<const TableDesc> m_tabdesc;

    // TODO add any private members you need

public:

    /*!
     * The Iterator interface for scanning the heap file.
     *
     * Note: a call to Table::InsertRecord(), Table::UpdateRecord(), or
     * Table::EraseRecord() may insert a new record and/or remove a record
     * somewhere in the file. You do not have to worry about them in the heap
     * file project as long as the implementation is able to return exactly all
     * the records in the file, provided that there are no concurrent update
     * operations, or there's only in-place update of the current record.
     */
    class Iterator {
    public:
        /*!
         * Constructs an invalid iterator.
         */
        Iterator():
            m_table(nullptr) {}

    private:
        /*!
         * Constructs an iterator right before the specified record ID. A
         * `Next()` call will return the first valid record after that if any.
         */
        Iterator(Table* tbl, RecordId rid);

    public:
        /*!
         * Destructs an iterator. It should attemp to call EndScan() but may
         * not throw any error.
         */
        ~Iterator() {
            try {
                if (GetTable()) {
                    EndScan();
                }
            } catch (const TDBError &e) {
                // Logs a warning about errors in the end scan call, which is
                // most likely because BufferManager::UnpinPage() throws some
                // error caused by some bug in the Iterator. There's no
                // sensible thing we could do here to mitigate the issue. If
                // the page is still pinned, the buffer manager will throw a
                // fatal error when the Database destroys it, which will
                // provide greater details for debugging than calling
                // std::terminate() here.
                LOG(kWarning, "unable to destruct the iterator due to an error "
                        "in Table::Iterator::EndScan(): \n%s", e.GetMessage());
            }
        }

        /*!
         * Default move constructor.
         */
        Iterator(Iterator&& it) = default;

        /*!
         * We can use the default move assignment only if EndScan does not do
         * anything other than unpin the page and we use ScopedBufferId to
         * store the buffer ID. Otherwise, you'll have to implement your own
         * move assignment operator.
         */
        Iterator &operator=(Iterator &&it) = default;

        /*!
         * Deleted copy constructor.
         */
        Iterator(const Iterator& it) = delete;

        /*!
         * Deleted copy assignment.
         */
        Iterator &operator=(const Iterator &it) = delete;

        /*!
         * Returns the table. It is only non-null when the iterator is a valid
         * iterator and has not been ended.
         */
        constexpr Table*
        GetTable() const {
            return m_table;
        }

        /*!
         * Returns the current record, which is only valid when a previous
         * Next() call returns true.
         */
        constexpr const Record&
        GetCurrentRecord() const {
            return m_cur_record;
        }

        /*!
         * Returns the record ID of the current record.
         */
        RecordId
        GetCurrentRecordId() const {
            return m_cur_record.GetRecordId();
        }

        /*!
         * Returns `GetCurrentRecord().IsValid()`.
         */
        bool
        IsAtValidRecord() const {
            return m_cur_record.IsValid();
        }

        /*!
         * Moves the iterator to the next record in the file. Returns `true` if
         * there is such a record, and the current record of this iterator
         * stores a pointer to its buffer on a buffered page. The page pin
         * should be held as long as the current record still points the buffer
         * on the buffered page. Otherwise, it returns `false`.
         *
         * Note: the tuples that appear on an earlier page of the file must be
         * iterated before those that appear on a later page, and the tuples
         * on the same page must be iterated in the order from the smallest
         * occupied slot id to the largest occupied slot id.
         */
        bool Next();

        /*!
         * Ends the scan, which may perform operations that may throw errors
         * such as releasing page pins. You should always explicitly calling
         * EndScan() to avoid any error being thrown and caught in the
         * destructor.
         *
         * After the EndScan() call, the iterator becomes invalid and the only
         * functions may be called on this iterator are EndScan() and the
         * destructor (which should do nothing). Note that EndScan() may be
         * called from the destructor after the table object is destructed in
         * exception handling, so make sure not to dreference `m_table` in
         * EndScan().
         */
        void EndScan();

    private:
        //! The table that the iterator is iterating on.
        Table*          m_table;

        /*!
         * The current record the iterator is on if the most recent Next() call
         * returns `true`. Otherwise, it should be invalid.
         */
        Record          m_cur_record;

        /*!
         * A pin on the page where the current record is located.
         */
        ScopedBufferId  m_pinned_bufid;

        // TODO add additional members if needed

        friend class Table;
    };

    friend class Iterator;
};

}   // namespace taco
