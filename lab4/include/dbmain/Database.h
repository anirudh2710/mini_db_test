/*!
 * @file
 *
 * This file defines the global database class and its global instance.
 *
 * As almost every source file in this project includes dbmain/Datbase.h
 * indirectly from tdb.h, we must not include any header file except tdb_base.h.
 * We use forward declaration of needed classes instead.
 */
#ifndef DBMAIN_DATABASE_H
#define DBMAIN_DATABASE_H

#include "base/tdb_base.h"

namespace taco {

// Always use forward declaration of other classes instead of including the
// headers to avoid cyclic references. Don't use the class members in the
// Database.h header!!
class FileManager;
class CatCache;
class BufferManager;
class Table;
class Schema;

/*!
 * Set this to true if you don't want Database to create and initialize the
 * buffer manager. Default: false. Only to use in tests. Must be set before
 * Database::open() call.
 */
extern bool g_test_no_bufman;

/*!
 * Set this to true if you don't want Database to create and initialize the
 * catalog and its cache. Default: false. Only to use to tests. Must be set
 * before Database::open() call.
 */
extern bool g_test_no_catcache;

/*!
 * Set this to true if you don't want the Database to disallow building any
 * index (including the catalog tables). This will force the catalog cache to
 * fall back to table scans to find records.
 */
extern bool g_test_no_index;

/*!
 * Set this to true if you want the catalog cache to build volatile tree index
 * over the catalog tables. This only has effect if g_test_no_index is false
 * and during initialization of the database catalog. If the database catalog
 * has already been initialized, the choice of index during the initialization
 * takes precedence.
 */
extern bool g_test_catcache_use_volatiletree;

/*!
 * The class for a database instance.
 *
 * There may only be one global database instance ``g_db'' at any time (see
 * below). The constructor and destructor of the ``Database'' class must be
 * trivial because of the existence of the global variable. To
 * initialize/destroy a database, call ``g_db->open(...)'' and
 * ``g_db->close()''.
 */
class Database {
public:
    //! A trivial constructor.
    Database():
        m_initialized(false) {}

    /*!
     * Automatically closes the database if not closed.
     */
    ~Database() {
        close();
    }

    /*!
     * Initializes all the global objects that are not associated with a
     * particular database. This must be called the first thing in main() and
     * this may not be called more than once.
     */
    static void init_global();

    /*!
     * Initializes the database components and opens or creates the database at
     * the specified \p path.
     *
     * If \p allow_overwrite and create are both true and the specified path
     * already exists, it silently removes the existing directory before
     * opens the database.
     */
    void open(const std::string& path, size_t bpool_size, bool create,
                bool allow_overwrite = false);

    constexpr bool
    is_open() const {
        return m_initialized;
    }

    /*!
     * Closes the database. This also releases all the resources even if
     * the database ends up in an error state.
     */
    void close();

    FileManager*
    file_manager() const {
        return m_file_manager;
    }

    BufferManager*
    buf_manager() const {
        return m_buf_manager;
    }

    CatCache*
    catcache() const {
        return m_catcache;
    }

    Schema* actres_schema() const;

    /*!
     * Creates a table named ``tabname'', with ``coltypid.size()'' columns.
     * All the remaining vectors  may be empty or of the same size as
     * ``coltypid''. We will assign default values to the remainder of the
     * parameters if they are empty. More specifically, the default type
     * parameter is 0. The default field name for the ith column (starting from
     * 0) is ``col_i''. By default, none of the field is nullable or is an
     * array.
     */
    void CreateTable(absl::string_view tabname,
                     std::vector<Oid> coltypid,
                     std::vector<uint64_t> coltypparam = {},
                     const std::vector<absl::string_view> &field_names = {},
                     std::vector<bool> colisnullable = {},
                     std::vector<bool> colisarray = {});

    /*!
     * Createa an index named ``idxname'' and inserts into the catalog.
     * If ``idxname'' is an empty string, a default name will be generated.
     *
     * The caller may optionally provide non-default < and = operators, as
     * specified in CatCacheBase::AddIndex(). Both \p idxcolltfuncids and \p
     * idxcoleqfuncids may be shorter than \p idxcoltabcolids and may have
     * \p InvalidOid. Missing values and \p InvalidOid are looked up from
     * the catalog using the default operators for the type.
     */
    void CreateIndex(absl::string_view idxname,
                     Oid idxtabid,
                     IdxType idxtyp,
                     bool idxunique,
                     std::vector<FieldId> idxcoltabcolids,
                     std::vector<Oid> idxcolltfuncids = {},
                     std::vector<Oid> idxcoleqfuncids = {});

    const std::string&
    GetLastDBPath() const {
        return m_db_path;
    }

private:
    bool            m_initialized;
    std::string     m_db_path;
    FileManager     *m_file_manager;
    BufferManager   *m_buf_manager;
    CatCache        *m_catcache;
    Schema          *m_actres_schema;
};

/*! The global instance of Database. */
extern Database * const g_db;

/*! Shorthand for `g_db->buf_manager()` */
#define g_bufman g_db->buf_manager()

/*! Shorthand for `g_db->file_manager()` */
#define g_fileman g_db->file_manager()

/*! Shorthand for `g_db->catcache()` */
#define g_catcache g_db->catcache()

/*! Shorthand for `g_db->actres_sch()` */
#define g_actsch g_db->actres_schema()

}  // namespace taco

#endif      // DBMAIN_DATABSE_H
