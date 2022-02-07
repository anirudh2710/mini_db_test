#ifndef CATALOG_CATCACHEBASE_H
#define CATALOG_CATCACHEBASE_H

#include "tdb.h"

#include <absl/container/flat_hash_map.h>

#include "catalog/TableDesc.h"
#include "catalog/IndexDesc.h"
#include "catalog/systables.h"
#include "storage/Record.h"

namespace taco {

/*!
 * This is an internal data structure of the catalog cache for storing an
 * in-memory catalog table entry.
 */
struct CCLookupTableEntry {
    CCLookupTableEntry(RecordId recid, std::shared_ptr<void> systable_struct):
        m_recid(recid),
        m_systable_struct(std::move(systable_struct)) {}

    CCLookupTableEntry(const CCLookupTableEntry&) = delete;
    CCLookupTableEntry &operator=(const CCLookupTableEntry&) = delete;
    CCLookupTableEntry(CCLookupTableEntry&& e) = default;
    CCLookupTableEntry &operator=(CCLookupTableEntry&& e) = default;

    RecordId                m_recid;
    std::shared_ptr<void>   m_systable_struct;
};

/*!
 * Some internal functions of catalog cache implementations. This is a friend
 * class of all SysTable_xxx structs so that it can provide access to members
 * that are otherwise inaccessible.
 */
class CatCacheInternalAccess {
protected:

    /*!
     * Creates a SysTable_xxx struct for a row stored in a record payout in the
     * specified table. This overload returns an std::shared_ptr<void> which is
     * an alias for an std::shared_ptr<SysTable_xxx> and will automatically
     * destruct the object when there's no reference to the object.
     *
     * The implementation of this function is automatically generated in
     * catalog/CatCacheBase_gen.cpp.
     */
    static std::shared_ptr<void> CreateSysTableStruct(
        Oid tabid, const std::vector<Datum> &data);

    /*!
     * Creates a SysTable_xxx struct from a vector of data by invoking its
     * Create() function. This overload returns a plain pointer and the caller
     * is responsible for managing the memory of the object, as the caller
     * knows the exact type of the returned struct.
     */
    template<class T>
    static T*
    CreateSysTableStruct(const std::vector<Datum> &data) {
        return T::Create(data);
    }

    template<class T>
    static T*
    CopySysTableStruct(const T *s) {
        T *copy = new T(*s);
        return copy;
    }

    /*!
     * Constructs a SysTable_xxx struct from c++ values by invoking its
     * constructor.
     */
    template<class T, class ...Args>
    static T*
    ConstructSysTableStruct(Args&& ...args) {
        return new T(std::forward<Args>(args)...);
    }

    template<class T>
    std::vector<Datum>
    GetDatumVector(const T& systable_struct) {
        return systable_struct->GetDatumVector();
    }
};

/*!
 * CatCacheBase implements the common routines and public interfaces for
 * accessing and modifying the system catalog files. Internally it maintains a
 * few caches of the catalog entries so all other components should always
 * access the catalog files through the global catalog cache `g_catcache'.
 *
 * It depends on its subclass \p CatCacheCls to provide a fe functions for
 * catalog file accesses. Currently we have two implementations:
 * `VolatileCatCache' which provides file accesses to virtual in-memory files,
 * and `PersistentCatCache' which provides file accsess to the regular files
 * managed by the file manager. See these two classes for a list of required
 * file access functions.
 */
template<class CatCacheCls>
class CatCacheBase: public CatCacheInternalAccess {
public:
    CatCacheBase();

    /*!
     * Initializes the catalog cache from the existing data.
     */
    void InitializeFromExistingData();

    /*!
     * Creates the catalog from an init data file and initializes the catalog
     * cache.
     */
    void InitializeFromInitData(const std::string &init_datafile);

    /*!
     * Returns if the catalog cache has been initialized.
     */
    constexpr
    bool IsInitialized() const {
        return m_initialized;
    }

    inline std::shared_ptr<const SysTable_Table>
    FindTable(Oid tabid) {
        auto entry =
            SearchForCatalogEntryByOid(initoids::TAB_Table,
                                       initoids::IDX_Table_tabid,
                                       SysTable_Table::tabid_colid(),
                                       tabid);
        if (!entry) {
            return nullptr;
        }
        return static_pointer_cast<const SysTable_Table>(
            entry->m_systable_struct);
    }

    inline Oid
    FindTableByName(absl::string_view tabname) {
        auto entry =
            SearchForCatalogEntryByName(initoids::TAB_Table,
                                        initoids::IDX_Table_tabname,
                                        SysTable_Table::tabname_colid(),
                                        tabname);
        if (!entry) {
            return InvalidOid;
        }
        return ((const SysTable_Table*)(entry->m_systable_struct.get()))
            ->tabid();
    }

    std::shared_ptr<const TableDesc> FindTableDesc(Oid tabid);

    inline std::shared_ptr<const SysTable_Type>
    FindType(Oid typid) {
        auto entry =
            SearchForCatalogEntryByOid(initoids::TAB_Type,
                                       initoids::IDX_Type_typid,
                                       SysTable_Type::typid_colid(),
                                       typid);
        if (!entry) {
            return nullptr;
        }
        return static_pointer_cast<const SysTable_Type>(
            entry->m_systable_struct);
    }

    inline std::shared_ptr<const SysTable_Function>
    FindFunction(Oid funcid) {
        auto entry =
            SearchForCatalogEntryByOid(initoids::TAB_Function,
                                       initoids::IDX_Function_funcid,
                                       SysTable_Function::funcid_colid(),
                                       funcid);
        if (!entry) {
            return nullptr;
        }
        return static_pointer_cast<const SysTable_Function>(
            entry->m_systable_struct);
    }

    inline Oid
    FindFunctionByName(absl::string_view funcname) {
        auto entry =
            SearchForCatalogEntryByName(initoids::TAB_Function,
                                        initoids::IDX_Function_funcname,
                                        SysTable_Function::funcname_colid(),
                                        funcname);
        if (!entry) {
            return InvalidOid;
        }
        return ((const SysTable_Function*)(entry->m_systable_struct.get()))
            ->funcid();
    }

    inline std::shared_ptr<const SysTable_FunctionArgs>
    FindFunctionArgs(Oid funcid, int16_t funcargid) {
        auto entry = SearchForCatalogEntry<true, 2, false>::Call(
            this, initoids::TAB_FunctionArgs,
            initoids::IDX_FunctionArgs_funcid_funcargid,
            {SysTable_FunctionArgs::funcid_colid(),
             SysTable_FunctionArgs::funcargid_colid()},
            {initoids::FUNC_OID_eq, initoids::FUNC_INT2_eq},
            funcid, funcargid);
        if (!entry) {
            return nullptr;
        }
        return static_pointer_cast<const SysTable_FunctionArgs>(
            entry->m_systable_struct);
    }

    inline std::shared_ptr<const SysTable_Index>
    FindIndex(Oid idxid) {
        auto entry =
            SearchForCatalogEntryByOid(initoids::TAB_Index,
                                       initoids::IDX_Index_idxid,
                                       SysTable_Index::idxid_colid(),
                                       idxid);
        if (!entry) {
            return nullptr;
        }
        return static_pointer_cast<const SysTable_Index>(
            entry->m_systable_struct);
    }

    inline Oid
    FindIndexByName(absl::string_view idxname) {
        auto entry =
            SearchForCatalogEntryByName(initoids::TAB_Index,
                                        InvalidOid,
                                        SysTable_Index::idxname_colid(),
                                        idxname);
        if (!entry) {
            return InvalidOid;
        }
        return ((const SysTable_Index*)(entry->m_systable_struct.get()))
            ->idxid();
    }

    inline std::vector<Oid>
    FindAllIndexesOfTable(Oid idxtabid) {
        auto entries = SearchForCatalogEntry<false, 1, false>::Call(
            this, initoids::TAB_Index, initoids::IDX_Index_idxtabid, 0,
            {SysTable_Index::idxtabid_colid()}, {initoids::FUNC_OID_eq},
            idxtabid);
        std::vector<Oid> ret;
        ret.reserve(entries.size());
        for (auto &entry: entries) {
            ret.push_back(
                ((SysTable_Index*) entry->m_systable_struct.get())->idxid());
        }
        return ret;
    }

    std::shared_ptr<const IndexDesc> FindIndexDesc(Oid idxid);

    /*!
     * Returns the operator function id of the one with the specific operand
     * types and operator type.
     *
     * It is an error if the operand types are not fully specified (e.g., only
     * one side specified for a binary operator). TODO Use FindOperators()
     * instead to find operators that partially match the operand types.
     */
    Oid FindOperator(OpType optype, Oid oparg0typid, Oid oparg1typid);

    /*!
     * Searches for the systable entry with ID \p recid in the given systable
     * \p systabid.
     *
     * This assumes that there is only one record matching the oid. Otherwise,
     * it will return the first one it finds.
     */
    inline CCLookupTableEntry*
    SearchForCatalogEntryByOid(
        Oid systabid,
        Oid idxid_hint,
        FieldId oid_colid,
        Oid oid) {
        return SearchForCatalogEntry<true, 1, false>::Call(
            this, systabid, idxid_hint,
            {oid_colid}, {initoids::FUNC_OID_eq}, oid);
    }

    /*!
     * Searches for the systable entry with its name column at \p name_colid
     * equals to \p name.
     *
     * This assumes there is only one record matching the name. Otherwise, it
     * will return the first one it finds.
     */
    inline CCLookupTableEntry*
    SearchForCatalogEntryByName(
        Oid systabid,
        Oid idxid_hint,
        FieldId name_colid,
        absl::string_view name) {
        return SearchForCatalogEntry<true, 1, false>::Call(
            this, systabid, InvalidOid, {name_colid},
            {initoids::FUNC_VARCHAR___STRING_eq_ci}, name);
    }

    template<bool expect_unique, size_t NPreds, bool no_cache>
    struct SearchForCatalogEntry {
        using EntryPtr = typename std::conditional<
            no_cache,
            std::unique_ptr<CCLookupTableEntry>,
            CCLookupTableEntry*>::type;
        using RetType = typename std::conditional<
            expect_unique,
            EntryPtr,
            std::vector<EntryPtr>>::type;

        template<class RHS0, class ...RemRHS>
        inline static
        typename std::enable_if<std::is_same<RHS0, RHS0>::value
                                && expect_unique, RetType>::type
        Call(CatCacheBase *_this,
             Oid systabid,
             Oid idxid_hint,
             const std::array<FieldId, NPreds> &fieldid,
             const std::array<Oid, NPreds> &eq_funcid,
             RHS0&& rhs0,
             RemRHS&& ...remrhs) {
            return Impl(_this, systabid, idxid_hint, 0, fieldid, eq_funcid,
                std::forward<RHS0>(rhs0), std::forward<RemRHS>(remrhs)...);
        }

        template<class RHS0, class ...RemRHS>
        inline static
        typename std::enable_if<std::is_same<RHS0, RHS0>::value
                                && !expect_unique, RetType>::type
        Call(CatCacheBase *_this,
             Oid systabid,
             Oid idxid_hint,
             size_t expect_n,
             const std::array<FieldId, NPreds> &fieldid,
             const std::array<Oid, NPreds> &eq_funcid,
             RHS0&& rhs0,
             RemRHS&& ...remrhs) {
            return Impl(_this, systabid, idxid_hint, expect_n, fieldid,
                eq_funcid, std::forward<RHS0>(rhs0),
                std::forward<RemRHS>(remrhs)...);
        }

    private:
        template<class ...RHS>
        static RetType
        Impl(CatCacheBase *_this,
             Oid systabid,
             Oid idxid_hint,
             size_t expect_n,
             const std::array<FieldId, NPreds> &fieldid,
             const std::array<Oid, NPreds> &eq_funcid,
             RHS&& ...rhs);
    };

    template<bool, size_t, bool> friend struct SearchForCatalogEntry;

    /*!
     * Adds a table into the catalog.
     */
    Oid AddTable(absl::string_view tabname,
                 std::vector<Oid> coltypid,
                 std::vector<uint64_t> coltypparam,
                 std::vector<std::string> field_names,
                 std::vector<bool> colisnullable,
                 std::vector<bool> colisarray,
                 FileId tabfid);

    /*!
     * Adds an index into the catalog.
     *
     * The caller may optionally provide the less functions and equality
     * functions that might be different from the default < and = operators
     * for the type (e.g., case insensitive string comparison). Any InvalidOid
     * or missing values in idxcolltfuncids and idxcoleqfuncids are looked up
     * from the catalog.
     */
    Oid AddIndex(absl::string_view idxname,
                 Oid idxtabid,
                 IdxType idxtyp,
                 bool idxunique,
                 std::vector<FieldId> idxcoltabcolids,
                 FileId idxfid,
                 std::vector<Oid> idxcolltfuncids,
                 std::vector<Oid> idxcoleqfuncids);
private:
    /*!
     * Creates and initializes the database meta file. This must be called
     * first when creating a new database.
     */
    void CreateDBMeta();

    /*!
     * Loads the init catalog file and writes them into the catalog files.
     * Returns a map from catalog table ID to its file ID.
     */
    absl::flat_hash_map<Oid, FileId> LoadInitFile(
        const std::string &init_datafile,
        BootstrapCatCache *catcache);

    /*!
     * Finishes the initialization of the catalog and updates the DB meta file
     * if necessary.
     */
    void FinishInitCatalog(const absl::flat_hash_map<Oid, FileId> &tabid2fid,
                           BootstrapCatCache *catcache);

    /*!
     * Loads the minimum set of the required catalog entries for initializing
     * the cache. This function completes the initialization of the catalog
     * cache.
     */
    void LoadMinCache(BootstrapCatCache *catcache);

    /*!
     * Checks if any of the catalog indexes need to be initialized and/or
     * rebuilt. If \p init is true, it will go over all the entries in the
     * Index systable; update them; initialize and load the new indexes.
     *
     * Its behavior is also affected by `g_test_no_index` and
     * `g_test_catcache_use_volatiletree`. See documents for those two global
     * variables for details.
     */
    void CheckIndexes(bool init);

    void BuildIndex(bool init, Oid idxid);

    /*!
     * Allocates an object ID. Returns a valid Oid that will never be returned
     * again unless it is deallocated on success. Otherwise, it returns
     * InvalidOid.
     */
    Oid AllocateOid();

    /*!
     * Returns the catalog entry in the systable by its table id and record id.
     * It searches in the cache first, returns that if found. Otherwise, it
     * reads the entry into the cache. Currently we do not evict any cached
     * catalog entry.
     */
    template<bool no_cache>
    typename std::conditional<no_cache,
                              std::unique_ptr<CCLookupTableEntry>,
                              CCLookupTableEntry*>::type
    GetOrCreateCachedEntry(Oid systabid,
                           RecordId recid,
                           const Schema *schema,
                           const char *buf);

    /*!
     * Inserts the new catalog entries in \p data into the systable \p systabid
     * and also update all its indexes.
     */
    void InsertCatalogEntries(Oid systabid,
                              const std::vector<std::vector<Datum>> &data);

    bool m_initialized;
    bool m_use_index;
    //absl::flat_hash_map<Oid, CCLookupTableEntry> m_oid_lookup_table;
    absl::flat_hash_map<RecordId, CCLookupTableEntry> m_recid_lookup_table;
    absl::flat_hash_map<Oid, std::shared_ptr<TableDesc>> m_table_desc;
    absl::flat_hash_map<Oid, std::shared_ptr<IndexDesc>> m_index_desc;
};

}   // namespace taco

#endif      // CATALOG_CATCACHEBASE_H
