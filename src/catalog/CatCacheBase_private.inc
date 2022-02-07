// No header guard intentionally. This must be included from some source file.
#ifndef CATALOG_CATCACHEBASE_H
#error "catalog/CatCacheBase.h must be included before CatCacheBase_private.h"
#endif

#include <absl/container/flat_hash_set.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/utility/utility.h>

#include "catalog/BootstrapCatCache.h"
#include "catalog/InitDataFileReader.h"
#include "catalog/systables/initoids.h"
#include "index/idxtyps.h"
#include "storage/FileManager.h"
#include "query/expr/optypes.h"
#include "utils/typsupp/varchar.h"
#include "utils/builtin_funcs.h"

namespace taco {

static bool heapfile_warn_rec_appear_twice = false;

/*!
 * The file ID of the database metadata file. This file is always allocated
 * through the derived catalog cache class's CreateCatalogFile() function,
 * and is assumed to have the file ID of 1.
 *
 * TODO if you change FileManager, make sure it reserves the file ID 1 for
 * the DB meta file.
 */
constexpr FileId DBMETA_FID = 1;
constexpr uint64_t DBMETA_MAGIC = 0xfe135724427531eful;

struct DBMetaPage {
    //! We include this so that `DBMetaPage' is compatible with the FileManager.
    PageHeaderData  m_ph;

    //! This should always be DBMETA_MAGIC.
    uint64_t        m_magic;

    /*!
     * The next OID for allocation.
     */
    atomic<Oid>     m_next_oid;

    /*!
     * The file ID of the system table `Table`.
     */
    FileId          m_systable_table_fid;
};
static_assert(sizeof(DBMetaPage) <= PAGE_SIZE);

template<class CatCacheCls>
CatCacheBase<CatCacheCls>::CatCacheBase():
    m_initialized(false),
    m_use_index(false) {
}

template<class CatCacheCls>
void
CatCacheBase<CatCacheCls>::CreateDBMeta() {
    FileId dbmeta_fid = ((CatCacheCls*)this)->CreateCatalogFile(false);
    if (dbmeta_fid != DBMETA_FID) {
        LOG(kFatal, "The first call to CreateCatalogFile in the "
                    "CatCache implementation must return 1 "
                    "as the file ID but we got " FILEID_FORMAT, dbmeta_fid);
    }

    auto fh = ((CatCacheCls*)this)->OpenCatalogFile(DBMETA_FID, nullptr);
    char *pagebuf;
    auto pghandle = ((CatCacheCls*)this)->GetFirstPage(fh, &pagebuf);
    DBMetaPage *dbmetapg = (DBMetaPage*) pagebuf;
    dbmetapg->m_magic = DBMETA_MAGIC;
    dbmetapg->m_next_oid.store(max_sys_oid + 1, memory_order_relaxed);

    // The file ID of systable Table will be updated to the allocated values in
    // the FinishInitCatalog() call.
    dbmetapg->m_systable_table_fid = INVALID_FID;
    ((CatCacheCls*)this)->MarkPageDirty(pghandle);
}

template<class CatCacheCls>
absl::flat_hash_map<Oid, FileId>
CatCacheBase<CatCacheCls>::LoadInitFile(
    const std::string &init_datafile,
    BootstrapCatCache *catcache) {

    InitDataFileReader reader(init_datafile, catcache);
    char *buf;
    FieldOffset buf_size;
    bool done = false;
    Oid tabid = InvalidOid;
    typename CatCacheCls::FileHandle fh;
    FileId fid = INVALID_FID;
    Record rec;
    absl::flat_hash_map<Oid, FileId> tabid2fid;
    for (;!done;) {
        switch (reader.next(&buf, &buf_size)) {
        case InitLineType::Eof:
            done = true;
            break;

        case InitLineType::Table:
            if (tabid != InvalidOid) {
                if (fid != INVALID_FID) {
                    ((CatCacheCls*)this)->CloseCatalogFile(fh);
                    fid = INVALID_FID;
                } else {
                    LOG(kFatal, "systable " OID_FORMAT " is empty in init.dat",
                        tabid);
                }
            }
            tabid = reader.LastTabid();
            break;

        case InitLineType::Data:
            ASSERT((FieldOffset) MAXALIGN(buf_size) == buf_size);

            if (fid == INVALID_FID) {
                fid = ((CatCacheCls*)this)->CreateCatalogFile(true);
                // Variable-length data page build never needs the second
                // argument for the tabdsec.
                fh = ((CatCacheCls*)this)->OpenCatalogFile(fid, nullptr);
                tabid2fid.emplace(tabid, fid);
            }

            rec.GetData() = buf;
            rec.GetLength() = buf_size;
            ((CatCacheCls*) this)->AppendRecord(fh, rec);
            break;
        case InitLineType::Error:
            LOG(kError, "parsing error in init data file on line %lu",
                    reader.LastLineNo());
        }
    }
    // fh should automatically close
    return tabid2fid;
}

template<class CatCacheCls>
void
CatCacheBase<CatCacheCls>::FinishInitCatalog(
    const absl::flat_hash_map<Oid, FileId> &tabid2fid,
    BootstrapCatCache *catcache) {

    // 1. Scan all Table entries to fix the tabfid.
    {
        FileId table_fid;
        auto map_iter = tabid2fid.find(initoids::TAB_Table);
        if (map_iter == tabid2fid.end()) {
            LOG(kFatal, "could not find the file ID of systable Table");
        }
        table_fid = map_iter->second;

        const TableDesc *table_tabdesc =
            catcache->FindTableDesc(initoids::TAB_Table);
        FieldId tabid_fieldid = SysTable_Table::tabid_colid();
        FieldId tabfid_fieldid = SysTable_Table::tabfid_colid();
        auto fh = ((CatCacheCls*)this)->OpenCatalogFile(table_fid,
                                                        table_tabdesc);
        auto fiter = ((CatCacheCls*)this)->IterateCatEntry(fh);

        // PAGE_SIZE is more than enough to avoid repeated memory allocation
        maxaligned_char_buf entry_buf;
        entry_buf.reserve(PAGE_SIZE);
        while (((CatCacheCls*)this)->NextCatEntry(fiter)) {
            const char *buf = ((CatCacheCls*)this)->GetCurrentCatEntry(fiter);

            // update the tabfid field
            std::vector<Datum> data =
                table_tabdesc->GetSchema()->DissemblePayload(buf);
            Oid tabid = data[tabid_fieldid].GetOid();
            FileId fid = data[tabfid_fieldid].GetUInt32();
            if (fid != INVALID_FID) {
                // Don't update the same entry twice, as updated record may
                // appear at the end of the iteration. This will prevent
                // infinite loop just in case the Table implementation performs
                // a deletion followed by a insertion, and the iterator does
                // prevent enumerating the new tuples as well.
                if (!heapfile_warn_rec_appear_twice) {
                    auto severity =
                        kFatal;
                    LOG(severity, "The same Table record appears again after "
                                  "Table::UpdateRecord() call. This should not "
                                  "happen if you update the record in place "
                                  "whenever possible.");
                    heapfile_warn_rec_appear_twice = true;
                }
                continue;
            }
            {
                auto map_iter = tabid2fid.find(tabid);
                if (map_iter == tabid2fid.end()) {
                    FieldId tabname_fieldid = table_tabdesc->GetSchema()
                        ->GetFieldIdFromFieldName("tabname");
                    absl::string_view &&tabname =
                        varchar_to_string_view(data[tabname_fieldid]);
                    LOG(kFatal, "coult not find the file ID of systable %s",
                                tabname);
                }
                fid = map_iter->second;
            }
            data[tabfid_fieldid] = Datum::From(fid);

            entry_buf.clear();
            if (-1 == table_tabdesc->GetSchema()
                        ->WritePayloadToBuffer(data, entry_buf)) {
                LOG(kFatal, "unable to write table entry back for table "
                            OID_FORMAT " %s", tabid,
                            varchar_to_string_view(
                                data[SysTable_Table::tabname_colid()]));
            }
            Record rec(entry_buf);
            ((CatCacheCls*)this)->UpdateCurrentCatEntry(fiter, rec);
        }
    }

    // 2. Write the minimal set of required file IDs to the meta page.
    {
        auto fh = ((CatCacheCls*) this)->OpenCatalogFile(DBMETA_FID, nullptr);
        char *pagebuf;
        auto pghandle = ((CatCacheCls*)this)->GetFirstPage(fh, &pagebuf);
        DBMetaPage *dbmetapg = (DBMetaPage*) pagebuf;

        // no need to check again for whether these systable tabids exist
        // in the map -- as those are already checked in step 1
        dbmetapg->m_systable_table_fid = tabid2fid.at(initoids::TAB_Table);
        ((CatCacheCls*) this)->MarkPageDirty(pghandle);
    }
}

template<class CatCacheCls>
void
CatCacheBase<CatCacheCls>::LoadMinCache(BootstrapCatCache *catcache) {
    // 1. Read systable Table's file ID from the meta page.
    FileId table_fid;
    {
        auto fh = ((CatCacheCls*) this)->OpenCatalogFile(DBMETA_FID, nullptr);
        char *pagebuf;
        auto pghandle = ((CatCacheCls*) this)->GetFirstPage(fh, &pagebuf);
        (void) pghandle; // suppress unused warning
        DBMetaPage *dbmetapg = (DBMetaPage*) pagebuf;
        if (dbmetapg->m_magic != DBMETA_MAGIC) {
            LOG(kFatal, "Incorrect magic in the DB meta page. "
                        "Database is probably corrupted.");
        }
        table_fid = dbmetapg->m_systable_table_fid;
    }

    // 2. Load the Table entry for Table, Type and Column into cache.
    // And create the cached table descriptors for them by copying
    // the schema object from the bootstrap catache.
    {
        // We still use the bootstrap catalog cache's descriptor here since
        // ours haven't been built yet.
        const TableDesc *table_tabdesc =
            catcache->FindTableDesc(initoids::TAB_Table);
        FieldId tabid_fieldid = SysTable_Table::tabid_colid();
        auto fh = ((CatCacheCls*) this)->OpenCatalogFile(table_fid,
                                                         table_tabdesc);
        auto fiter = ((CatCacheCls*) this)->IterateCatEntry(fh);
        int nfound = 0;
        // These systables should appear at the beginning of the heap file,
        // so we don't expect to scan more than a page here.
        while (((CatCacheCls*)this)->NextCatEntry(fiter)) {
            const char *buf = ((CatCacheCls*)this)->GetCurrentCatEntry(fiter);
            Oid tabid = table_tabdesc->GetSchema()
                ->GetField(tabid_fieldid, buf).GetOid();
            if (tabid == initoids::TAB_Table || tabid == initoids::TAB_Type ||
                tabid == initoids::TAB_Column) {
                // create the cached table entry
                RecordId recid =
                    ((CatCacheCls*)this)->GetCurrentCatEntryRecordId(fiter);
                auto tab = static_pointer_cast<const SysTable_Table>(
                    GetOrCreateCachedEntry<false>(
                        initoids::TAB_Table, recid, table_tabdesc->GetSchema(),
                        buf)->m_systable_struct);

                // create the table descriptor
                const TableDesc *old_tabdesc = catcache->FindTableDesc(tabid);
                auto schema =
                    absl::make_unique<Schema>(*old_tabdesc->GetSchema());
                std::shared_ptr<TableDesc> new_tabdesc(
                    TableDesc::Create(std::move(tab),
                                      std::move(schema)));
                m_table_desc.emplace(tabid, std::move(new_tabdesc));

                if (++nfound == 3) {
                    break;
                }
            }
        }
        ((CatCacheCls*)this)->EndIterateCatEntry(fiter);
    }

    // 3. Also collects the set of type IDs needed for Column, Type and Table
    // tables.
    absl::flat_hash_set<Oid> min_typids;
    {
        for (Oid tabid : {initoids::TAB_Table, initoids::TAB_Type,
                          initoids::TAB_Column}) {
            std::shared_ptr<const TableDesc> tabdesc = FindTableDesc(tabid);
            const Schema *schema = tabdesc->GetSchema();
            for (FieldId fieldid = 0; fieldid < schema->GetNumFields();
                    ++fieldid) {
                Oid typid = schema->GetFieldTypeId(fieldid);
                min_typids.emplace(typid);
            }
        }
    }

    // 4. Copy the minimal set of required types into the cache.
    {
        std::shared_ptr<const SysTable_Table> type_tabentry =
            FindTable(initoids::TAB_Type);
        FileId type_tabfid = type_tabentry->tabfid();
        const TableDesc *type_tabdesc =
            catcache->FindTableDesc(initoids::TAB_Type);
        FieldId typid_fieldid = SysTable_Type::typid_colid();
        auto fh = ((CatCacheCls*)this)->OpenCatalogFile(type_tabfid,
                                                        type_tabdesc);
        auto fiter = ((CatCacheCls*)this)->IterateCatEntry(fh);
        size_t nfound = 0;
        // again we should only scan the first page or two to find all the
        // types we need for initialization
        while (((CatCacheCls*)this)->NextCatEntry(fiter)) {
            const char *buf = ((CatCacheCls*)this)->GetCurrentCatEntry(fiter);
            Oid typid = type_tabdesc->GetSchema()
                ->GetField(typid_fieldid, buf).GetOid();
            if (min_typids.find(typid) != min_typids.end()) {
                RecordId recid =
                    ((CatCacheCls*)this)->GetCurrentCatEntryRecordId(fiter);
                (void) GetOrCreateCachedEntry<false>(
                    initoids::TAB_Type, recid, type_tabdesc->GetSchema(), buf);
                if (++nfound == min_typids.size()) {
                    break;
                }
            }
        }
        ((CatCacheCls*)this)->EndIterateCatEntry(fiter);
    }

    // mark the initialization as done
    m_initialized = true;
}

template<class CatCacheCls>
void
CatCacheBase<CatCacheCls>::CheckIndexes(bool init) {
    if (g_test_no_index) {
        // m_use_index should be false at construction
        // let's make sure of that here
        m_use_index = false;
        return ;
    }

    LOG(kFatal, "index support not available yet");
}

template<class CatCacheCls>
void
CatCacheBase<CatCacheCls>::BuildIndex(bool init, Oid idxid) {
    LOG(kFatal, "index support not available yet");
}

template<class CatCacheCls>
void
CatCacheBase<CatCacheCls>::InitializeFromInitData(
    const std::string &init_datafile) {
    // creates and initializes the bootstrap catalog cache
    BootstrapCatCache catcache;
    catcache.Init();

    // creates the database meta data file
    CreateDBMeta();

    // loads the catalog data from the init file
    auto tabid2fid = LoadInitFile(init_datafile, &catcache);

    // complete all the post-processing needed for initialize the catalog
    FinishInitCatalog(tabid2fid, &catcache);

    // initialization of the cache
    LoadMinCache(&catcache);

    // check the catalog indexes and initialize them if necessary
    CheckIndexes(true);
}


template<class CatCacheCls>
void
CatCacheBase<CatCacheCls>::InitializeFromExistingData() {
    BootstrapCatCache catcache;
    catcache.Init();

    // initialization of the cache
    LoadMinCache(&catcache);

    // check the catalog indexes if they need to be built upon restart
    CheckIndexes(false);
}

template<class CatCacheCls>
std::shared_ptr<const TableDesc>
CatCacheBase<CatCacheCls>::FindTableDesc(Oid tabid) {
    auto iter = m_table_desc.find(tabid);
    if (iter != m_table_desc.end()) {
        return iter->second;
    }

    // Column is the only table that we should have loaded in LoadMinCache()
    // rather than here, since that is needed for collecting all the column
    // information. This would prevent any infinite recursion should we somehow
    // failed to create the table descriptor for Column during initialization.
    ASSERT(tabid != initoids::TAB_Column,
           "Column systable descriptor should have been built during "
           "catalog cache initialization");

    ASSERT(m_initialized);
    // Not found in cache. Does this table exist at all?
    std::shared_ptr<const SysTable_Table> table = FindTable(tabid);
    if (!table) {
        // No, it doesn't exist yet
        return nullptr;
    }

    // Otherwise, this table exists and we need to create a new cached
    // TableDesc for it.

    auto cols =
        SearchForCatalogEntry<false, 1, true>::Call(
            this, initoids::TAB_Column,
            // we would like to have sorted column ids
            initoids::IDX_Column_coltabid_colid,
            table->tabncols(),
            {SysTable_Column::coltabid_colid()}, {initoids::FUNC_OID_eq},
            tabid);

    if (cols.size() != (size_t) table->tabncols()) {
        LOG(kFatal, "the number of entries in Column table does not match "
                    "tabncols for table " OID_FORMAT, ", got %lu, "
                    "expecting %lu", tabid, cols.size(),
                    (size_t) table->tabncols());
    }

    std::vector<const SysTable_Column*> colptrs;
    colptrs.reserve(cols.size());
    for (const std::unique_ptr<CCLookupTableEntry> &entry: cols) {
        colptrs.push_back(
            (const SysTable_Column*) entry->m_systable_struct.get());
    }

    // they should have been sorted if we're using index
    if (!m_use_index)
        std::sort(colptrs.begin(), colptrs.end(),
            [](const SysTable_Column *a, const SysTable_Column *b) -> bool {
                return a->colid() < b->colid();
            });

    std::shared_ptr<TableDesc> tabdesc(TableDesc::Create(table, colptrs));
    if (!tabdesc)
        LOG(kFatal, "unable to create table desc for table %s",
                    table->tabname());
    m_table_desc.emplace(table->tabid(), tabdesc);
    return tabdesc;
}

template<class CatCacheCls>
std::shared_ptr<const IndexDesc>
CatCacheBase<CatCacheCls>::FindIndexDesc(Oid idxid) {
    auto iter = m_index_desc.find(idxid);
    if (iter != m_index_desc.end()) {
        return iter->second;
    }

    // Different from FindTableDesc, this is also called when m_use_index is
    // false, which indicates we're initializing/checking the indexes during
    // catalog cache initialization/loading. The caller should guanratee
    // that any index that may be used already has their index descriptors
    // cached when m_use_index == true.
    std::shared_ptr<const SysTable_Index> index = FindIndex(idxid);
    if (!index) {
        // the index does not exist
        return nullptr;
    }

    // collect the index columns
    auto idxcol_entries =
        SearchForCatalogEntry<false, 1, true>::Call(
            this, initoids::TAB_IndexColumn,
            // we would like to have sorted index column ids
            initoids::IDX_IndexColumn_idxcolidxid_idxcolid,
            index->idxncols(),
            {SysTable_IndexColumn::idxcolidxid_colid()},
            {initoids::FUNC_OID_eq},
            idxid);
    if (idxcol_entries.size() != (size_t) index->idxncols()) {
        LOG(kFatal, "the number of entries in IndexColumn table does not match "
                    "idxncols for index " OID_FORMAT", got %lu, "
                    "expecting %lu", idxid, idxcol_entries.size(),
                    (size_t) index->idxncols());
    }

    std::vector<std::shared_ptr<SysTable_IndexColumn>> idxcolptrs;
    idxcolptrs.reserve(index->idxncols());
    for (std::unique_ptr<CCLookupTableEntry> &entry: idxcol_entries) {
        idxcolptrs.emplace_back(static_pointer_cast<SysTable_IndexColumn>(
            entry->m_systable_struct));
    }
    idxcol_entries.clear();

    if (!m_use_index) {
        std::sort(idxcolptrs.begin(), idxcolptrs.end(),
            [](std::shared_ptr<SysTable_IndexColumn> &a,
               std::shared_ptr<SysTable_IndexColumn> &b) -> bool {
                return a->idxcolid() < b->idxcolid();
           });
    }

    // construct the key schema
    std::vector<Oid> typid;
    std::vector<uint64_t> typparam;
    std::vector<bool> isnullable;
    typid.reserve(index->idxncols());
    typparam.reserve(index->idxncols());
    isnullable.reserve(index->idxncols());
    for (std::shared_ptr<SysTable_IndexColumn> &idxcol: idxcolptrs) {
        typid.push_back(idxcol->idxcoltypid());
        typparam.push_back(idxcol->idxcoltypparam());
        isnullable.push_back(idxcol->idxcolisnullable());
    }
    std::unique_ptr<Schema> key_schema =
        absl::WrapUnique(Schema::Create(typid, typparam, isnullable));
    key_schema->ComputeLayout();

    // create the index descriptor
    std::shared_ptr<IndexDesc> indexdesc(
        IndexDesc::Create(std::move(index), std::move(idxcolptrs),
                          std::move(key_schema)));
    // put it into cache
    m_index_desc.emplace(idxid, indexdesc);
    return indexdesc;
}

template<class CatCacheCls>
Oid
CatCacheBase<CatCacheCls>::FindOperator(OpType optype,
                                        Oid oparg0typid,
                                        Oid oparg1typid) {
    if (OpTypeIsUnary(optype)) {
        if (oparg0typid == InvalidOid) {
            LOG(kError, "missing operand type for unary op %d(\"%s\")",
                        (int) optype, GetOpTypeSymbol(optype));
        }
    } else {
        ASSERT(OpTypeIsBinary(optype));
        if (oparg0typid == InvalidOid || oparg1typid == InvalidOid) {
            LOG(kError, "missing operand type for binary op %d(\"%s\")",
                        (int) optype, GetOpTypeSymbol(optype));
        }
    }
    auto entry = SearchForCatalogEntry<true, 3, false>::Call(
        this, initoids::TAB_Operator,
        initoids::IDX_Operator_optype_oparg0typid_oparg1typid,
        {SysTable_Operator::optype_colid(),
         SysTable_Operator::oparg0typid_colid(),
         SysTable_Operator::oparg1typid_colid()},
        {initoids::FUNC_UINT1_eq,
         initoids::FUNC_OID_eq,
         initoids::FUNC_OID_eq},
        optype, oparg0typid, oparg1typid);
    if (!entry) {
        return InvalidOid;
    }
    return ((SysTable_Operator*)(entry->m_systable_struct.get()))->opfuncid();
}

template<class CatCacheCls>
Oid
CatCacheBase<CatCacheCls>::AddTable(absl::string_view tabname,
                                    std::vector<Oid> coltypid,
                                    std::vector<uint64_t> coltypparam,
                                    std::vector<std::string> field_names,
                                    std::vector<bool> colisnullable,
                                    std::vector<bool> colisarray,
                                    FileId tabfid) {
    FieldId num_fields = (FieldId) coltypid.size();

    if (tabfid == INVALID_FID) {
        LOG(kError, "no valid data page allocated for the new table");
    }

    if (coltypid.size() == 0) {
        LOG(kError, "no column specified in a new table");
    }
    if (coltypid.size() > 0xffff) {
        LOG(kError, "too many columns specified in a new table");
    }

    if (coltypparam.empty()) {
        coltypparam.resize(num_fields, 0);
    } else if ((FieldId) coltypparam.size() != num_fields) {
        LOG(kError, "different number of type parameters than the number of "
                "type IDs given in a new table");
    }

    if (field_names.empty()) {
        for (FieldId i = 0; i < num_fields; ++i) {
            field_names.emplace_back(absl::StrCat("col_", i));
        }
    } else if ((FieldId) field_names.size() != num_fields) {
        LOG(kError, "different number of field names than the number of "
                "type IDs given in a new table");
    }

    if (colisnullable.empty()) {
        colisnullable.resize(num_fields, true);
    } else if ((FieldId) colisnullable.size() != num_fields) {
        LOG(kError, "different number of isnullable values than the number of "
                "type IDs given in a new table");
    }

    if (colisarray.empty()) {
        colisarray.resize(num_fields, false);
    } else if ((FieldId) colisarray.size() != num_fields) {
        LOG(kError, "different number of isarray values than the number of "
                "type IDs given in a new table");
    }

    bool tabisvarlen = false;

    for (FieldId i = 0; i < num_fields; ++i) {
        std::shared_ptr<const SysTable_Type> typ = FindType(coltypid[i]);
        if (!typ) {
            LOG(kError, "type " OID_FORMAT " not found", coltypid[i]);
        }
    }

    tabisvarlen = true;

    // get a new Oid
    Oid tabid = AllocateOid();
    if (tabid == InvalidOid)
        LOG(kError, "run out of valid Oid");

    // create the Table entry
    // NOTE: this must be kept in sync with catalog/systables/Table.inc
    std::unique_ptr<SysTable_Table> table = absl::WrapUnique(
        ConstructSysTableStruct<SysTable_Table>(
            tabid,
            /* tabissys = */ false,
            tabisvarlen,
            /*tabncols=*/ num_fields,
            tabfid,
            cast_as_string(tabname)));
    ASSERT(table.get());
    if (!table)
        LOG(kFatal, "unable to create new Table catalog entry");
    std::vector<std::vector<Datum>> table_data;
    table_data.emplace_back(GetDatumVector(table));

    std::vector<std::unique_ptr<SysTable_Column>> column;
    std::vector<std::vector<Datum>> column_data;
    column.reserve(num_fields);
    column_data.reserve(num_fields);
    for (FieldId i = 0; i < num_fields; ++i) {
        column.emplace_back(absl::WrapUnique(
            ConstructSysTableStruct<SysTable_Column>(
                /*coltabid = */tabid,
                /*colid = */i,
                colisarray[i],
                colisnullable[i],
                coltypid[i],
                coltypparam[i],
                /*colname = */std::move(field_names[i]))));
        ASSERT(column.back().get());
        column_data.emplace_back(GetDatumVector(column.back()));
    }

    InsertCatalogEntries(initoids::TAB_Table, table_data);
    InsertCatalogEntries(initoids::TAB_Column, column_data);

    return tabid;
}

template<class CatCacheCls>
Oid
CatCacheBase<CatCacheCls>::AddIndex(absl::string_view idxname,
                                    Oid idxtabid,
                                    IdxType idxtyp,
                                    bool idxunique,
                                    std::vector<FieldId> idxcoltabcolids,
                                    FileId idxfid,
                                    std::vector<Oid> idxcolltfuncids,
                                    std::vector<Oid> idxcoleqfuncids) {
    std::shared_ptr<const TableDesc> tabdesc = FindTableDesc(idxtabid);
    if (!tabdesc) {
        LOG(kError, "table " OID_FORMAT " does not exist", idxtabid);
    }

    // create default index name if not given
    std::string idxname_;
    if (idxname.empty()) {
        std::vector<absl::string_view> idxname_comp;
        idxname_comp.reserve(tabdesc->GetTableEntry()->tabncols() + 1);
        idxname_comp.emplace_back(tabdesc->GetTableEntry()->tabname());
        for (FieldId i = 0; i < tabdesc->GetSchema()->GetNumFields(); ++i) {
            idxname_comp.emplace_back(tabdesc->GetSchema()->GetFieldName(i));
        }

        idxname_ = absl::StrJoin(idxname_comp, "_");
        idxname = idxname_;
    }

    if (!IdxTypeIsValid(idxtyp)) {
        LOG(kError, "unknown index type %d", (int) idxtyp);
    }

    if (IdxTypeIsVolatile(idxtyp)) {
        if (idxfid != INVALID_FID) {
            LOG(kError, "volatile (in-memory) index should not have a file");
        }
    } else {
        if (idxfid == INVALID_FID) {
            LOG(kError, "index needs a valid file");
        }
    }

    if (idxcoltabcolids.size() > (size_t) std::numeric_limits<FieldId>::max()) {
        LOG(kError, "too many key columns");
    }
    FieldId idxncols = (FieldId) idxcoltabcolids.size();

    // collect the index column info
    std::vector<Oid> idxcoltypids;
    idxcoltypids.reserve(idxncols);
    std::vector<bool> idxcolisnullable;
    idxcolisnullable.reserve(idxncols);
    std::vector<uint64_t> idxcoltypparams;
    idxcoltypparams.reserve(idxncols);

    // these may have user supplied function ids
    idxcoleqfuncids.resize(idxncols, InvalidOid);
    idxcolltfuncids.resize(idxncols, InvalidOid);

    const Schema *tabschema = tabdesc->GetSchema();
    for (FieldId i = 0; i < idxncols; ++i) {
        FieldId idxcoltabcolid = idxcoltabcolids[i];
        if (idxcoltabcolid >= tabschema->GetNumFields()) {
            LOG(kError, "key field " FIELDID_FORMAT " is out of bound [0, "
                        FIELDID_FORMAT, ")", idxcoltabcolid,
                        tabschema->GetNumFields());
        }

        Oid idxcoltypid = tabschema->GetFieldTypeId(idxcoltabcolid);
        idxcoltypids.push_back(idxcoltypid);
        idxcolisnullable.push_back(tabschema->FieldIsNullable(idxcoltabcolid));
        idxcoltypparams.push_back(tabschema->GetFieldTypeParam(idxcoltabcolid));

        // find the operators
        Oid eqfuncid = idxcoleqfuncids[i];
        if (IdxTypeNeedsEqualOperator(idxtyp)) {
            if (eqfuncid == InvalidOid) {
                eqfuncid = FindOperator(OPTYPE(EQ), idxcoltypid, idxcoltypid);
            }
            if (eqfuncid == InvalidOid) {
                LOG(kError, "can't find \"=\" operator for type " OID_FORMAT
                            " required by index type %s", idxcoltypid,
                            IdxTypeGetName(idxtyp));
            }
        }
        idxcoleqfuncids[i] = eqfuncid;

        Oid ltfuncid = idxcolltfuncids[i];
        if (IdxTypeNeedsLessOperator(idxtyp)) {
            if (ltfuncid == InvalidOid) {
                ltfuncid = FindOperator(OPTYPE(LT), idxcoltypid, idxcoltypid);
            }
            if (ltfuncid == InvalidOid) {
                LOG(kError, "can't find \"<\" operator for type " OID_FORMAT
                            " required by index type %s", idxcoltypid,
                            IdxTypeGetName(idxtyp));
            }
        }
        idxcolltfuncids[i] = ltfuncid;

        // check the two operator function argument types
        for (Oid opfuncid : { eqfuncid, ltfuncid }) {
            if (opfuncid == InvalidOid)
                continue;
            std::shared_ptr<const SysTable_Function> function =
                FindFunction(opfuncid);
            if (!function || function->funcnargs() != 2 ||
                function->funcrettypid() != initoids::TYP_BOOL) {
                LOG(kError, "function %s is not a valid \"%s\" operator for "
                            "type " OID_FORMAT,
                            function->funcname(),
                            (opfuncid == eqfuncid) ? "=" : "<",
                            idxcoltypid);
            }

            for (int16_t funcargid = 0; funcargid < 2; ++funcargid) {
                std::shared_ptr<const SysTable_FunctionArgs> functionargs =
                    FindFunctionArgs(opfuncid, funcargid);
                if (!functionargs ||
                    functionargs->funcargtypid() != idxcoltypid) {
                    LOG(kError, "function %s is not a valid \"%s\" operator for "
                                "type " OID_FORMAT,
                                function->funcname(),
                                (opfuncid == eqfuncid) ? "=" : "<",
                                idxcoltypid);
                }
            }
        }
    }

    // XXX probably should check for the uniqueness of index name before
    // attempting to do any catalog update as that will waste a file and an oid
    // without txn rollback.

    Oid idxid = AllocateOid();
    if (idxid == InvalidOid) {
        LOG(kError, "run out of valid Oid");
    }

    std::unique_ptr<SysTable_Index> idx = absl::WrapUnique(
        ConstructSysTableStruct<SysTable_Index>(
            idxid,
            idxtabid,
            idxtyp,
            idxunique,
            idxncols,
            idxfid,
            cast_as_string(idxname)));
    ASSERT(idx.get());
    std::vector<std::vector<Datum>> idx_data;
    idx_data.emplace_back(GetDatumVector(idx));

    std::vector<std::unique_ptr<SysTable_IndexColumn>> idxcol;
    idxcol.reserve(idxncols);
    std::vector<std::vector<Datum>> idxcol_data;
    idxcol_data.reserve(idxncols);
    for (FieldId i = 0; i < idxncols; ++i) {
        idxcol.emplace_back(absl::WrapUnique(
            ConstructSysTableStruct<SysTable_IndexColumn>(
                /*idxcolidxid=*/ idxid,
                /*idxcolid=*/ i,
                idxcoltabcolids[i],
                idxcoltypids[i],
                idxcolisnullable[i],
                idxcoltypparams[i],
                idxcoleqfuncids[i],
                idxcolltfuncids[i])));
        ASSERT(idxcol.back().get());
        idxcol_data.emplace_back(GetDatumVector(idxcol.back()));
    }

    InsertCatalogEntries(initoids::TAB_Index, idx_data);
    InsertCatalogEntries(initoids::TAB_IndexColumn, idxcol_data);

    return idxid;
}

template<class CatCacheCls>
Oid
CatCacheBase<CatCacheCls>::AllocateOid() {
    auto fh = ((CatCacheCls*)this)->OpenCatalogFile(DBMETA_FID, nullptr);
    char *pagebuf;
    auto pghandle = ((CatCacheCls*)this)->GetFirstPage(fh, &pagebuf);
    DBMetaPage *dbmetapg = (DBMetaPage*) pagebuf;

    Oid next_oid = dbmetapg->m_next_oid.load(memory_order_relaxed);
    for (;;) {
        if (next_oid > MaxOid) {
            return InvalidOid;
        }
        if (dbmetapg->m_next_oid.compare_exchange_strong(
                next_oid, next_oid + 1, memory_order_relaxed)) {
            ((CatCacheCls*)this)->MarkPageDirty(pghandle);
            return next_oid;
        }
    }
}

namespace get_or_create_cached_entry_impl {

template<bool wrap_unique = false>
struct WrapUnique {
    template<class T>
    static inline T* r(T* t) {
        return t;
    }
};

template<>
struct WrapUnique<true> {
    template<class T>
    static inline std::unique_ptr<T> r(T* t) {
        return absl::WrapUnique(t);
    }
};

}   // namespace get_or_create_cached_entry_impl

template<class CatCacheCls>
template<bool no_cache>
typename std::conditional<no_cache,
                          std::unique_ptr<CCLookupTableEntry>,
                          CCLookupTableEntry*>::type
CatCacheBase<CatCacheCls>::GetOrCreateCachedEntry(
    Oid systabid,
    RecordId recid,
    const Schema *schema,
    const char *buf) {
    if (!no_cache) {
        auto iter = m_recid_lookup_table.find(recid);
        if (iter != m_recid_lookup_table.end()) {
            return get_or_create_cached_entry_impl::WrapUnique<no_cache>
                    ::r(&iter->second);
        }
    }

    std::shared_ptr<void> systable_struct;
    if (buf) {
        ASSERT(schema);
        std::vector<Datum> data = schema->DissemblePayload(buf);
        systable_struct = CreateSysTableStruct(systabid, data);
    } else {
        std::shared_ptr<const TableDesc> tabdesc = FindTableDesc(systabid);
        FileId tabfid = tabdesc->GetTableEntry()->tabfid();

        // this may get called when the initialization has not finished,
        // so pass the tabdesc to OpenCatalogFile just to be safe
        auto fh = ((CatCacheCls*)this)->OpenCatalogFile(tabfid, tabdesc.get());
        auto fiter = ((CatCacheCls*)this)->IterateCatEntryFrom(fh, recid);
        if (!((CatCacheCls*)this)->NextCatEntry(fiter)) {
            LOG(kFatal, "record %s not found in systable %s",
                        recid.ToString(), tabdesc->GetTableEntry()->tabname());
        }
        const char *buf = ((CatCacheCls*)this)->GetCurrentCatEntry(fiter);
        std::vector<Datum> data = schema->DissemblePayload(buf);
        systable_struct = CreateSysTableStruct(systabid, data);
        ((CatCacheCls*)this)->EndIterateCatEntry(fiter);
    }

    if (!no_cache) {
        m_recid_lookup_table.emplace(
            recid,
            CCLookupTableEntry(recid, std::move(systable_struct)));
        return get_or_create_cached_entry_impl::WrapUnique<no_cache>::r(
            &m_recid_lookup_table.at(recid));
    } else {
        return get_or_create_cached_entry_impl::WrapUnique<no_cache>::r(
            new CCLookupTableEntry(recid, std::move(systable_struct)));
    }
}

namespace search_for_catalog_entry_impl {

template<class RHS>
typename std::enable_if<
    !std::is_same<typename std::decay<RHS>::type, absl::string_view>::value &&
    !std::is_same<typename std::decay<RHS>::type, std::string>::value &&
    !std::is_same<typename std::decay<RHS>::type, char*>::value,
    Datum>::type
ToDatum(RHS &&rhs) {
    return Datum::From(rhs);
}

template<class RHS>
typename std::enable_if<
    std::is_same<typename std::decay<RHS>::type, absl::string_view>::value ||
    std::is_same<typename std::decay<RHS>::type, std::string>::value,
    Datum>::type
ToDatum(RHS &&rhs) {
    return Datum::FromVarlenAsStringView(rhs);
}

template<class RHS>
typename std::enable_if<
    std::is_same<typename std::decay<RHS>::type, char*>::value,
    Datum>::type
ToDatum(RHS &&rhs) {
    return Datum::FromCString(rhs);
}

template<size_t NPreds, size_t... I>
std::array<FunctionInfo, NPreds>
FuncIdArrayToFunctionInfoArray(std::array<Oid, NPreds> eq_funcid,
                               absl::integer_sequence<size_t, I...>) {
    return {{ (FindBuiltinFunction(eq_funcid[I]))... }};
}


bool
CheckIfEqual(const char *recbuf,
             const Schema *schema,
             FieldId N,
             const FieldId *fieldid,
             const Datum *rhs_datum,
             const FunctionInfo *eq_func) {

    for (FieldId i = 0; i < N; ++i) {
        Datum lhs_datum = schema->GetField(fieldid[i], recbuf);
        Datum res = FunctionCall(eq_func[i], lhs_datum, rhs_datum[i]);
        ASSERT(!res.isnull(), "null result for predicate %lu", i);
        if (!res.GetBool()) {
            return false;
        }
    }
    return true;
}

template<class T, class U>
typename std::enable_if<std::is_same<T, std::vector<U>>::value>::type
OptionalEmplaceBack(T &v, U &&u) {
    v.emplace_back(std::forward<U>(u));
}

template<class T, class U>
typename std::enable_if<!std::is_same<T, std::vector<U>>::value>::type
OptionalEmplaceBack(T &v, U &&u) {}


template<class T, class = void>
struct IsSomeVector {
    constexpr static const bool value = false;
};

template<class T>
struct IsSomeVector<T, typename std::enable_if<
    std::is_same<T, std::vector<typename T::value_type>>::value>::type> {
    constexpr static const bool value = true;
};

template<class T>
typename std::enable_if<IsSomeVector<T>::value>::type
OptionalReserve(T &v, size_t capacity) {
    v.reserve(capacity);
}

template<class T>
typename std::enable_if<!IsSomeVector<T>::value>::type
OptionalReserve(T &v, size_t capacity) {}

template<class T>
typename std::enable_if<IsSomeVector<T>::value, size_t>::type
OptionalSize(T &v) {
    return v.size();
}

template<class T>
typename std::enable_if<!IsSomeVector<T>::value, size_t>::type
OptionalSize(T &v) {
    return 0;
}

template<bool forward_input=true>
struct ReturnInputOrVector{
    template<class T>
    static inline T r(T&& t) {
        return t;
    }
};

template<>
struct ReturnInputOrVector<false> {
    template<class T>
    static inline std::vector<T> r(T&&) {
        return {};
    }
};

} // namespace search_for_catalog_entry_impl

template<class CatCacheCls>
template<bool expect_unique, size_t NPreds, bool no_cache>
template<class ...RHS>
typename CatCacheBase<CatCacheCls>
    ::template SearchForCatalogEntry<expect_unique, NPreds, no_cache>::RetType
CatCacheBase<CatCacheCls>
    ::SearchForCatalogEntry<expect_unique, NPreds, no_cache>
    ::Impl(CatCacheBase<CatCacheCls> *_this,
           Oid systabid,
           Oid idxid_hint,
           size_t expect_n,
           const std::array<FieldId, NPreds> &fieldid,
           const std::array<Oid, NPreds> &eq_funcid,
           RHS&& ...rhs) {
    ASSERT(!expect_unique || expect_n <= 1);

    typename std::conditional<expect_unique, nullptr_t, RetType>::type ret;
    search_for_catalog_entry_impl::OptionalReserve(ret, expect_n);

    std::array<Datum, NPreds> rhs_datum {{
        (search_for_catalog_entry_impl::ToDatum(rhs))...
    }};

    std::array<FunctionInfo, NPreds> eq_func(
        search_for_catalog_entry_impl::FuncIdArrayToFunctionInfoArray(
            eq_funcid,
            absl::make_integer_sequence<size_t, NPreds>()));

    std::shared_ptr<const TableDesc> tabdesc = _this->FindTableDesc(systabid);
    const Schema *schema = tabdesc->GetSchema();
    FileId tabfid = tabdesc->GetTableEntry()->tabfid();

    if (_this->m_use_index) {
        LOG(kFatal, "index support not available yet");
    }

    // if not using index, defaults to the slower table scan
    auto fh = ((CatCacheCls*) _this)->OpenCatalogFile(tabfid, tabdesc.get());
    auto fiter = ((CatCacheCls*) _this)->IterateCatEntry(fh);

    while (((CatCacheCls*) _this)->NextCatEntry(fiter)) {
        const char *recbuf = ((CatCacheCls*) _this)->GetCurrentCatEntry(fiter);

        if (search_for_catalog_entry_impl::CheckIfEqual(
                recbuf, schema, (FieldId) NPreds,
                fieldid.data(), rhs_datum.data(), eq_func.data())){
            RecordId recid =
                ((CatCacheCls*) _this)->GetCurrentCatEntryRecordId(fiter);
            if (expect_unique) {
                auto ret = _this->GetOrCreateCachedEntry<no_cache>(
                    systabid, recid, schema, recbuf);
                return search_for_catalog_entry_impl
                    ::ReturnInputOrVector<expect_unique>
                    ::r(std::move(ret));
            } else {
                //XXX if constexpr is only available from c++17. That will
                // allow us to just call emplace_back on ret here.
                search_for_catalog_entry_impl::OptionalEmplaceBack(
                    ret, _this->GetOrCreateCachedEntry<no_cache>(
                        systabid, recid, schema, recbuf));
                // if the caller knows how many to expect, we can break early
                // when we find the first `expect_n` entries.
                if (expect_n != 0 &&
                    search_for_catalog_entry_impl::OptionalSize(ret)
                        == expect_n) {
                    break;
                }
            }
        }
    }

    if (expect_unique)
        return search_for_catalog_entry_impl
            ::ReturnInputOrVector<expect_unique>
            ::r(EntryPtr(nullptr));
    else
        return ret;
}

template<class CatCacheCls>
void
CatCacheBase<CatCacheCls>::InsertCatalogEntries(
    Oid systabid,
    const std::vector<std::vector<Datum>> &data) {

    // find the table descriptor and open the systable
    std::shared_ptr<const TableDesc> tabdesc = FindTableDesc(systabid);
    ASSERT(tabdesc.get());
    const Schema *schema = tabdesc->GetSchema();
    FileId tabfid = tabdesc->GetTableEntry()->tabfid();
    auto tabfh = ((CatCacheCls*)this)->OpenCatalogFile(tabfid, tabdesc.get());

    // insert the records and update the indexes
    maxaligned_char_buf recbuf;
    recbuf.reserve(128);
    for (const std::vector<Datum> & recdata: data) {
        recbuf.clear();
        if (-1 == schema->WritePayloadToBuffer(recdata, recbuf)) {
            LOG(kFatal, "unable to write table entry in systable %s",
                        tabdesc->GetTableEntry()->tabname());
        }
        Record rec(recbuf);
        // table insert
        ((CatCacheCls*)this)->AppendRecord(tabfh, rec);
    }
    ((CatCacheCls*)this)->CloseCatalogFile(tabfh);
}

}   // namespace taco

