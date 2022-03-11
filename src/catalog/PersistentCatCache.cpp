#include "catalog/PersistentCatCache.h"

#include "catalog/systables/bootstrap_data.h"
#include "storage/FileManager.h"
#include "storage/Table.h"

// include the private implementations of the CatCacheBase
#include "CatCacheBase_private.inc"

namespace taco {

// explicit instantiation of the base class of PersistentCatCache
template class CatCacheBase<PersistentCatCache>;

PersistentCatCache::PersistentCatCache():
    m_dbmetapg_pid(INVALID_PID) {}

FileId
PersistentCatCache::CreateCatalogFile(bool format_heapfile) {
    ASSERT(!IsInitialized());
    std::unique_ptr<File> f = g_fileman->Open(NEW_REGULAR_FID);

    if (format_heapfile) {
        // create a fake table descriptor with the tabfid, tabissys and
        // tabisvarlen set in the table entry which is the minimum required
        // things in the catalog cache.
        std::shared_ptr<SysTable_Table> tabentry(
            ConstructSysTableStruct<SysTable_Table>(
                /*tabid = */InvalidOid,
                /*tabissys = */true,
                /*tabisvarlen = */true,
                /*tabisncols = */0,
                /*tabfid = */f->GetFileId(),
                /*tabname = */""));
        std::unique_ptr<Schema> sch =
            // we don't need the schema for variable-length data page build
            nullptr;
        TableDesc *tabdesc = TableDesc::Create(std::move(tabentry),
                                               std::move(sch));
        Table::Initialize(tabdesc);
    } else {
        // For the db meta file, we cache its first page number and directly go
        // to the buffer manager to pin the page. We won't open the file again
        // later on.
        if (m_dbmetapg_pid != INVALID_PID) {
            LOG(kFatal, "PersistentCatCache does not support more than "
                        "1 non-heapfile");
        }
        if (f->GetFileId() != DBMETA_FID) {
            LOG(kFatal, "the non-heapfile created by PersistentCatCache must "
                        " be the first file ever created");
        }
        m_dbmetapg_pid = f->GetFirstPageNumber();
    }

    return f->GetFileId();
}

PersistentCatCache::FileHandle
PersistentCatCache::OpenCatalogFile(FileId fid, const TableDesc *tabdesc) {
    if (!IsInitialized()) {
        if (fid == DBMETA_FID && m_dbmetapg_pid == INVALID_PID) {
            // First time load the db meta page, we need to open the file
            // and cache the first page number.
            auto f = g_fileman->Open(DBMETA_FID);
            m_dbmetapg_pid = f->GetFirstPageNumber();
        }

        // We may not be able to create the Table object here for heap files if
        // the catalog cache has not finished its initialization, as we don't
        // know the length of the Schema for variable-length
        // This only applies to opening files during initialization.
        FileHandle fh{fid, nullptr};
        _CreateTableIfNullForFileHandle(fh, 0);
        return fh;
    }

    // DB meta file is not a heap file, so we don't create a Table object
    // either.
    if (fid == DBMETA_FID) {
        return FileHandle{fid, nullptr};
    }

    ASSERT(tabdesc);
    Oid tabid = tabdesc->GetTableEntry()->tabid();

    // even if we're passed a table descriptor, we still need one with
    // ownership.
    std::shared_ptr<const TableDesc> tabdesc_ = FindTableDesc(tabid);
    ASSERT(tabdesc_.get());
    return FileHandle{fid, Table::Create(std::move(tabdesc_))};
}

void
PersistentCatCache::CloseCatalogFile(FileHandle &fh) {
    fh.m_fid = INVALID_FID;
    fh.m_table = nullptr;
}

PersistentCatCache::PageHandle
PersistentCatCache::GetFirstPage(FileHandle &fh, char **pagebuf) {
    if (fh.m_fid != DBMETA_FID) {
        LOG(kFatal, "can't access a heapfile catalog file as a non-heapfile in "
                    "PersistentCatCache");
    }
    // implicit conversion from BufferId to ScopedBufferId
    return g_bufman->PinPage(m_dbmetapg_pid, pagebuf);
}

void
PersistentCatCache::MarkPageDirty(PageHandle &pghandle) {
    g_bufman->MarkDirty(pghandle);
}

void
PersistentCatCache::ReleasePage(PageHandle &pghandle) {
    g_bufman->UnpinPage(pghandle);
}

void
PersistentCatCache::AppendRecord(FileHandle &fh, Record &rec) {
    ASSERT(fh.m_table.get());
    fh.m_table->InsertRecord(rec);
}

PersistentCatCache::CatFileIterator
PersistentCatCache::IterateCatEntry(FileHandle &fh) {
    ASSERT(fh.m_table.get());
    return fh.m_table->StartScan();
}

PersistentCatCache::CatFileIterator
PersistentCatCache::IterateCatEntryFrom(FileHandle &fh, RecordId rid) {
    ASSERT(fh.m_table.get());
    return fh.m_table->StartScanFrom(rid);
}

bool
PersistentCatCache::NextCatEntry(CatFileIterator &iter) {
    return iter.Next();
}

const char*
PersistentCatCache::GetCurrentCatEntry(CatFileIterator &iter) {
    return iter.GetCurrentRecord().GetData();
}

RecordId
PersistentCatCache::GetCurrentCatEntryRecordId(CatFileIterator &iter) {
    return iter.GetCurrentRecordId();
}

void
PersistentCatCache::UpdateCurrentCatEntry(CatFileIterator &iter, Record &rec) {
    Table *table = iter.GetTable();
    table->UpdateRecord(iter.GetCurrentRecordId(), rec);
}

void
PersistentCatCache::EndIterateCatEntry(CatFileIterator &iter) {
    // just invalidates the iterator
    iter.EndScan();
}

void
PersistentCatCache::_CreateTableIfNullForFileHandle(FileHandle &fh,
                                                    FieldOffset reclen) {
    if (fh.m_table.get() == nullptr) {
        ASSERT(MAXALIGN(reclen) == reclen);

        // We may have to create a fake table descriptor on the first call
        // to AppendRecord() in order to construct a Table object during
        // bootstrapping/initialization.
        std::shared_ptr<SysTable_Table> tabentry(
            ConstructSysTableStruct<SysTable_Table>(
                /*tabid = */InvalidOid,
                /*tabissys = */true,
                /*tabisvarlen = */true,
                /*tabisncols = */0,
                /*tabfid = */fh.m_fid,
                /*tabname = */""));
        std::unique_ptr<Schema> sch =
            // we don't need the schema for variable-length data page build
            nullptr;
        std::shared_ptr<const TableDesc> tabdesc(
            TableDesc::Create(std::move(tabentry), std::move(sch)));
        fh.m_table = Table::Create(std::move(tabdesc));
    }

    // we should have a valid Table object at this point
    ASSERT(fh.m_table.get());
}


}   // namespace taco
