#include "catalog/VolatileCatCache.h"

// include the private implementations of the CatCacheBase
#include "CatCacheBase_private.inc"

namespace taco {

// explicit instantiation of the base class of VolatileCatCache
template class CatCacheBase<VolatileCatCache>;

FileId
VolatileCatCache::CreateCatalogFile(bool format_heapfile, FieldOffset /*unused*/) {
    if (format_heapfile) {
        m_systables.emplace_back(absl::make_unique<InmemFile>());
        // file ID - 2 is the index into the m_systables
        return (FileId)(m_systables.size() + 1);
    }

    // Raw file for DB meta file, which must have a file ID of 1.
    // XXX here we assume the CatCacheBase only asks for one raw file
    // as the DB meta file, and it only has one single page. If that's changed,
    // we must also update the implementation here.
    if (m_dbmeta_page) {
        LOG(kFatal, "VolatileCatCache does not support more than "
                    "1 non-heapfile");
    }
    m_dbmeta_page = unique_aligned_alloc(512, PAGE_SIZE);
    return 1;
}

VolatileCatCache::FileHandle
VolatileCatCache::OpenCatalogFile(FileId fid, const TableDesc*) {
    if (fid == INVALID_FID || fid > m_systables.size() + 1) {
        LOG(kFatal, "not a valid catalog file ID " FILEID_FORMAT, fid);
    }
    return fid;
}

void
VolatileCatCache::CloseCatalogFile(FileHandle &fh) {
    fh = INVALID_FID;
}

VolatileCatCache::PageHandle
VolatileCatCache::GetFirstPage(FileHandle &fh, char **pagebuf) {
    if (fh != DBMETA_FID) {
        LOG(kFatal, "can't access a heapfile catalog file as a non-heapfile in "
                    "VolatileCatCache");
    }
    *pagebuf = (char*) m_dbmeta_page.get();
    return 1;
}

void
VolatileCatCache::MarkPageDirty(PageHandle &pghandle) {
    if (pghandle != 1) {
        LOG(kFatal, "invalid page handle in VolatileCatCache: %d",
                    pghandle);
    }
}

void
VolatileCatCache::ReleasePage(PageHandle &pghandle) {
    if (pghandle != 1) {
        LOG(kFatal, "invalid page handle in VolatileCatCache: %d",
                    pghandle);
    }
    pghandle = 0;
}

void
VolatileCatCache::AppendRecord(FileHandle &fh, Record &rec) {
    if (fh <= 1 || fh > m_systables.size() + 1) {
        LOG(kFatal, "invalid heapfile handle in VolatileCatCache: "
                    FILEID_FORMAT, fh);
    }

    InmemFile *f = m_systables[fh - 2].get();
    if (f->m_offsets.size() >=
            (size_t) std::numeric_limits<uint16_t>::max() - 1) {
        LOG(kFatal, "no more than %u records supported in a catalog file of "
                    "VolatileCatCache",
                    std::numeric_limits<uint16_t>::max() - 1);
    }

    size_t offset = f->m_bytes.size();
    ASSERT(MAXALIGN(offset) == offset);
    size_t new_bytes_size = MAXALIGN(offset + rec.GetLength());
    f->m_valid.push_back(true);
    f->m_offsets.push_back(offset);
    f->m_bytes.resize(new_bytes_size);
    memcpy(f->m_bytes.data() + offset, rec.GetData(), rec.GetLength());

    rec.GetRecordId().pid = fh;
    rec.GetRecordId().sid = f->m_offsets.size() - 1;
}

VolatileCatCache::CatFileIterator
VolatileCatCache::IterateCatEntry(FileHandle &fh) {
    FileId fid = fh;
    ASSERT(fid > 1 && fid <= m_systables.size() + 1);
    InmemFile *f = m_systables[fid - 2].get();
    return CatFileIterator{fh, 0, (uint16_t) f->m_offsets.size()};
}

VolatileCatCache::CatFileIterator
VolatileCatCache::IterateCatEntryFrom(FileHandle &fh, RecordId rid) {
    FileId fid = fh;
    ASSERT(fid > 1 && fid <= m_systables.size() + 1);
    ASSERT(fid == (FileId) rid.pid);
    InmemFile *f = m_systables[fid - 2].get();
    return CatFileIterator{fh, (uint16_t) rid.sid,
                               (uint16_t) f->m_offsets.size()};
}

bool
VolatileCatCache::NextCatEntry(CatFileIterator &iter) {
    const FileId fid = iter.m_fid;
    uint16_t next_idx = iter.m_nextidx;
    const uint16_t end_idx = iter.m_endidx;

    ASSERT(fid > 1 && fid <= m_systables.size() + 1);
    InmemFile *f = m_systables[fid - 2].get();
    ASSERT(end_idx <= f->m_offsets.size());
    while (next_idx < end_idx) {
        if (f->m_valid[next_idx]) {
            iter.m_nextidx = next_idx + 1;
            return true;
        }
        ++next_idx;
    }
    iter.m_nextidx = end_idx + 1;
    return false;
}

const char*
VolatileCatCache::GetCurrentCatEntry(CatFileIterator &iter) {
    const FileId fid = iter.m_fid;
    uint16_t cur_idx = iter.m_nextidx - 1;

    ASSERT(fid > 1 && fid <= m_systables.size() + 1);
    InmemFile *f = m_systables[fid - 2].get();
    ASSERT(f->m_valid[cur_idx]);
    ASSERT(cur_idx < f->m_offsets.size());
    return f->m_bytes.data() + f->m_offsets[cur_idx];
}

RecordId
VolatileCatCache::GetCurrentCatEntryRecordId(CatFileIterator &iter) {
    const FileId fid = iter.m_fid;
    uint16_t cur_idx = iter.m_nextidx - 1;
    RecordId recid;
    recid.pid = fid;
    recid.sid = cur_idx;
    return recid;
}

void
VolatileCatCache::UpdateCurrentCatEntry(CatFileIterator &iter,
                                        Record &rec) {
    const FileId fid = iter.m_fid;
    uint16_t cur_idx = iter.m_nextidx - 1;

    ASSERT(fid > 1 && fid <= m_systables.size() + 1);
    InmemFile *f = m_systables[fid - 2].get();
    ASSERT(f->m_valid[cur_idx]);
    ASSERT(cur_idx < f->m_offsets.size());
    size_t len = (cur_idx == f->m_offsets.size() - 1) ?
        (f->m_bytes.size() - f->m_offsets[cur_idx]) :
        (f->m_offsets[cur_idx + 1] - f->m_offsets[cur_idx]);
    if ((size_t) rec.GetLength() <= len) {
        // in place update
        memcpy(f->m_bytes.data() + f->m_offsets[cur_idx],
               rec.GetData(), rec.GetLength());

        // We reuse these two fields to save the record ID in the virtual file.
        rec.GetRecordId().pid = fid;
        rec.GetRecordId().sid = cur_idx;
    } else {
        // invalidate the current record and append the updated one
        f->m_valid[cur_idx] = false;
        FileHandle fh = fid;
        AppendRecord(fh, rec);
    }
}

void
VolatileCatCache::EndIterateCatEntry(CatFileIterator &iter) {
    iter.m_fid = 0;
}

}   // namespace taco

