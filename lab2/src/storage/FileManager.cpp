#include <sys/stat.h>
#include <sys/types.h>

#include "storage/BufferManager.h"
#include "storage/FileManager_private.h"
#include "storage/FSFile.h"
#include "utils/MutexGuard.h"
#include "utils/fsutils.h"

namespace taco {

static void check_datadir(const std::string &datadir, bool create);
static void create_datadir(const std::string &datadir);

FileManager::FileManager():
    m_datadir(""),
    m_mainfiles(),
    m_meta((FMMetaPageData *) aligned_alloc(512, PAGE_SIZE)) {

    memset((char*) m_meta, 0, PAGE_SIZE);
    // make sure m_mainfiles never resizes
    m_mainfiles.reserve(MaxNumDataFiles);
}

FileManager::~FileManager() {
    CloseAll();
    free(m_meta);
}

void
FileManager::CloseAll() {
    m_mainfiles.clear();
}

static void
check_datadir(const std::string &datadir, bool create) {
    // Check whether datadir exists and has the correct permission
    // We don't enforce 0700 permission though -- we only require that we have
    // access to the directory.
retry:
    if (access(datadir.c_str(), R_OK | W_OK | X_OK)) {
        if (errno == ENOENT) {
            if (!create) {
                LOG(kFatal, "data directory %s does not exist", datadir);
            }

            create_datadir(datadir);
            create = false;
            // double-check if the directory we just created is ok
            goto retry;
        }
        LOG(kFatal, "unable to access the data directory %s", datadir);
    }

    if (create) {
        // Checks if the directory is empty. If so, we need to create the
        // subdirs -- this supports the use case where the top datadir is
        // created by mkstemp(3). However, we do not allow an non-empty
        // directory.
        if (dir_empty(datadir.c_str())) {
            create_datadir(datadir);
            create = false;
            goto retry;
        }
        LOG(kFatal, "the data directory %s already exists", datadir);
    }

    std::string maindir = datadir + "main";
    if (access(maindir.c_str(), R_OK | W_OK | X_OK)) {
        LOG(kFatal, "unable to access the main data directory %s", maindir);
    }

    std::string tmpdir = datadir + "tmp";
    if (access(tmpdir.c_str(), R_OK | W_OK | X_OK)) {
        LOG(kFatal, "unable to access the temporary data directory %s", tmpdir);
    }

    std::string waldir = datadir + "wal";
    if (access(waldir.c_str(), R_OK | W_OK | X_OK)) {
        LOG(kFatal, "unable to access the WAL directory %s", waldir);
    }
}

static void
create_datadir(const std::string &datadir) {
    LOG(kInfo, "creating data directory %s", datadir);

    // pg_mkdir_p modifies the string, so make a mutable copy.
    unique_malloced_ptr datadir_copy = wrap_malloc(strdup(datadir.c_str()));

    // now we defaults to 0700 for permission when creating the data directory.
    if (pg_mkdir_p((char*) datadir_copy.get(), 0700)) {
        char *errstr = strerror(errno);
        LOG(kFatal, "unable to create data directory %s: %s", datadir, errstr);
    }

    std::string main_datafile_dir = datadir + "main";
    if (mkdir(main_datafile_dir.c_str(), 0700)) {
        char *errstr = strerror(errno);
        LOG(kFatal, "unable to write to data directory %s: %s", datadir, errstr);
    }

    std::string tmpdir = datadir + "tmp";
    if (mkdir(tmpdir.c_str(), 0700)) {
        char *errstr = strerror(errno);
        LOG(kFatal, "unable to write to data directory %s: %s", datadir, errstr);
    }

    std::string waldir = datadir + "wal";
    if (mkdir(waldir.c_str(), 0700)) {
        char *errstr = strerror(errno);
        LOG(kFatal, "unable to write to data directory %s: %s", datadir, errstr);
    }
}

void
FileManager::Init(std::string datadir, size_t init_size, bool create) {
    if (!m_datadir.empty()) {
        LOG(kFatal, "cannot initialize FileManager twice");
    }

    if (datadir.empty()) {
        datadir = "./";
    }
    if (datadir.back() != '/') {
        datadir.push_back('/');
    }

    // Check or create the data directory to ensure we have the correct
    // permission to access it. If the data directory exists and we are asked
    // to create it, this check will throw a fatal error.
    check_datadir(datadir, create);
    m_datadir = datadir;
    m_maindir = m_datadir + "main/";
    m_tmpdir = m_datadir + "tmp/";
    m_waldir = m_datadir + "wal/";

    // create or open the main data file
    FSFile *fsfile;
    if (create) {
        fsfile = FSFile::Open(GetMainDataFilePath(0),
                              /* trunc = */true,
                              /* o_direct = */true,
                              /* o_creat = */true,
                              /* mode = */0600);
        if (!fsfile) {
            char *errstr = strerror(errno);
            LOG(kFatal, "unable to create main data file %s: %s",
                        GetMainDataFilePath(1), errstr);
        }
        m_mainfiles.emplace_back(fsfile);
    } else {
        uint64_t i;
        for (i = 0; i < MaxNumDataFiles; ++i) {
            std::string fpath = GetMainDataFilePath(i);
            fsfile = FSFile::Open(GetMainDataFilePath(i),
                                  /* trunc = */false,
                                  /* o_direct = */true,
                                  /* o_creat = */false,
                                  /* mode = */0600);
            if (!fsfile) {
                if (errno == ENOENT) {
                    break;
                }
                char *errstr = strerror(errno);
                LOG(kFatal, "unable to open main data file %s: %s",
                            GetMainDataFilePath(i), errstr);
            }
            m_mainfiles.emplace_back(fsfile);
        }
        if (m_mainfiles.empty()) {
            LOG(kFatal, "no main data file found in %s", m_maindir);
        }
        // TODO check if there're any exccessive files in m_maindir?
        fsfile = m_mainfiles.front().get();
    }

    if (create) {
        if (init_size % PAGE_SIZE != 0) {
            LOG(kFatal, "invalid init_size: %lu % PAGE_SIZE = %lu != 0",
                    init_size, init_size % PAGE_SIZE);
        }
        PageNumber init_fsize = init_size / PAGE_SIZE;
        // allocate only the the necessary pages if this is new
        if (init_fsize == 0) {
            init_fsize = 3;
        }
        // round init_fsize to PageGroupSize boundries
        init_fsize =
            (init_fsize + PageGroupSize - 1) / PageGroupSize * PageGroupSize;
        if (init_fsize > MaxNumPagesPerFile) {
            LOG(kFatal,
                "init_fsize = %u excceeds the maximum number of pages per file "
                "%u", init_fsize, MaxNumPagesPerFile);
        }
        fsfile->Allocate(init_fsize * PAGE_SIZE);

        // initialize the metadata
        FormatMeta();
    } else {
        // load the metadata
        LoadMeta();
    }
}

std::string
FileManager::GetMainDataFilePath(uint64_t DataFileId) const {
    ASSERT(DataFileId < MaxNumDataFiles);
    return absl::StrFormat("%s%0" STRINGIFY(DataFileNameLength) "X",
                            m_maindir, DataFileId);
}

void
FileManager::FormatMeta() {
    PageNumber meta = FM_META_PID;
    PageNumber fdir1, fdir2;

    ASSERT(m_mainfiles.size() == 1);
    ASSERT(m_mainfiles[0]->Size() >= 3 * PAGE_SIZE);

    auto fdirbuf = unique_aligned_alloc(512, PAGE_SIZE);
    memset(fdirbuf.get(), 0, PAGE_SIZE);
    FileDirectoryData *fdirdata = (FileDirectoryData *) fdirbuf.get();
    fdirdata->m_ph.m_flags = PageHeaderData::FLAG_META_PAGE;

    // writing the FM meta page
    m_meta->m_ph.m_flags = PageHeaderData::FLAG_META_PAGE;
    m_meta->m_magic = DBFILE_MAGIC;
    InitFreePageList(&m_meta->m_fpl, meta);
    m_meta->m_fdir1_pid = fdir1 = GetNextFreePageNumber(&m_meta->m_fpl,
                                                        INVALID_BUFID);
    fdir2 = GetNextFreePageNumber(&m_meta->m_fpl, INVALID_BUFID);
    m_mainfiles[0]->Write(m_meta, PAGE_SIZE, meta * PAGE_SIZE);

    // writing the l1 file directory
    fdirdata->m_pid[0] = fdir2;
    m_mainfiles[0]->Write(fdirdata, PAGE_SIZE, fdir1 * PAGE_SIZE);

    // writing the l2 file directory
    fdirdata->m_pid[0] = INVALID_PID;
    m_mainfiles[0]->Write(fdirdata, PAGE_SIZE, fdir2 * PAGE_SIZE);

    // make sure these writes are persistent
    m_mainfiles[0]->Flush();
}

void
FileManager::LoadMeta() {
    PageNumber meta = 0;

    m_mainfiles[0]->Read((char*) m_meta, PAGE_SIZE, meta * PAGE_SIZE);
    if ((m_meta->m_ph.m_flags & (PageHeaderData::FLAG_META_PAGE |
                                 PageHeaderData::FLAG_VFILE_PAGE)) !=
        PageHeaderData::FLAG_META_PAGE ||
        m_meta->m_magic != DBFILE_MAGIC) {
        LOG(kFatal, "the main data file %s is corrupted",
            GetMainDataFilePath(0));
    }
}

std::unique_ptr<File>
FileManager::Open(FileId fid) {
    bool is_wal = fid & (WAL_FILEID_MASK);
    bool is_tmp = fid & (TMP_FILEID_MASK);
    bool is_main = !is_wal && !is_tmp;

    // the fid with no flag bits
    FileId fid_n = fid & ~(WAL_FILEID_MASK | TMP_FILEID_MASK);
    bool create_new = fid_n == INVALID_FID;

    if ((is_wal && is_tmp) || (is_main && fid_n > MaxRegularFileId)) {
        LOG(kFatal, "invalid file id " FILEID_FORMAT, fid);
    }

    // TODO
    if (is_wal) {
        LOG(kFatal, "WAL file is not implemented yet");
    }

    if (is_tmp) {
        if (create_new) {
            return CreateTmpFile();
        } else {
            return OpenTmpFile(fid);
        }
    }

    if (create_new) {
        return CreateRegularFile();
    }
    return OpenRegularFile(fid);
}

/*
void
FileManager::ReadPage(PageNumber pid, char *buf) {
    taco_fileman_readpage_impl(this, pid, buf);
}

void
FileManager::WritePage(PageNumber pid, const char *buf) {
    taco_fileman_writepage_impl(this, pid, buf);
} */

std::unique_ptr<File>
FileManager::CreateTmpFile() {
    // TODO
    LOG(kFatal, "tmp file is not implemented yet");
    return nullptr;
}

std::unique_ptr<File>
FileManager::OpenTmpFile(FileId fid) {
    ASSERT((fid & TMP_FILEID_MASK) && !(fid & WAL_FILEID_MASK));
    // TODO
    LOG(kFatal, "tmp file is not implemented yet");
    return nullptr;
}

std::unique_ptr<File>
FileManager::CreateRegularFile() {
    FileId fid = FindFreeFileId();

    // Find a free page group for the new file.
    // Currently we always append a page group at the end of the last file.
    // TODO find a free page group instead?
    PageNumber meta_pid = AllocatePageGroup();

    // write the meta page
    char *meta_buf;
    ScopedBufferId meta_bufid = g_bufman->PinPage(meta_pid, &meta_buf);
    RegularFileMetaPageData *meta = (RegularFileMetaPageData *) meta_buf;
    meta->m_ph.m_flags = PageHeaderData::FLAG_META_PAGE |
                         PageHeaderData::FLAG_VFILE_PAGE;
    meta->m_ph.m_fid = fid;
    meta->m_ph.m_prev_pid.store(INVALID_PID, memory_order_relaxed);
    meta->m_ph.m_next_pid.store(INVALID_PID, memory_order_relaxed);
    InitFreePageList(&meta->m_fpl, meta_pid);

    // allocate the first page
    PageNumber firstpg_pid = GetNextFreePageNumber(&meta->m_fpl, meta_bufid);
    meta->m_first_pid = firstpg_pid;
    meta->m_last_pid.store(firstpg_pid, memory_order_relaxed);
    g_bufman->MarkDirty(meta_bufid);
    g_bufman->UnpinPage(meta_bufid);

    // initialize the first page's header
    char *firstpg_buf;
    ScopedBufferId firstpg_bufid = g_bufman->PinPage(firstpg_pid, &firstpg_buf);
    PageHeaderData *firstpg_hdr = (PageHeaderData *) firstpg_buf;
    firstpg_hdr->m_flags = PageHeaderData::FLAG_VFILE_PAGE;
    firstpg_hdr->m_fid = fid;
    firstpg_hdr->m_prev_pid.store(INVALID_PID, memory_order_relaxed);
    firstpg_hdr->m_next_pid.store(INVALID_PID, memory_order_relaxed);
    g_bufman->MarkDirty(firstpg_bufid);
    g_bufman->UnpinPage(firstpg_bufid);

    // link the file in the page directory
    auto fdir1_off = FileIdGetDir1Offset(fid);
    auto fdir2_off = FileIdGetDir2Offset(fid);
    PageNumber fdir2_pid;
    {
        char *buf;
        FileDirectoryData *fdir;
        ScopedBufferId bufid = g_bufman->PinPage(m_meta->m_fdir1_pid, &buf);
        fdir = (FileDirectoryData*) buf;
        fdir2_pid = fdir->m_pid[fdir1_off].load(memory_order_relaxed);
    }

    ASSERT(fdir2_pid != INVALID_PID);
    {
        char *buf;
        FileDirectoryData *fdir;
        ScopedBufferId bufid = g_bufman->PinPage(fdir2_pid, &buf);
        fdir = (FileDirectoryData*) buf;
        ASSERT(fdir->m_pid[fdir2_off].load(memory_order_relaxed) ==
               PENDING_CREATION_PID);
        fdir->m_pid[fdir2_off].store(meta_pid, memory_order_relaxed);
        g_bufman->MarkDirty(bufid);
    }

    // return a file structure
    std::unique_ptr<File> f(new File());
    f->m_fileid = fid;
    f->m_meta_pid = meta_pid;
    return f;
}

std::unique_ptr<File>
FileManager::OpenRegularFile(FileId fid) {
    ASSERT(!(fid & TMP_FILEID_MASK) && !(fid & WAL_FILEID_MASK));

    auto fdir1_off = FileIdGetDir1Offset(fid);
    auto fdir2_off = FileIdGetDir2Offset(fid);
    PageNumber fdir1_pid = m_meta->m_fdir1_pid;
    PageNumber fdir2_pid;
    PageNumber meta_pid;

    // look up the first level file directory
    BufferId bufid;
    char *buf;
    bufid = g_bufman->PinPage(fdir1_pid, &buf);
    FileDirectoryData *fdir1 = (FileDirectoryData*) buf;
    fdir2_pid = fdir1->m_pid[fdir1_off].load(memory_order_relaxed);
    g_bufman->UnpinPage(bufid);

    if (fdir2_pid == INVALID_PID) {
        LOG(kError, "regular virtual file " FILEID_FORMAT " does not exist",
                fid);
    }

    // look up the second level file directory
    bufid = g_bufman->PinPage(fdir2_pid, &buf);
    FileDirectoryData *fdir2 = (FileDirectoryData*) buf;
    meta_pid = fdir2->m_pid[fdir2_off].load(memory_order_relaxed);
    g_bufman->UnpinPage(bufid);

    if (meta_pid == INVALID_PID) {
        LOG(kError, "regular virtual file " FIELDID_FORMAT " does not exist",
            fid);
    }

    // create and return the file structure
    std::unique_ptr<File> f(new File());
    f->m_fileid = fid;
    f->m_meta_pid = meta_pid;
    return f;
}

FileId
FileManager::FindFreeFileId() {
    FileId last_free = m_meta->m_last_free_fid.load(memory_order_relaxed);
    FileId fid = (last_free == MaxRegularFileId) ? MinRegularFileId
                                                 : (last_free + 1);
    uint64_t n_searched = 0;
    ScopedBufferId fdir1_bufid;
    char *fdir1_buf;
    FileDirectoryData *fdir1;
    ScopedBufferId fdir2_bufid;
    char *fdir2buf = nullptr;
    FileDirectoryData *fdir2 = nullptr;

    fdir1_bufid = g_bufman->PinPage(m_meta->m_fdir1_pid, &fdir1_buf);
    fdir1 = (FileDirectoryData *) fdir1_buf;

    // We shouldn't need to search a lot for a free file ID unless we are
    // almost full or heavily contended.
    for (; n_searched < 512; ++n_searched) {
        auto fdir1_off = FileIdGetDir1Offset(fid);
        auto fdir2_off = FileIdGetDir2Offset(fid);

        PageNumber fdir2_pid =
            fdir1->m_pid[fdir1_off].load(memory_order_relaxed);
        if (fdir2_pid == INVALID_PID) {
            // We use a latch here as finding a new page could potentially be
            // quite expensive and we don't want many threads to simultaneouly
            // try to allocate the same second-level directory page.
            MutexGuard lg(&m_fdir1_latch);
            fdir2_pid =
                fdir1->m_pid[fdir1_off].load(memory_order_relaxed);
            if (fdir2_pid == INVALID_PID) {
                // check again in case someone else has allocated an L2-dir
                AllocateL2FileDirectory(fdir1_bufid, fdir1_off);
                fdir2_pid = fdir1->m_pid[fdir1_off].load(memory_order_relaxed);
            }
        }

        ASSERT(fdir2_pid != INVALID_PID);
        if (!fdir2_bufid.IsValid() ||
            g_bufman->GetPageNumber(fdir2_bufid) != fdir2_pid) {
            fdir2_bufid.Reset();
            fdir2_bufid = g_bufman->PinPage(fdir2_pid, &fdir2buf);
            fdir2 = (FileDirectoryData*) fdir2buf;
        }

        PageNumber cmeta_pid =
            fdir2->m_pid[fdir2_off].load(memory_order_relaxed);
        while (cmeta_pid == INVALID_PID) {
            if (fdir2->m_pid[fdir2_off].compare_exchange_weak(
                    cmeta_pid, PENDING_CREATION_PID, memory_order_relaxed)) {
                // found and marked a free file ID
                g_bufman->MarkDirty(fdir2_bufid);
                m_meta->m_last_free_fid.store(fid, memory_order_relaxed);
                return fid;
            }
        }

        // this file ID has been used, try the next one
        fid = (fid == MaxRegularFileId) ? MinRegularFileId : (fid + 1);
    }

    LOG(kError, "unable to find a free file ID");
    return INVALID_FID;
}

void
FileManager::AllocateL2FileDirectory(BufferId fdir1_bufid, size_t fdir1_off) {
    PageNumber fdir2_pid = GetNextFreePageNumber(&m_meta->m_fpl, INVALID_BUFID);
    char *buf;

    {
        ScopedBufferId fdir2_bufid = g_bufman->PinPage(fdir2_pid, &buf);
        memset(buf, 0, PAGE_SIZE);
        FileDirectoryData *fdir2 = (FileDirectoryData*) buf;
        fdir2->m_ph.m_flags = PageHeaderData::FLAG_META_PAGE;
        g_bufman->MarkDirty(fdir2_bufid);
    }

    buf = g_bufman->GetBuffer(fdir1_bufid);
    FileDirectoryData *fdir1 = (FileDirectoryData*) buf;
    ASSERT(fdir1->m_pid[fdir1_off].load(memory_order_relaxed)
            == INVALID_PID);
    fdir1->m_pid[fdir1_off].store(fdir2_pid, memory_order_relaxed);
    g_bufman->MarkDirty(fdir1_bufid);
}

PageNumber
FileManager::AllocatePageGroup() {
    // allocate a new page group for the new file
    PageNumber pid;
    PageNumber fspid;
    uint64_t fsfileid;

    MutexGuard lg(&m_fsfile_extension_latch);

    FSFile *mainfile = m_mainfiles.back().get();
    if (mainfile->Size() >= MaxFileSize) {
        // create a new file
        uint64_t next_fileno = m_mainfiles.size();
        if (next_fileno == MaxNumDataFiles) {
            LOG(kError, "too many data files");
        }
        m_mainfiles.emplace_back(
            FSFile::Open(GetMainDataFilePath(next_fileno),
                         /*o_trunc = */true,
                         /*o_direct = */true,
                         /*o_creat = */true,
                         /*mode = */0600));
        mainfile = m_mainfiles.back().get();
    }

    fsfileid = m_mainfiles.size() - 1;
    fspid = mainfile->Size() / PAGE_SIZE;
    pid = DataFileIdAndPageIdGetPageNumber(fsfileid, fspid);
    mainfile->Allocate(PageGroupSize * PAGE_SIZE);
    ASSERT(mainfile->Size() / PAGE_SIZE == fspid + PageGroupSize);

    return pid;
}

void
FileManager::InitFreePageList(FreePageList *fpl, PageNumber last_pg) {
    ASSERT(last_pg % PageGroupSize == 0);
    fpl->m_next_fp_in_last_pg.store(last_pg + 1, memory_order_relaxed);
    fpl->m_last_pg.store(last_pg, memory_order_relaxed);
}

PageNumber
FileManager::GetNextFreePageNumber(FreePageList *fpl, BufferId meta_bufid) {
    if (meta_bufid != INVALID_BUFID) {
        // We don't keep a list of free pages for the fm meta file. This is
        // only for the regular files.
        // XXX acquire might not be necessary here as PinPage() should have
        // acquire semantics? We don't support cc right now and thus it doesn't
        // make any difference, but make sure to take a second look here once
        // we want to add cc support.
        PageNumber fp = fpl->m_fp_list.load(memory_order_acquire);
        for (;;) {
            if (fp == INVALID_PID) {
                // no free page available in the free list
                break;
            }
            char *pagebuf;
            ScopedBufferId bufid = g_bufman->PinPage(fp, &pagebuf);
            PageHeaderData *hdr = (PageHeaderData *) pagebuf;
            if (hdr->IsAllocated()) {
                // the page must have been allocated by someone else
                fp = fpl->m_fp_list.load(memory_order_acquire);
                continue;
            }

            PageNumber new_fp_list_head =
                hdr->m_next_pid.load(memory_order_relaxed);
            if (fpl->m_fp_list.compare_exchange_strong(
                    fp, new_fp_list_head, memory_order_acq_rel)) {
                g_bufman->MarkDirty(meta_bufid);
                return fp;
            }
        }
    }

    // We don't require any synchronization between m_last_pg and m_next_fp:
    // m_last_pg is only changed when holding a exclusive latch (or a mutex)
    // and m_next_fp may only be incremented when it is still within the range
    // of the last page group ([m_last_pg, m_last_pg + PageGroupSize)). If the
    // store to m_last_pg and m_next_fp is reordered, the worst that can happen
    // is m_next_fp seems to be out of the range of the last page group, in
    // which case we just have to reload the values and retry.
    PageNumber last_pg = fpl->m_last_pg.load(memory_order_relaxed);
    PageNumber next_fp = fpl->m_next_fp_in_last_pg.load(memory_order_relaxed);
    for (;;) {

        // next_fp seems valid, try to CAS it
        if (next_fp >= last_pg && next_fp < last_pg + PageGroupSize) {
            PageNumber new_fp = next_fp + 1;
            if (fpl->m_next_fp_in_last_pg.compare_exchange_weak(
                    next_fp, new_fp, memory_order_relaxed)) {
                if (meta_bufid != INVALID_BUFID) {
                    // mark the meta page as dirty if it is managed by the
                    // buffer manager
                    g_bufman->MarkDirty(meta_bufid);
                }
                // skip writing it back if it's an FM meta page -- TODO it can
                // be recovered if there's a crash.
                return next_fp;
            }
            // do the checks again
            continue;
        }

        if (next_fp == last_pg + PageGroupSize) {
            // we may have depleted this group, now it's time to ask for a new
            // group. We must exclusively latch the meta page where this free
            // page list is located, which is usually in the buffer pool.
            // However, a special case is when this page is the FM meta page,
            // it is not in the buffer pool and we'll have to latch the FM meta
            // page mutex in the file manager.
            ScopedBufferLatch latch  = (meta_bufid == INVALID_BUFID)
                ? INVALID_BUFID
                : g_bufman->LatchPage(meta_bufid, LatchMode::EX);
            MutexGuard mg((meta_bufid == INVALID_BUFID)
                ? &m_meta_latch : nullptr);

            PageNumber current_last_pg =
                fpl->m_last_pg.load(memory_order_relaxed);
            if (last_pg == current_last_pg) {
                // We allocate a new page group and keep the first page within
                // that as our new page.
                PageNumber new_pg = AllocatePageGroup();
                fpl->m_last_pg.store(new_pg, memory_order_relaxed);
                fpl->m_next_fp_in_last_pg.store(new_pg + 1,
                                                memory_order_relaxed);
                if (meta_bufid != INVALID_BUFID) {
                    // mark it dirty if it is managed by the buffer manager
                    g_bufman->MarkDirty(meta_bufid);
                } else {
                    // for FM meta page, write it back
                    WritePage(FM_META_PID, (const char *) m_meta);
                }
                return new_pg;
            }

            // If last_pg has changed since we grabed the latch, someone must
            // have allocated a new page group. so we just fall through to
            // reload the values of next_fp and last_pg.
        }

        // next_fp < last_pg || next_fp > last_pg + PageGroupSize
        // This can only happen when someone concurrently allocates a new page
        // group and we happen to have read the old value of next_fp. Just
        // refetch the two and try again.
        last_pg = fpl->m_last_pg.load(memory_order_relaxed);
        next_fp = fpl->m_next_fp_in_last_pg.load(memory_order_relaxed);
    }

    LOG(kFatal, "unreachable");
    return INVALID_PID;
}

void
FileManager::AddToFreePageList(FreePageList *fpl,
                               BufferId pg_bufid,
                               BufferId meta_bufid) {
    if (meta_bufid == INVALID_BUFID) {
        LOG(kFatal, "free page list not implemented for FM meta file");
    }

    PageNumber pid = g_bufman->GetPageNumber(pg_bufid);
    char *pagebuf = g_bufman->GetBuffer(pg_bufid);
    PageHeaderData *hdr = (PageHeaderData *) pagebuf;
    ASSERT(!hdr->IsAllocated());

    PageNumber fp = fpl->m_fp_list.load(memory_order_acquire);
    for (;;) {
        hdr->m_next_pid.store(fp, memory_order_relaxed);
        if (fpl->m_fp_list.compare_exchange_strong(
                fp, pid, memory_order_acq_rel)) {
            g_bufman->MarkDirty(pg_bufid);
            g_bufman->MarkDirty(meta_bufid);
            break;
        }
    };
}

void
FileManager::Flush() {
    for (uint64_t i = 0; i < m_mainfiles.size(); ++i) {
        m_mainfiles[i]->Flush();
    }
}

File::~File() {
    Close();
}

void
File::Close() {
    if (!IsOpen()) {
        return;
    }

    bool is_wal = m_fileid& (WAL_FILEID_MASK);
    bool is_tmp = m_fileid & (TMP_FILEID_MASK);
    bool is_main = !is_wal && !is_tmp;

    if (is_main) {
        m_fileid = INVALID_FID;
        return;
    }

    LOG(kFatal, "not implemented");
}

void
File::ReadPage(PageNumber pid, char *buf) {
    bool is_wal = m_fileid& (WAL_FILEID_MASK);
    bool is_tmp = m_fileid& (TMP_FILEID_MASK);
    bool is_main = !is_wal && !is_tmp;

    if (is_main) {
        g_fileman->ReadPage(pid, buf);

        PageHeaderData *ph = (PageHeaderData *) buf;
        if (ph->GetFileId() != m_fileid) {
            LOG(kError, "invalid page number " PAGENUMBER_FORMAT
                        " for file " FILEID_FORMAT
                        " : it is a page of file " FILEID_FORMAT,
                        pid, m_fileid, ph->GetFileId());
        }
        return;
    }

    // TODO for tmp and wal
    LOG(kFatal, "not implemented");
}

void
File::WritePage(PageNumber pid, const char *buf) {
    bool is_wal = m_fileid & (WAL_FILEID_MASK);
    bool is_tmp = m_fileid & (TMP_FILEID_MASK);
    bool is_main = !is_wal && !is_tmp;

    if (is_main) {
        const PageHeaderData *ph = (const PageHeaderData *) buf;
        if (ph->GetFileId() != m_fileid) {
            LOG(kError, "invalid page number " PAGENUMBER_FORMAT
                        " for file " FILEID_FORMAT
                        " : it is a page of file " FILEID_FORMAT,
                        pid, m_fileid, ph->GetFileId());
        }

        g_fileman->WritePage(pid, buf);
        return;
    }

    // TODO for tmp and wal
    LOG(kFatal, "not implemented");
}


void
File::Flush() {
    bool is_wal = m_fileid & (WAL_FILEID_MASK);
    bool is_tmp = m_fileid & (TMP_FILEID_MASK);
    bool is_main = !is_wal && !is_tmp;

    if (is_main) {
        LOG(kFatal, "flushing individual regular file is not supported");
    }

    // TODO for tmp and wal
    LOG(kFatal, "not implemented");
}

uint64_t
File::AllocatePageImpl(bool need_latch, LatchMode mode) {
    bool is_wal = m_fileid & (WAL_FILEID_MASK);
    bool is_tmp = m_fileid & (TMP_FILEID_MASK);
    bool is_main = !is_wal && !is_tmp;

    if (is_main) {
        char *buf;
        ScopedBufferId meta_bufid(g_bufman->PinPage(m_meta_pid, &buf));
        g_bufman->LatchPage(meta_bufid, LatchMode::SH);
        RegularFileMetaPageData *meta = (RegularFileMetaPageData *) buf;
        PageNumber newpg_pid =
            g_fileman->GetNextFreePageNumber(&meta->m_fpl, meta_bufid);

        // pin the newly allocated page
        ScopedBufferId newpg_bufid(g_bufman->PinPage(newpg_pid, &buf));

        // format the page header
        PageHeaderData *newpg_hdr = (PageHeaderData *) buf;
        newpg_hdr->m_flags = PageHeaderData::FLAG_VFILE_PAGE;
        newpg_hdr->m_fid = m_fileid;
        //newpg_hdr->m_prev_pid will be updated when we link it to the page
        //list tail
        newpg_hdr->m_next_pid.store(INVALID_PID, memory_order_relaxed);
        g_bufman->MarkDirty(newpg_bufid);

        // link the new page to the list tail, we may have to try several times
        // until we successfully CAS the m_next_pid in the old last page's
        // header
        PageNumber last_pid = INVALID_PID; // init to make the compiler happy
        PageNumber next_pid = meta->m_last_pid.load(memory_order_relaxed);
        ASSERT(next_pid != INVALID_PID);
        char *lastpg_buf;
        ScopedBufferId lastpg_bufid;
        PageHeaderData *lastpg_hdr;
        for (;;) {
            // first time in the loop or next_pid is no longer the list tail
            // and we need to move to the next
            if (next_pid != INVALID_PID) {
                if (lastpg_bufid.IsValid())
                    g_bufman->UnpinPage(lastpg_bufid);

                last_pid = next_pid;
                lastpg_bufid = g_bufman->PinPage(next_pid, &lastpg_buf);
                lastpg_hdr = (PageHeaderData *) lastpg_buf;
                next_pid = lastpg_hdr->m_next_pid.load(memory_order_acquire);
                continue;
            }

            newpg_hdr->m_prev_pid.store(last_pid, memory_order_relaxed);
            // m_next_pid serves as a release synchronization point with a
            // subsequent acquire load so that our updates to the newpg_hdr
            // happen before any access to them following the m_next_pid
            if (!lastpg_hdr->m_next_pid.compare_exchange_strong(
                    next_pid, newpg_pid,
                    /*success = */memory_order_release,
                    /*failure = */memory_order_relaxed)) {
                ASSERT(next_pid != INVALID_PID);
                // CAS failed, continue to move right..
                continue;
            }

            // We've successfully installed the new page pointer in the list.
            g_bufman->MarkDirty(lastpg_bufid);

            // Update the m_last_pid. Blind writes into it may be reordered,
            // but it will be eventually be fixed when someone reaches the end
            // again.
            meta->m_last_pid.store(newpg_pid, memory_order_release);
            g_bufman->MarkDirty(meta_bufid);
            break;
        }

        if (need_latch) {
            lastpg_bufid.Reset();
            meta_bufid.Reset();
            g_bufman->LatchPage(newpg_bufid, mode);
            return newpg_bufid.Release();
        }
        return newpg_pid;
    }

    if (need_latch) {
        LOG(kError, "TMP or WAL file page cannot be latched");
    }

    // TODO for tmp and wal
    LOG(kFatal, "not implemented");
    return INVALID_PID;
}

void
File::FreePage(BufferId bufid) {
    bool is_wal = m_fileid & (WAL_FILEID_MASK);
    bool is_tmp = m_fileid & (TMP_FILEID_MASK);
    bool is_main = !is_wal && !is_tmp;

    if (is_main) {
        // XXX the current implementation does not handle the concurrency well
        // as it needs to obtain an exclusive meta page latch to block any
        // other allocate/free calls.
        // It doesn't matter for now since we don't provide cc support anyway
        // but make sure to take another look at this later.
        ASSERT(bufid != INVALID_BUFID);
        char *pagebuf = g_bufman->GetBuffer(bufid);
        PageHeaderData *hdr = (PageHeaderData *) pagebuf;
        if (hdr->GetPrevPageNumber() == INVALID_PID) {
            // the first page
            g_bufman->UnpinPage(bufid);
            return ;
        }

        // pin the file meta page
        char *metabuf;
        ScopedBufferId meta_bufid(g_bufman->PinPage(m_meta_pid, &metabuf));
        g_bufman->LatchPage(meta_bufid, LatchMode::EX);
        RegularFileMetaPageData *meta = (RegularFileMetaPageData *) metabuf;

        PageNumber prev_pg = hdr->GetPrevPageNumber();
        PageNumber next_pg = hdr->GetNextPageNumber();

        // fix the next page's prev pointer
        if (next_pg != INVALID_PID) {
            char *rpagebuf;
            ScopedBufferId rbufid = g_bufman->PinPage(next_pg, &rpagebuf);
            PageHeaderData *rhdr = (PageHeaderData *) rpagebuf;
            rhdr->m_prev_pid.store(prev_pg, memory_order_relaxed);
            g_bufman->MarkDirty(rbufid);
        } else {
            meta->m_last_pid.store(prev_pg);
            g_bufman->MarkDirty(meta_bufid);
        }

        // fix the prev page's next pointer
        {
            char *lpagebuf;
            ScopedBufferId lbufid = g_bufman->PinPage(prev_pg, &lpagebuf);
            PageHeaderData *lhdr = (PageHeaderData *) lpagebuf;
            lhdr->m_next_pid.store(next_pg, memory_order_relaxed);
            g_bufman->MarkDirty(lbufid);
        }

        // marks the page as deallocated
        //hdr->m_prev_pid.store(INVALID_PID, memory_order_relaxed);
        //hdr->m_next_pid.store(INVALID_PID, memory_order_relaxed);
        //hdr->m_flags = 0; // this marks the page as not allocated
        memset(pagebuf, 0, PAGE_SIZE);
        g_bufman->MarkDirty(bufid);

        // and add it to the fpl
        g_bufman->UnlatchPage(meta_bufid);
        g_fileman->AddToFreePageList(&meta->m_fpl, bufid, meta_bufid);
        g_bufman->UnpinPage(bufid);
        return ;
    }

    LOG(kFatal, "can't free page in TMP or WAL files");
}

PageNumber
File::GetFirstPageNumber() {
    char *buf;
    ScopedBufferId meta_bufid = g_bufman->PinPage(m_meta_pid, &buf);
    RegularFileMetaPageData *meta = (RegularFileMetaPageData *) buf;
    return meta->m_first_pid;
}

PageNumber
File::GetLastPageNumber() {
    char *buf;
    ScopedBufferId meta_bufid = g_bufman->PinPage(m_meta_pid, &buf);
    RegularFileMetaPageData *meta = (RegularFileMetaPageData *) buf;
    return meta->m_last_pid.load(memory_order_acquire);
}

}   // namespace taco

