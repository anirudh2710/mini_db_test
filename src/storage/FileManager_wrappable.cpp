/*!
 * @file
 *
 * This file contains functions that may be wrapped by --wrap option to ld.
 * This must be in a separate file than FileManager.cpp, so that
 */
#include "storage/FileManager.h"

#include "storage/FileManager_private.h"
#include "storage/FSFile.h"

extern "C" void
taco_fileman_readpage_impl(::taco::FileManager *fileman,
                           ::taco::PageNumber pid,
                           char *buf) {
    uint64_t dfid = ::taco::PageNumberGetDataFileId(pid);
    ::taco::PageNumber dfpid = ::taco::PageNumberGetDataFilePageId(pid);

    if (dfid >= fileman->m_mainfiles.size()) {
        LOG(::taco::kFatal,
            "trying to read a non-existent page " PAGENUMBER_FORMAT, pid);
    }
    ::taco::FSFile *fsfile = fileman->m_mainfiles[dfid].get();
    ::taco::PageNumber size = fsfile->Size() / PAGE_SIZE;
    if (dfpid >= size) {
        LOG(::taco::kFatal,
            "trying to read a non-existent page " PAGENUMBER_FORMAT, pid);
    }

    fsfile->Read(buf, PAGE_SIZE, dfpid * PAGE_SIZE);
}

extern "C" void
taco_fileman_writepage_impl(::taco::FileManager *fileman,
                            ::taco::PageNumber pid,
                            const char *buf) {
    uint64_t dfid = ::taco::PageNumberGetDataFileId(pid);
    ::taco::PageNumber dfpid = ::taco::PageNumberGetDataFilePageId(pid);

    if (dfid >= fileman->m_mainfiles.size()) {
        LOG(::taco::kFatal,
            "trying to write a non-existent page " PAGENUMBER_FORMAT, pid);
    }
    ::taco::FSFile *fsfile = fileman->m_mainfiles[dfid].get();
    ::taco::PageNumber size = fsfile->Size() / PAGE_SIZE;
    if (dfpid >= size) {
        LOG(::taco::kFatal,
            "trying to write a non-existent page " PAGENUMBER_FORMAT, pid);
    }

    fsfile->Write(buf, PAGE_SIZE, dfpid * PAGE_SIZE);
}

