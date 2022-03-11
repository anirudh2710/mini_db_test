#include "storage/BufferManager.h"
#include "storage/FileManager.h"

namespace taco {

void
BufferUnpin::operator()(BufferId bufid) {
    g_bufman->UnpinPage(bufid);
}

void
BufferUnlatch::operator()(BufferId bufid) {
    g_bufman->UnlatchPage(bufid);
}

BufferId
BufferManager::PinPage(PageNumber pid, char **frame, FileId expected_fid) {
    char *frame2;
    BufferId bufid = PinPage(pid, &frame2);

    ASSERT(bufid != INVALID_BUFID);
    PageHeaderData *ph = (PageHeaderData *) frame2;
    if (ph->GetFileId() != expected_fid) {
        UnpinPage(bufid);
        return INVALID_BUFID;
    }

    *frame = frame2;
    return bufid;
}

BufferId
BufferManager::LatchPage(BufferId bufid, LatchMode mode) {
    // dummy implementation for BufferManager::LatchPage()
    return bufid;
}

void
BufferManager::UnlatchPage(BufferId bufid) {
    // dummy implementation for BufferManager::UnlatchPage()
}

}   // namespace taco

