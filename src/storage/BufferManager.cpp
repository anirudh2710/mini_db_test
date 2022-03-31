// src/storage/BufferManager.cpp

#include "storage/BufferManager.h"
#include "storage/FileManager.h"

namespace taco {

struct BufferMeta {
    PageNumber pid;
    uint32_t clock_count : 1;
    uint32_t dirty: 1;
    uint32_t pin_count : 30;
};
static_assert(sizeof(BufferMeta) == 8);

BufferManager::BufferManager():
    initialized(false) {}

BufferManager::~BufferManager() {
    if (metas)
        free(metas);
    if (buffer_frames)
        free(buffer_frames);
}

void
BufferManager::Init(BufferId buffer_size) {
    n = buffer_size;
    lookup_table = absl::flat_hash_map<PageNumber, BufferId>(1.25 * n);
    metas = static_cast<BufferMeta*>(aligned_alloc(CACHELINE_SIZE, sizeof(BufferMeta) * n));
    memset(metas, 0, sizeof(BufferMeta) * n);
    buffer_frames = static_cast<char*>(aligned_alloc(512, n * PAGE_SIZE));
    clock_hand = 0;
    initialized = true;
}

void
BufferManager::Destroy() {
    if (initialized) {
        Flush();
        initialized = false;
    }
}

BufferId
BufferManager::PinPage(PageNumber pid, char** frame) {
    ASSERT(frame);

    auto res = lookup_table.find(pid);
    BufferId bufid;
    if (res == lookup_table.end()) {
        bufid = GetFreeFrame();
        if (frame)
            *frame = buffer_frames + (bufid * PAGE_SIZE);
        g_fileman->ReadPage(pid, *frame);
        lookup_table.emplace(pid, bufid);
        metas[bufid].dirty = 0;
        metas[bufid].pid = pid;
        metas[bufid].pin_count = 1;
    } else {
        bufid = res->second;
        if (frame)
            *frame = buffer_frames + (bufid * PAGE_SIZE);
        metas[bufid].pin_count++;
    }
    metas[bufid].clock_count = 1;

    return bufid;
}

void
BufferManager::UnpinPage(BufferId bufid) {
    ASSERT(bufid < n);
    if (metas[bufid].pin_count == 0) {
        LOG(kFatal, "page not pinned");
    }
    --metas[bufid].pin_count;
}

void
BufferManager::MarkDirty(BufferId bufid) {
    if (metas[bufid].pin_count == 0) {
        LOG(kFatal, "page not pinned");
    }
    metas[bufid].dirty = 1;
}

void
BufferManager::Flush() {
    for (BufferId i = 0; i < n; ++i) {
        if (metas[i].pin_count != 0) {
            LOG(kFatal, "non-zero pin count %u of buffer " BUFFERID_FORMAT
                        " when BufferManager tries to flush it",
                        metas[i].pin_count, i);
        }
        if (metas[i].dirty) {
            g_db->file_manager()->WritePage(metas[i].pid, buffer_frames + (PAGE_SIZE * i));
            metas[i].dirty = 0;
        }
    }
    g_fileman->Flush();
}

BufferId
BufferManager::GetFreeFrame() {
    bool has_unpinned_page = false;
    BufferId num_pages_scanned = 0;
    while (metas[clock_hand].pin_count > 0 || metas[clock_hand].clock_count > 0) {
        if (metas[clock_hand].pin_count == 0) {
            --metas[clock_hand].clock_count;
            has_unpinned_page = true;
        }
        clock_hand = (++clock_hand >= n) ? 0 : clock_hand;
        ++num_pages_scanned;

        if (num_pages_scanned == n && !has_unpinned_page) {
            LOG(kError, "no evictable buffer frame available");
        }
    }

    //Evict
    if (metas[clock_hand].pid != INVALID_PID) {
        lookup_table.erase(metas[clock_hand].pid);
        if (metas[clock_hand].dirty) {
            g_fileman->WritePage(metas[clock_hand].pid, buffer_frames + (PAGE_SIZE * clock_hand));
        }
    }

    if (clock_hand + 1 == n) {
        clock_hand = 0;
        return n - 1;
    }
    return clock_hand++;
}

PageNumber
BufferManager::GetPageNumber(BufferId bufid) const {
    ASSERT(bufid < n);
    if (metas[bufid].pin_count == 0) {
        LOG(kFatal, "page not pinned");
    }
    return metas[bufid].pid;
}

char*
BufferManager::GetBuffer(BufferId bufid) const {
    ASSERT(bufid < n);
    if (metas[bufid].pin_count == 0) {
        LOG(kFatal, "page not pinned");
    }
    return buffer_frames + (bufid * PAGE_SIZE);
}

}   // namespace taco