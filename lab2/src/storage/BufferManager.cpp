// src/storage/BufferManager.cpp

#include "storage/BufferManager.h"
#include "storage/FileManager.h"

namespace taco {

BufferManager::BufferManager() {
    // TODO implement me
}

BufferManager::~BufferManager() {
    // TODO implement me
}

void
BufferManager::Init(BufferId buffer_size) {
    // TODO implement me
}

void
BufferManager::Destroy() {
    // TODO implement me
}

BufferId
BufferManager::PinPage(PageNumber pid, char** frame) {
    ASSERT(frame);

    // TODO implement me
    return INVALID_BUFID;
}

void
BufferManager::UnpinPage(BufferId bufid) {
    ASSERT(bufid != INVALID_BUFID /* && is valid for this instance */);
    // TODO implement me
}

void
BufferManager::MarkDirty(BufferId bufid) {
    // TODO implement me
}

void
BufferManager::Flush() {
    // TODO implement me
}

PageNumber
BufferManager::GetPageNumber(BufferId bufid) const {
    ASSERT(bufid != INVALID_BUFID /* && is valid for this instance */);
    // TODO implement me
    return INVALID_PID;
}

char*
BufferManager::GetBuffer(BufferId bufid) const {
    ASSERT(bufid != INVALID_BUFID /* && is valid for this instance */);
    // TODO implement me
    return nullptr;
}

}   // namespace taco
