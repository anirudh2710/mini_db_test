// src/storage/BufferManager.cpp

#include "storage/BufferManager.h"
#include "storage/FileManager.h"

namespace taco
{

    BufferManager::BufferManager()
    {
        // TODO implement me
        // BufferManager();
    }

    BufferManager::~BufferManager()
    {
        // TODO implement me
        Destroy();
    }

    void
    BufferManager::Init(BufferId buffer_size)
    {
        // BufferManager buf(); // calling default constructor
        bufferpool = (char *)aligned_alloc(512, buffer_size * PAGE_SIZE);
        indexPointer = bufferpool;
        temp = 0;
        info = new meta_info[buffer_size];
        for (BufferId i = 0; i < buffer_size; i++)
        {
            info[i].dirty = false;
            info[i].pinCount = 0;
            info[i].pageNo = -1;
            info[i].sc = 1;
        }
        umap.clear();
        bufferSize = buffer_size;
        clockpointer = 0;
        temp2 = 0;
    }

    void
    BufferManager::Destroy()
    {
        // TODO implement me
        Flush();
    }

    BufferId
    BufferManager::PinPage(PageNumber pid, char **frame)
    {
        ASSERT(frame);
        if (umap.find(pid) == umap.end())
        {
            LOG(kInfo, "abc");
            // int bufid = 0;
            //  clock eviction
            if (umap.size() == bufferSize)
            {
                int tempCount = 0;
                LOG(kInfo, "abc");
                // unsigned int nonZeroPinCount = 0;
                bool anyFreeSpace = false;
                int size = bufferSize;
                for (int i = 0; i < size; i++)
                {
                    if (info[i].pinCount == 0)
                    {
                        anyFreeSpace = true;
                        break;
                    }
                }

                if (!anyFreeSpace)
                {
                    LOG(kError, "No frame prestnt");
                }

                while (true && temp2 > 0)
                {
                    clockpointer = (clockpointer + tempCount) % bufferSize;

                    if (info[clockpointer].pinCount == 0)
                    {
                        if (info[clockpointer].sc == 1)
                        {
                            info[clockpointer].sc = 0;
                            tempCount++;
                        }
                        else if (info[clockpointer].sc == 0)
                        {
                            if (info[clockpointer].dirty)
                            {
                                char *buf = bufferpool + (clockpointer)*PAGE_SIZE;

                                g_fileman->WritePage(info[clockpointer].pageNo, buf);
                                
                                
                            }
                            char *buf = bufferpool + (clockpointer)*PAGE_SIZE;
                            g_fileman->ReadPage(pid, buf);
                            *frame = buf;
                            umap.erase(info[clockpointer].pageNo);
                            umap[pid] = clockpointer;
                            tempCount++;
                            info[clockpointer].pageNo = pid;
                            info[clockpointer].dirty = false;
                            info[clockpointer].pinCount = 1;
                            info[clockpointer].sc = 1;
                            temp2--;
                            return clockpointer;
                        }
                    }
                    else
                    {
                        tempCount++;
                    }
                }

                // non zero count
                if (temp2 == 0)
                {
                    LOG(kError, "absent");
                }
            }
            else
            {
                info[temp].dirty = false;
                info[temp].pageNo = pid;
                info[temp].pinCount = 1;
                info[temp].sc = 1;
                char *buf = bufferpool + (temp)*PAGE_SIZE;
                g_fileman->ReadPage(pid, buf);
                *frame = buf;
                int bufferid = temp;
                umap[pid] = temp;
                temp = temp+ 1;
                return bufferid;
            }
        }
        else
        {
            auto it = umap.find(pid);

            int id = it->second;
            info[id].pinCount = info[id].pinCount + 1;
            info[id].sc = 1;
            info[id].pageNo = pid;
            umap[pid] = id;
            char *buf = bufferpool + (id)*PAGE_SIZE;
            *frame = buf;
            return id;
        }

        // TODO implement me
        return INVALID_BUFID;
    }

    void
    BufferManager::UnpinPage(BufferId bufid)
    {
        ASSERT(bufid != INVALID_BUFID /* && is valid for this instance */);
        if (umap.size() > 0 && info[bufid].pinCount > 0)
        {
            info[bufid].pinCount = info[bufid].pinCount - 1;
            if (info[bufid].pinCount == 0)
            {
                info[bufid].sc = 1;
                temp2++;
            }
        }
        else
        {

            LOG(kFatal, " undefined : %s", strerror(errno));
        }
    }

    void
    BufferManager::MarkDirty(BufferId bufid)
    {
        if (umap.size() == 0)
        {
            char *errstr = strerror(errno);
        }
        else if (info[bufid].pinCount > 0)
        {
            info[bufid].dirty = true;
        }
        else
        {
            char *errstr = strerror(errno);
        }
    }

    void
    BufferManager::Flush()
    {
        // int count = 0;
        if (umap.size() > 0)
        {

            for (auto it = umap.begin(); it != umap.end(); it++)
            {
                int i = it->second;
                if (info[i].dirty)
                {
                    char *buf = bufferpool + (i)*PAGE_SIZE;
                    g_fileman->WritePage(info[i].pageNo, buf);
                    umap.erase(info[i].pageNo);
                    temp--;
                }
            }
        }

        for (auto it = umap.begin(); it != umap.end(); it++)
        {
            int i = it->second;
            LOG(kInfo, "checking %d ", i);
            if (info[i].pinCount > 0)
            {
                info[i].pinCount = 0;
                char *errstr = strerror(errno);

            }
        }
    }

    PageNumber
    BufferManager::GetPageNumber(BufferId bufid) const
    {
        ASSERT(bufid != INVALID_BUFID /* && is valid for this instance */);
        if (umap.size() > 0 && info[bufid].pinCount > 0)
        {
            return info[bufid].pageNo;
        }
        else
        {
            char *errstr = strerror(errno);
        }
        return INVALID_PID;
    }

    char *
    BufferManager::GetBuffer(BufferId bufid) const
    {
        ASSERT(bufid != INVALID_BUFID /* && is valid for this instance */);
        // TODO implement me
        char *curP = bufferpool;
        if (info[bufid].pinCount > 0)
        {
            curP = bufferpool + (bufid)*PAGE_SIZE;
            return curP;
        }
        else
        {
            char *errstr = strerror(errno);
        }
        return nullptr;
    }
} // namespace taco
