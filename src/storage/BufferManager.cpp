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
        ind = 0;
        minfo = new meta_info[buffer_size];
        for (BufferId i = 0; i < buffer_size; i++)
        {
            minfo[i].dirty = false;
            minfo[i].pinCount = 0;
            minfo[i].pageNo = -1;
            minfo[i].sc = 1;
        }
        umap.clear();
        bufferSize = buffer_size;
        clockpointer = 0;
        secondChance = 0;
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
        // int metaInfoSize = sizeof minfo / sizeof minfo[0];
        // bufferpool[ind] = **frame;
        LOG(kInfo, "abc----pid %d", pid);
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
                    if (minfo[i].pinCount == 0)
                    {
                        anyFreeSpace = true;
                        break;
                    }
                }

                if (!anyFreeSpace)
                {
                    LOG(kError, "No frame prestnt");
                }

                while (true && secondChance > 0)
                {

                    LOG(kInfo, "abc dixita %d --------pid  --%d", clockpointer, pid);
                    clockpointer = (clockpointer + tempCount) % bufferSize;

                    if (minfo[clockpointer].pinCount == 0)
                    {
                        LOG(kInfo, "abc");
                        if (minfo[clockpointer].sc == 1)
                        {
                            minfo[clockpointer].sc = 0;
                            tempCount++;
                        }
                        else if (minfo[clockpointer].sc == 0)
                        {
                            LOG(kInfo, "abc");
                            if (minfo[clockpointer].dirty)
                            {
                                LOG(kInfo, "abc");
                                char *buf = bufferpool + (clockpointer)*PAGE_SIZE;

                                g_fileman->WritePage(minfo[clockpointer].pageNo, buf);
                                
                                
                            }
                            char *buf = bufferpool + (clockpointer)*PAGE_SIZE;
                            g_fileman->ReadPage(pid, buf);
                            *frame = buf;
                            LOG(kInfo, "abc");
                            umap.erase(minfo[clockpointer].pageNo);
                            umap[pid] = clockpointer;
                            tempCount++;
                            minfo[clockpointer].pageNo = pid;
                            minfo[clockpointer].dirty = false;
                            minfo[clockpointer].pinCount = 1;
                            minfo[clockpointer].sc = 1;
                            // ind--;
                            secondChance--;
                            return clockpointer;
                        }
                    }
                    else
                    {
                        tempCount++;
                    }
                }

                // non zero count
                if (secondChance == 0)
                {
                    LOG(kError, "not availabel ");
                }
            }
            else
            {
                // no eviction
                LOG(kInfo, "abc");
                minfo[ind].dirty = false;
                LOG(kInfo, "fdfd");
                minfo[ind].pageNo = pid;
                minfo[ind].pinCount = 1;
                minfo[ind].sc = 1;
                LOG(kInfo, "fdfd");
                char *buf = bufferpool + (ind)*PAGE_SIZE;
                g_fileman->ReadPage(pid, buf);
                *frame = buf;
                LOG(kInfo, "%d---------777-----", ind);
                LOG(kInfo, "------add-----88------%d", PAGE_SIZE);

                int bufferid = ind;
                umap[pid] = ind;
                ind = ind + 1;
                return bufferid;
            }
        }
        else
        {
            LOG(kInfo, "abc");
            auto it = umap.find(pid);

            int id = it->second;
            minfo[id].pinCount = minfo[id].pinCount + 1;
            minfo[id].sc = 1;
            minfo[id].pageNo = pid;
            umap[pid] = id;
            char *buf = bufferpool + (id)*PAGE_SIZE;
            // g_fileman->ReadPage(pid, buf);
            *frame = buf;
            LOG(kInfo, "jdkfdfkdf");
            return id;
        }
        LOG(kInfo, "abc");

        // TODO implement me
        return INVALID_BUFID;
    }

    void
    BufferManager::UnpinPage(BufferId bufid)
    {
        ASSERT(bufid != INVALID_BUFID /* && is valid for this instance */);
        LOG(kInfo, "%d unpin buffer id ----%d ---%d--", bufid, minfo[bufid].pinCount, umap.size());
        if (umap.size() > 0 && minfo[bufid].pinCount > 0)
        {
            LOG(kInfo, "unpins %d ", bufid);
            minfo[bufid].pinCount = minfo[bufid].pinCount - 1;
            if (minfo[bufid].pinCount == 0)
            {
                LOG(kInfo, "found 0----");
                minfo[bufid].sc = 1;
                secondChance++;
            }
        }
        else
        {

            LOG(kFatal, " It is undefined : %s", strerror(errno));
        }
    }

    void
    BufferManager::MarkDirty(BufferId bufid)
    {
        if (umap.size() == 0)
        {
            char *errstr = strerror(errno);
            LOG(kFatal, " It is undefined : %s", errstr);
        }
        else if (minfo[bufid].pinCount > 0)
        {
            minfo[bufid].dirty = true;
        }
        else
        {
            char *errstr = strerror(errno);
            LOG(kFatal, " It is undefined : %s", errstr);
        }
    }

    void
    BufferManager::Flush()
    {
        // TODO implement me
        LOG(kInfo, "inside flush dfjdkfdfd ---%d %d  ", ind, umap.size());
        // int count = 0;
        if (umap.size() > 0)
        {

            for (auto it = umap.begin(); it != umap.end(); it++)
            {
                int i = it->second;
                LOG(kInfo, "inside flush--- %d ---%d----%d", i, minfo[i].pinCount, minfo[i].dirty);
                if (minfo[i].dirty)
                {
                    LOG(kInfo, "is dirsty flush");

                    LOG(kInfo, "writing page ");
                    // minfo[i].pinCount = 0;
                    char *buf = bufferpool + (i)*PAGE_SIZE;
                    g_fileman->WritePage(minfo[i].pageNo, buf);
                    umap.erase(minfo[i].pageNo);
                    ind--;
                    LOG(kInfo, "no error while writing in flush %d ", umap.size());
                }
            }
        }

        for (auto it = umap.begin(); it != umap.end(); it++)
        {
            int i = it->second;
            LOG(kInfo, "checking %d ", i);
            if (minfo[i].pinCount > 0)
            {
                minfo[i].pinCount = 0;
                LOG(kInfo, "found pincount > 0 %d ", minfo[i].pinCount);
                char *errstr = strerror(errno);
                LOG(kFatal, " It is undefined - bufid- %s", errstr);
            }
        }
    }

    PageNumber
    BufferManager::GetPageNumber(BufferId bufid) const
    {
        ASSERT(bufid != INVALID_BUFID /* && is valid for this instance */);
        // TODO implement me
        LOG(kInfo, "dfd%d-----", minfo[bufid].pinCount);
        if (umap.size() > 0 && minfo[bufid].pinCount > 0)
        {
            return minfo[bufid].pageNo;
        }
        else
        {
            char *errstr = strerror(errno);
            LOG(kFatal, " It is undefined - bufid- %d : %s", bufid, errstr);
        }
        // TODO implement me
        return INVALID_PID;
    }

    char *
    BufferManager::GetBuffer(BufferId bufid) const
    {
        ASSERT(bufid != INVALID_BUFID /* && is valid for this instance */);
        // TODO implement me
        char *curP = bufferpool;
        LOG(kInfo, "Getting buffer for buff id %d", bufid);
        if (minfo[bufid].pinCount > 0)
        {
            curP = bufferpool + (bufid)*PAGE_SIZE;
            return curP;
        }
        else
        {
            char *errstr = strerror(errno);
            LOG(kFatal, " cannot find buffer for buffer id %d", bufid);
        }
        return nullptr;
    }
} // namespace taco
