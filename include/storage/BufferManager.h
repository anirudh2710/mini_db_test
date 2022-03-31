// storage/BufferManager.h
#pragma once

#include "tdb.h"

#include <absl/container/flat_hash_map.h>

#include "utils/Latch.h"
#include "utils/ResourceGuard.h"

namespace taco {

// private data structure
struct BufferMeta;

struct BufferUnpin {
    void operator()(BufferId bufid);
};

struct BufferUnlatch {
    void operator()(BufferId bufid);
};

/*!
 * An RAII-style guard for a pinned buffer frame, which is unpinned when it
 * goes out of its scope. A typical usage is:
 *
 *  {
 *      char *buf;
 *      ScopedBufferId bufid = g_bufman->PinPage(some_pid, &buf);
 *      // do something with the buffered page
 *  } // buffer pin automatically dropped at the end of the current scope
 *
 * Note that this is only meant to be a convenient tool for you to write
 * correct code, but you should always keep an eye on when and where the buffer
 * pin is dropped.  Suppose bufid is a ScopedBufferId, excplicitly calling
 * g_bufman->UnpinPage(bufid) is safe, as it resets it upon retrun.  However,
 * it is not safe to call g_bufman->UnpinPage(bufid.Get()) without resetting
 * it, because you will end up with a double "unpin" at the end of the scope.
 *
 */
using ScopedBufferId = ResourceGuard<BufferId, BufferUnpin,
                                     BufferId, INVALID_BUFID>;

/*!
 * You may ignore this type alias since we won't be implementing concurrency
 * control and recovery.
 */
using ScopedBufferLatch = ResourceGuard<BufferId, BufferUnlatch,
                                        BufferId, INVALID_BUFID>;

/*!
 * \p BufferManager implements a steal and no-force buffer pool with a fixed
 * number of page frames to buffer the pages in the regular files. It should
 * track which page is currently pinned and how many times it is currently
 * pinned, as well as whether it is dirty. When someone requests a pin on a
 * page, it should return either an existing buffer frame that buffers the
 * page, or returns a free buffer frame (possibly evicting some clean pages)
 * and reads the page from the file manager into the buffer frame.
 *
 * A note on buffer Id assignment: all valid values of the type BufferId except
 * `INVALID_BUFID` may be used for denote a buffer frame. There's no
 * restriction on how to assign BufferId values to each buffer frame (meaning
 * you could use any subset of values to denote the buffer frames). However,
 * the implementation should guarantee that, given the same buffer pool size,
 * 1) the same buffer frame is always denoted with the same buffer ID in a
 * BufferManager instance (i.e., the same buffer frame should be denoted with
 * the same buffer ID regardless what page it is holding); 2) and the set of
 * valid buffer ID stays the same in different BufferManager instances (i.e., a
 * valid (resp. invaild) buffer ID in a BufferManager should also be valid
 * (resp. invalid) in a BufferManager initialized with the same buffer pool
 * size).
 *
 * You may ignore any latch related functions since we won't be implementing
 * concurrency control and recovery.
 */
class BufferManager {
public:
    /*!
     * Constructs a buffer manager that is uninitialized. It must not throw
     * any error.
     */
    BufferManager();

    /*!
     * Destructs a buffer manager in any valid or invalid state. It must not
     * throw any error.
     */
    ~BufferManager();

    /*!
     * Initializes a buffer manager. It may throw errors, in which case, the
     * buffer manager is uninitialized and only the destructor may be called.
     *
     * Note: the buffer frames should be aligned to 512-byte boundries so that
     * they can be used for O_DIRECT I/O (see `man 2 open`).
     *
     * It is undefined to call Init() more than once.
     */
    void Init(BufferId buffer_size);


    /*!
     * Flushes all dirty pages back to disk and destroys the buffer manager.
     * This may perform clean ups that may fail and throw errors (such as
     * Flush()), in which case, the buffer manager is in an undefined state.
     * After calling Destroy(), one can only invoke the destructor on the
     * buffer manager.
     *
     * This should not fail when a previous Init() call fails or Init() was
     * never called.  However, it is undefined to call Destroy() more than
     * once.
     */
    void Destroy();

    /*!
     * Pins the page in a buffer frame and sets \p *frame to the buffer frame
     * (you may assume \p frame is never a `nullptr`).  Then it returns the
     * Buffer ID of the buffer frame where the requested page is pinned.
     *
     * PinPage() should always prefer an unpinned empty frame. If there's no
     * free buffer frame, it should try to evict an unpinned page from the
     * buffer. If the page is dirty, it should be written to the disk before it
     * is evicted. It is an error if all buffer frames are pinned.  It may also
     * throw other errors due to I/O errors in the underlying files. In either
     * case, the buffer manager should still be in a valid state.
     *
     */
    BufferId PinPage(PageNumber pid, char **frame);

    /*!
     * This overload of PinPage() calls the two-argument overload first and
     * then additionally whether the pinned page belong to the file with
     * `expected_fid`. If not, no page is pinned and it returns an
     * `INVALID_BUFID`. The value of \p *frame is also not modified. Otherwise,
     * it returns the buffer ID of the buffer frame with the requested page
     * pinned and sets \p *frame to the buffer frame.
     *
     * This is mainly used for testing and debugging but you may also use this
     * overload where appropriate. You do not need to implement it.
     */
    BufferId PinPage(PageNumber pid, char **frame, FileId expected_fid);

    /*!
     * Unpins the buffered page. Throws a fatal error if the buffer is not
     * pinned.  However, it is undefined if bufid is invalid or the buffer is
     * not pinned by the calling thread. It should not throw a non-fatal error.
     *
     */
    void UnpinPage(BufferId bufid);

    /*!
     * Unpins the buffered page. Throws a fatal error if the buffer is not
     * pinned.  However, it is undefined if bufid is invalid or the buffer is
     * not pinned by the calling thread. It should not throw a non-fatal error.
     *
     * This overload prevents double unpin when someone calls
     * BufferManager::UnpinPage on a ScopedBufferId.
     */
    void
    UnpinPage(ScopedBufferId &bufid) {
        bufid.Reset();
    }

    /*!
     * You may ignore this function since we won't be implementing concurrency
     * control and recovery.
     */
    BufferId LatchPage(BufferId bufid, LatchMode mode);

    /*!
     * You may ignore this function since we won't be implementing concurrency
     * control and recovery.
     */
    void UnlatchPage(BufferId bufid);

    /*!
     * You may ignore this function since we won't be implementing concurrency
     * control and recovery.
     */
    void UnlatchPage(ScopedBufferLatch bufid) {
        bufid.Reset();
    }

    /*!
     * Marks a buffered page as dirty. Throws a fatal error if the buffer is not
     * pinned. However, it is undefined if bufid is invalid or the
     * buffer is not pinned by the calling thread.

     */
    void MarkDirty(BufferId bufid);

    /*!
     * Returns the page number of the pinned page in the specified buffer
     * frame. Throws a fatal error if the buffer is not pinned. However, it is
     * undefined if bufid is invalid or the buffer is not pinned by the calling
     * thread.
     */
    PageNumber GetPageNumber(BufferId bufid) const;

    /*!
     * Returns a pointer to the buffer frame with the given buffer ID. It is
     * the same as the pointer stored in `*frame` in the previous
     * BufferManager::PinPage() call that returns the `bufid`. Throws a fatal
     * error if the buffer is not pinned. However, it is undefined if bufid is
     * invalid or the buffer is not pinned by the calling thread.
     */
    char* GetBuffer(BufferId bufid) const;

private:
    /*!
     * Flushes all dirty page back to disk. If there's still some page with
     * a non-zero pin count, it throws a fatal error.
     *
     * Note: this is only used for shutting down the database or testing, when
     * we are sure no pin is supposed to be held.
     */
    void Flush();

    BufferId GetFreeFrame();

    bool initialized;
    absl::flat_hash_map<PageNumber, BufferId> lookup_table;
    BufferId n;
    BufferId clock_hand;
    BufferMeta* metas;
    char* buffer_frames;

};

}   // namespace taco
