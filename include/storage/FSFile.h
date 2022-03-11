#ifndef STORAGE_FSFILE_H
#define STORAGE_FSFILE_H

#include "tdb.h"

namespace taco {

/*!
 * Represents an open file in the file system.
 */
class FSFile {
public:
    /*!
     * Opens or creates a file in the file system for read and write. Returns a
     * non-null pointer on success. The boolean parameters \p o_trunc, \p
     * o_direct and \p o_creat specify whether O_TRUNC, O_DIRECT and O_CREAT
     * should be added to the \p flags passed to open(2).
     *
     * If any error occurs, returns a null pointer and errno may be set by the
     * failed syscall if the error is caused by the failed syscall. If the
     * error is not associated with any syscall failure, then errno == 0. See
     * `man 2 open' for details.
     *
     * The default permission of a created file is 0600.
     *
     * Note: this function modifies \p errno.
     */
    static FSFile *Open(const std::string& path, bool o_trunc,
                        bool o_direct, bool o_creat,
                        mode_t mode = 0600);

    /*!
     * Desctructs the FSFile. If the underlying file is still opened, it should
     * close the file automatically.
     */
    ~FSFile();

    /*!
     * Opens the file again. Returns `true' on success.  If any error occurs,
     * returns `false' and errno may be set by the failed syscall if the
     * error is caused by the failed syscall.  If the error is not associated
     * with any syscall failure, then \p errno == 0.
     *
     * Note: this function modifies \p errno.
     */
    bool Reopen();

    /*!
     * Closes the file if it is still open. It logs a warning if it fails to
     * close the file (the log message contains a text description of the error
     * returned by `strerror(errno)'). After the call, no function other than
     * Reopen(), Close() or the destructor may be called.
     */
    void Close();

    /*!
     * Returns whether this file is open.
     */
    bool IsOpen() const;

    /*!
     * Deletes the file from the file system. If it is currently open, it
     * remains to exist until it is closed.
     *
     * It logs a warning if the deletion fails and the log message contains a
     * text description from `strerror(errno)'.
     */
    void Delete() const;

    /*!
     * Reads \p count bytes from the file at the \p offset into the buffer \p
     * buf. It is a fatal error if the specified range [\p offset, \p offset +
     * \p count) falls out of the file. It is also a fatal error if the
     * underlying syscall fails or only partially reads the file.
     *
     * This function is thread-safe.
     */
    void Read(void *buf, size_t count, off_t offset);

    /*!
     * Writes \p count bytes from the buffer \p buf into the file at the \p
     * offset. It is a fatal error if the specified range [\p offset, \p offset
     * + \p count) falls out of the file. It is also a fatal error if the
     * underlying syscall fails or only partially writes the data.
     *
     * This function is thread-safe.
     */
    void Write(const void *buf, size_t count, off_t offset);

    /*!
     * Allocates \p count bytes at the end of the file and zeros those bytes.
     *
     * This function is **NOT** thread-safe. The caller is responsible for not
     * calling Allocate() from multiple threads. This function also does not
     * need to impose any memory order on the file size.
     */
    void Allocate(size_t count);

    /*!
     * Returns the size of the file.
     *
     * This function should be thread-safe but it does not need to impose any
     * memory order on the file size.
     */
    size_t Size() const noexcept;

    /*!
     * Flushes the data written to the disk. It is a fatal error if the flush
     * fails.
     */
    void Flush();

private:
    // TODO implement it
};

/*!
 * Calls fallocate(2) with mode FALLOC_FL_ZERO_RANGE if it is available.
 * Returns true only if the system has falloate(2); the file system supports
 * the mode and the allocation is successful. In case fallocate(2) exists but
 * fails, errno will be the one set by fallocate(2) call. Otherwise, errno is
 * set to 0 upon return.
 */
bool fallocate_zerofill_fast(int fd, off_t offset, off_t len);

}   // namespace taco

#endif      // STORAGE_FSFILE_H
