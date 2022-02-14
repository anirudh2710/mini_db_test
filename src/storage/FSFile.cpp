#include "storage/FSFile.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>

#include "utils/zerobuf.h"

namespace taco {

FSFile*
FSFile::Open(const std::string& path, bool o_trunc,
             bool o_direct, bool o_creat, mode_t mode) {
    // Hint: wse open(2) to obtain a file descriptor of the file for read/write.
    // The file should be opened with O_RDWR flag per specification.
    // Run "man 2 open" in the shell for details.

    //TODO implement it
    return nullptr;
}

FSFile::~FSFile() {
    //TODO implement it
}

bool
FSFile::Reopen() {
    //TODO implement it
    return false;
}

void
FSFile::Close() {
    // Hint: use close(2)
    //TODO implement it
}

bool
FSFile::IsOpen() const {
    //TODO implement it
    return false;
}

void
FSFile::Delete() const {
    // Hint: use unlink(2)
    //TODO implement it
}

void
FSFile::Read(void *buf, size_t count, off_t offset) {
    // Hint: use pread(2)
    //TODO implement it
}

void
FSFile::Write(const void *buf, size_t count, off_t offset) {
    // Hint: use pwrite(2)
    //TODO implement it
}

void
FSFile::Allocate(size_t count) {
    // Hint: call fallocate_zerofill_fast() first to see if we can use
    // the faster fallocate(2) to extend the file.
    //
    // If it returns false and errno == EOPNOTSUPP (not supported by the file
    // system), fall back to writing `count' of zeros at the end of the file.
    // You may use g_zerobuf defined in utils/zerobuf.h as a large buffer that
    // is always all 0.
    //
    // If fallocate_zerofill_fast() returns false and errno is not either 0 or
    // EOPNOTSUPP, log a fatal error with strerror(errno) as a substring.

    //TODO implement it
}

size_t
FSFile::Size() const noexcept {
    // Hint: you may obtain the file size using stat(2)
    // FSFile::Size() is frequently called to determine the file size, so
    // you might want to cache the result of stat(2) in the file (but then
    // an FSFile::Allocate() call may extend it). You may assume no one may
    // extend or shrink the file externally when the database is running.

    //TODO implement it
    return ~(size_t) 0;
}

void
FSFile::Flush() {
    // Hint: use fsync(2) or fdatasync(2).
    //TODO implement it
}

}   // namespace taco
