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
    errno = 0;

    int flags = O_RDWR;
    if (o_creat) {
        flags |= O_CREAT;
    }
    if (o_direct) {
        flags |= O_DIRECT;
    }
    if (o_trunc) {
        flags |= O_TRUNC;
    }

    int fd = open(path.c_str(), flags, mode);
    if (fd < 0) {
        return nullptr;
    }

    struct stat stat_buf;
    if (fstat(fd, &stat_buf) < 0) {
        close(fd);
        return nullptr;
    }

    size_t flen = stat_buf.st_size;
    return new FSFile(path, fd, flen, o_direct);
}

FSFile::~FSFile() {
    Close();
}

bool
FSFile::Reopen() {
    errno = 0;
    if (m_fd >= 0) {
        // cannot reopen an open file
        return false;
    }

    m_fd = open(m_path.c_str(), O_RDWR | O_DIRECT);
    if (m_fd < 0) {
        return false;
    }

    struct stat stat_buf;
    if (fstat(m_fd, &stat_buf) < 0) {
        int errno_save = errno;
        Close();
        errno = errno_save;
        return false;
    }
    m_flen.store(stat_buf.st_size, memory_order_relaxed);

    return true;
}

void
FSFile::Close() {
    if (m_fd >= 0) {
        if (close(m_fd) < 0) {
            char *errstr = strerror(errno);
            LOG(kWarning, "failed to close file %s: %s", m_path, errstr);
        }
        m_fd = -1;
    }
}

bool
FSFile::IsOpen() const {
    return m_fd >= 0;
}

void
FSFile::Delete() const {
    if (unlink(m_path.c_str()) < 0) {
        char *errstr = strerror(errno);
        LOG(kWarning, "failed to delete file %s: %s", m_path, errstr);
    }
}

void
FSFile::Read(void *buf, size_t count, off_t offset) {
    size_t flen = Size();
    if (offset < 0 || (size_t) offset >= flen || offset + count > flen) {
        LOG(kFatal,
            "reading [%lu, %ld) from file %s is out of its range [0, %lu)",
            offset, offset + count, m_path, flen);
    }
    ReadImpl(buf, count, offset);
}

void
FSFile::Write(const void *buf, size_t count, off_t offset) {
    size_t flen = Size();
    if (offset < 0 || (size_t) offset >= flen || offset + count > flen) {
        LOG(kFatal,
            "writing [%lu, %ld) into file %s is out of its range [0, %lu)",
            offset, offset + count, m_path, flen);
    }
    WriteImpl(buf, count, offset);
}

void
FSFile::Allocate(size_t count) {
    if (count == 0)
        return ;
    size_t flen = Size();

    // Implementation note:
    // I'm changing the former impl. from using poxis_fallocate(3) to
    // fallocate(2) or pwrite(2) because we'd want the page to be zero filled
    // (at least for the page headers) to be treated as unallocated pages. That
    // will be quite important for implementing crash recovery later on,
    // especially if we don't want to log file header updates.
    if (fallocate_zerofill_fast(m_fd, flen, count)) {
        m_flen.store(flen + count, memory_order_relaxed);
        return ;
    } else {
        if (errno != 0 && errno != EOPNOTSUPP) {
            char *errstr = strerror(errno);
            LOG(kFatal, "unable to extend file %s: %s", m_path, errstr);
        }
    }

    // falls back to writing zeros at the end of the file if fallocate does
    // not exists or does not work.
    while (count > 0) {
        size_t sz = std::min(count, g_zerobuf_size);
        WriteImpl(g_zerobuf, sz, flen);
        flen += sz;
        count -= sz;
    }
    m_flen.store(flen, memory_order_relaxed);
}

size_t
FSFile::Size() const noexcept {
    return m_flen.load(memory_order_relaxed);
}

void
FSFile::Flush() {
    if (fdatasync(m_fd) < 0) {
        char *errstr = strerror(errno);
        LOG(kFatal, "unable to flush data on file %s: %s", m_path, errstr);
    }
}

void
FSFile::ReadImpl(void *buf, size_t count, off_t offset) {
    ssize_t res;

    for (;;) {
        res = pread(m_fd, buf, count, offset);
        if (res < 0) {
            if (errno == EINTR) {
                continue;
            }

            char *errstr = strerror(errno);
            LOG(kFatal, "error when reading file %s: %s", m_path, errstr);
        } else {
            if ((size_t) res == count) {
                return ;
            }
            LOG(kFatal, "error when reading file %s: "
                "partially read %lu bytes when %lu requested at offset %lu",
                m_path, (size_t) res, count, offset);
        }
    }
}

void
FSFile::WriteImpl(const void *buf_, size_t count, off_t offset) {
    ssize_t res;
    const char *buf = (const char *) buf_;

    for (;;) {
        ASSERT(count > 0);
        res = pwrite(m_fd, buf, count, offset);
        if (res < 0) {
            if (errno == EINTR) {
                continue;
            }

            char *errstr = strerror(errno);
            LOG(kFatal, "error when writing file %s: %s", m_path, errstr);
        } else {
            if ((size_t) res == count) {
                return ;
            }

            // Partial write may not be an error. We Weed to write again to
            // find out. O_DIRECT requires 512-byte alignment, so we may also
            // have to write it again.
            if (m_o_direct)
                res = res / 512 * 512;
            count -= res;
            buf += res;
            offset += res;
        }
    }
}

}   // namespace taco
