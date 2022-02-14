#ifndef TESTS_STORAGE_BASICTESTFSFILE_H
#define TESTS_STORAGE_BASICTESTFSFILE_H

#include "base/TDBNonDBTest.h"

#include <fcntl.h>
#include <dirent.h>
#include <unistd.h>

#include <absl/strings/str_format.h>
#include <absl/flags/declare.h>
#include <absl/flags/flag.h>

#include "utils/fsutils.h"
#include "utils/numbers.h"

ABSL_DECLARE_FLAG(bool, test_never_call_fallocate);

namespace taco {

class BasicTestFSFile: public TDBNonDBTest {
protected:
    void
    SetUp() override {
        // This forces the linker to link the flag into the executable, even if
        // the empty implementation of FSFile does not call
        // fallocate_zerofill_fast().  Otherwise our test case will complain of
        // unknown command line arg "--test_never_call_fallocate"
        (void) absl::GetFlag(FLAGS_test_never_call_fallocate);

        TDBNonDBTest::SetUp();

        // MakeTempDir() must not fail, or we can't move forward with testing.
        ASSERT_NO_ERROR(m_tmpdir = MakeTempDir());
        m_fno = 0;
    }

    void
    TearDown() override {
        std::vector<int> fds = GetAllOpenFDs();

        // close any unclosed fd in case we failed to clean that up
        for (int fd : fds) {
            // don't close stdin, stdout, and stderr
            if (fd <= 2)
                continue;
            (void) close(fd);
        }

        TDBNonDBTest::TearDown();
    }

    std::string
    GetFreshFilePath() {
        return absl::StrFormat("%s/tmpf.%06d", m_tmpdir, m_fno++);
    }

    std::vector<int>
    GetAllOpenFDs() {
        DIR *dir = opendir("/proc/self/fd");
        ASSERT(dir != nullptr);

        // need to exclude the file descriptor for the opened dir
        int myfd = dirfd(dir);
        ASSERT(myfd != -1);

        std::vector<int> fds;
        struct dirent *dp;
        errno = 0;
        while ((dp = readdir(dir))) {
            // skip "." and ".."
            if (dp->d_name[0] == '.') {
                if (dp->d_name[1] == '\0' ||
                    (dp->d_name[1] = '.' && dp->d_name[2] == '\0')) {
                    continue;
                }
            }

            int fd;
            bool res = SimpleAtoiWrapper(dp->d_name, &fd);
            ASSERT(res);

            // skip the opened dir's fd
            if (fd == myfd)
                continue;

            fds.push_back(fd);
        }
        ASSERT(errno == 0);

        int close_ret = closedir(dir);
        ASSERT(close_ret == 0);
        return fds;
    }

    /*!
     * Tests whether the file system requires 512-byte aligned buffer for
     * O_DIRECT I/O (e.g., NFS doesn't require it). Be conservative about the
     * test and if any syscall fails just return false, so that we disable the
     * a few tests for invalid argument errors in O_DIRECT I/O.
     */
    bool
    DoesDirectIORequiresAlignedBuffer() {
        std::string fpath = MakeTempFile();
        int fd = open(fpath.c_str(), O_RDWR | O_DIRECT);
        if (fd == -1)
            return false;

        unique_malloced_ptr buf = unique_aligned_alloc(512, PAGE_SIZE * 2);
        ssize_t res = pwrite(fd, (char*) buf.get() + 1, PAGE_SIZE, 0);
        bool alignment_required;
        if (res == -1 && errno == EINVAL) {
            alignment_required = true;
        } else {
            alignment_required = false;
        }

        close(fd);
        return alignment_required;
    }

    //! My private temporary directory.
    std::string m_tmpdir;

    int m_fno;

    static constexpr uint64_t MAGIC = 0xfaebdc0918274655ul;
};

}

#endif      // TESTS_STORAGE_BASICTESTFSFILE_H
