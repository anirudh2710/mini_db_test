// Basic tests for FSFile
#include "storage/BasicTestFSFile.h"

#include <cerrno>
#include <algorithm>
#include <iterator>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <absl/strings/str_format.h>

#include "storage/FSFile.h"
#include "utils/fsutils.h"
#include "utils/zerobuf.h"

namespace taco {

TEST_F(BasicTestFSFile, TestCreateFile) {
    TDB_TEST_BEGIN
    std::unique_ptr<FSFile> f;
    int errno_save;

    // create a nonexistent file with the default 0600 permission
    std::string fpath = GetFreshFilePath();
    EXPECT_NO_ERROR(f.reset(FSFile::Open(fpath, false, true, true)));
    EXPECT_NE(f.get(), nullptr)
        << absl::StrFormat("unable to create a new file: %s", strerror(errno));
    EXPECT_TRUE(f->IsOpen());
    EXPECT_EQ(f->Size(), 0u);
    EXPECT_NO_ERROR(f->Close());
    EXPECT_FALSE(f->IsOpen());

    // check if the file created looks good without using the FSFile
    // implementation
    int fd = open(fpath.c_str(), O_RDWR);
    errno_save = errno;
    ASSERT_NE(fd, -1) << "unable to open the newly created file: "
                      << strerror(errno_save);

    struct stat stat_buf;
    ASSERT_EQ(fstat(fd, &stat_buf), 0)
        << absl::StrFormat("unable to stat the newly created file: %s",
                strerror(errno));
    ASSERT_EQ(stat_buf.st_size, 0);
    // Note: we assume no one changes the default value 0600 for the mode
    // parameter in FSFile::Open(). If someone changes the default value, this
    // check will fail but we can just continue with the remainder of the test
    // with no issues.
    EXPECT_EQ(stat_buf.st_mode & 07777, 0600u);

    ssize_t nbytes_written = write(fd, g_zerobuf, PAGE_SIZE);
    errno_save = errno;
    ASSERT_EQ(nbytes_written , (ssize_t) PAGE_SIZE)
        << "unable to write to the newly created file: "
        << ((nbytes_written == -1) ? (strerror(errno_save))
            : (absl::StrFormat("partially written %u out of %u bytes",
                                (unsigned) nbytes_written,
                                (unsigned) PAGE_SIZE)));
    (void) close(fd);

    // open an already created file with no O_TRUNC -- it should just open
    // the existing file
    EXPECT_NO_ERROR(f.reset(FSFile::Open(fpath, false, true, true)))
        << absl::StrFormat("unable to open the new file: %s", strerror(errno));
    EXPECT_NE(f.get(), nullptr);
    EXPECT_TRUE(f->IsOpen());
    EXPECT_EQ(f->Size(), (size_t) nbytes_written);
    EXPECT_NO_ERROR(f->Close());
    EXPECT_FALSE(f->IsOpen());

    // open an already created file with O_TRUNC -- it should truncate the file
    EXPECT_NO_ERROR(f.reset(FSFile::Open(fpath, true, true, true)))
        << absl::StrFormat("unable to truncate the new file: %s",
                           strerror(errno));
    EXPECT_NE(f.get(), nullptr);
    EXPECT_TRUE(f->IsOpen());
    EXPECT_EQ(f->Size(), 0u);
    EXPECT_NO_ERROR(f->Close());
    EXPECT_FALSE(f->IsOpen());

    // create with a different permission -- test whether the open honors the
    // permission required. This needs to be a new file (otherwise the mode is
    // ignored in open(2)).
    fpath = GetFreshFilePath();
    EXPECT_NO_ERROR(f.reset(FSFile::Open(fpath, false, true, true, 0664)));
    EXPECT_NE(f.get(), nullptr);
    EXPECT_TRUE(f->IsOpen());
    EXPECT_NO_ERROR(f->Close());
    EXPECT_FALSE(f->IsOpen());

    ASSERT_EQ(stat(fpath.c_str(), &stat_buf), 0)
        << absl::StrFormat("unable to stat the newly created file: %s",
                strerror(errno));
    EXPECT_EQ(stat_buf.st_mode & 07777, 0664u);

    TDB_TEST_END
}

TEST_F(BasicTestFSFile, TestOpenFile) {
    TDB_TEST_BEGIN

    std::unique_ptr<FSFile> f;
    std::string fpath = MakeTempFile();

    // open an existing file
    EXPECT_NO_ERROR(f.reset(FSFile::Open(fpath, false, true, false, 0644)));
    EXPECT_NE(f.get(), nullptr)
        << absl::StrFormat("unable to open the existing file: %s",
                           strerror(errno));
    EXPECT_TRUE(f->IsOpen());
    EXPECT_EQ(f->Size(), 0u);
    EXPECT_NO_ERROR(f->Close());
    EXPECT_FALSE(f->IsOpen());

    // make sure the file permission doesn't change because we passed a
    // different mode 0644.
    struct stat stat_buf;
    ASSERT_EQ(stat(fpath.c_str(), &stat_buf), 0)
        << absl::StrFormat("unable to stat the newly created file: %s",
                strerror(errno));
    EXPECT_EQ(stat_buf.st_mode & 07777, 0600u);

    TDB_TEST_END
}

TEST_F(BasicTestFSFile, TestOpenDirectory) {
    TDB_TEST_BEGIN
    std::string dpath = m_tmpdir;
    std::unique_ptr<FSFile> f;

    // open a directory should fail with a null return value
    EXPECT_NO_ERROR(f.reset(FSFile::Open(dpath, false, true, false)));
    int errno_save = errno;
    EXPECT_EQ(f.get(), nullptr);
    EXPECT_EQ(errno_save, EISDIR);
    TDB_TEST_END
}

TEST_F(BasicTestFSFile, TestOpenNonexistentFile) {
    TDB_TEST_BEGIN
    std::string nonexistent_fpath = GetFreshFilePath();
    std::unique_ptr<FSFile> f;

    // open a non-existent file should fail with null return value
    EXPECT_NO_ERROR(f.reset(FSFile::Open(nonexistent_fpath, false, true, false)));
    int errno_save = errno;
    EXPECT_EQ(f.get(), nullptr);
    EXPECT_EQ(errno_save, ENOENT);
    TDB_TEST_END
}

TEST_F(BasicTestFSFile, TestOpenFileWithInsufficientPermission) {
    TDB_TEST_BEGIN
    std::string fpath = MakeTempFile();
    std::unique_ptr<FSFile> f;

    // tries to open a file with insufficient permission
    ASSERT_EQ(chmod(fpath.c_str(), 0400), 0)
        << absl::StrFormat("unable to change the mode of the existing file: %s",
                           strerror(errno));
    EXPECT_NO_ERROR(f.reset(FSFile::Open(fpath, false, true, false)));
    int errno_save = errno;
    EXPECT_EQ(f.get(), nullptr);
    EXPECT_EQ(errno_save, EACCES);

    // change it back
    ASSERT_EQ(chmod(fpath.c_str(), 0600), 0)
        << absl::StrFormat("unable to change the mode of the existing file: %s",
                           strerror(errno));

    TDB_TEST_END
}

TEST_F(BasicTestFSFile, TestReopen) {
    TDB_TEST_BEGIN
    std::string fpath = MakeTempFile();
    std::unique_ptr<FSFile> f;

    ASSERT_NO_ERROR(f.reset(FSFile::Open(fpath, false, true, false)));
    ASSERT_NE(f.get(), nullptr);
    EXPECT_TRUE(f->IsOpen());
    ASSERT_NO_ERROR(f->Close());
    EXPECT_FALSE(f->IsOpen());

    // write something into f
    int fd = open(fpath.c_str(), O_RDWR);
    int errno_save = errno;
    ASSERT_NE(fd, -1) << "unable to open the newly created file: "
                      << strerror(errno_save);

    ssize_t nbytes_written = write(fd, g_zerobuf, PAGE_SIZE);
    errno_save = errno;
    ASSERT_EQ(nbytes_written , (ssize_t) PAGE_SIZE)
        << "unable to write to the newly created file: "
        << ((nbytes_written == -1) ? (strerror(errno_save))
            : (absl::StrFormat("partially written %u out of %u bytes",
                                (unsigned) nbytes_written,
                                (unsigned) PAGE_SIZE)));
    (void) close(fd);

    EXPECT_FALSE(f->IsOpen());
    ASSERT_TRUE(f->Reopen())
        << absl::StrFormat("unable to reopen the file: %s", strerror(errno));
    EXPECT_TRUE(f->IsOpen());
    EXPECT_EQ(f->Size(), (size_t) nbytes_written)
        << "probably wrong file is reopened because size doesn't match what "
        << "we wrote";

    TDB_TEST_END
}

TEST_F(BasicTestFSFile, TestAutoClose) {
    TDB_TEST_BEGIN
    std::string fpath = MakeTempFile();

    std::unique_ptr<FSFile> f;

    std::vector<int> initial_fds = GetAllOpenFDs();

    // open the file and find the open fd of FSFile
    ASSERT_NO_ERROR(f.reset(FSFile::Open(fpath, false, true, false)));
    ASSERT_NE(f.get(), nullptr);

    std::vector<int> current_fds = GetAllOpenFDs();
    ASSERT_EQ(initial_fds.size() + 1, current_fds.size())
        << "expecting to have one more open file descriptor after "
           "FSFile::Open() call";

    std::vector<int> fd_set_diff;
    std::set_difference(current_fds.begin(), current_fds.end(),
                        initial_fds.begin(), initial_fds.end(),
                        std::back_inserter(fd_set_diff));
    ASSERT_EQ(fd_set_diff.size(), 1u)
        << "expecting to have one more open file descriptor after "
           "FSFile::Open() call";

    // openfd is the file descriptor opened by the FSFile
    int openfd = fd_set_diff.front();

    // now close the file manually, this should close the underlying fd
    ASSERT_NO_ERROR(f->Close());
    current_fds = GetAllOpenFDs();
    ASSERT_EQ(initial_fds.size(), current_fds.size())
        << "expecting to have the underlying fd " << openfd << " closed by "
           "FSFile::Close()";
    fd_set_diff.clear();
    std::set_difference(current_fds.begin(), current_fds.end(),
                        initial_fds.begin(), initial_fds.end(),
                        std::back_inserter(fd_set_diff));
    ASSERT_TRUE(fd_set_diff.empty())
        << "expecting to have the underlying fd " << openfd << " closed by "
           "FSFile::Close()";

    ASSERT_TRUE(f->Reopen())
        << absl::StrFormat("unable to reopen the file: %s", strerror(errno));
    current_fds = GetAllOpenFDs();
    ASSERT_EQ(initial_fds.size() + 1, current_fds.size())
        << "expecting to have one more open file descriptor after "
           "FSFile::Reopen() call";
    fd_set_diff.clear();
    std::set_difference(current_fds.begin(), current_fds.end(),
                        initial_fds.begin(), initial_fds.end(),
                        std::back_inserter(fd_set_diff));
    ASSERT_EQ(fd_set_diff.size(), 1u)
        << "expecting to have one more open file descriptor after "
           "FSFile::Reopen() call";
    openfd = fd_set_diff.front();

    // now test the automatic close of the file through the destructor
    f.reset();
    current_fds = GetAllOpenFDs();
    ASSERT_EQ(initial_fds.size(), current_fds.size())
        << "expecting to have the underlying fd " << openfd
        << " automatically closed by FSFile::~FSFile()";
    fd_set_diff.clear();
    std::set_difference(current_fds.begin(), current_fds.end(),
                        initial_fds.begin(), initial_fds.end(),
                        std::back_inserter(fd_set_diff));
    ASSERT_TRUE(fd_set_diff.empty())
        << "expecting to have the underlying fd " << openfd
        << " automatically closed by FSFile::~FSFile()";

    TDB_TEST_END
}

TEST_F(BasicTestFSFile, TestDoubleCloseWarning) {
    TDB_TEST_BEGIN

    std::string fpath = MakeTempFile();
    std::unique_ptr<FSFile> f;
    std::vector<int> initial_fds = GetAllOpenFDs();

    ASSERT_NO_ERROR(f.reset(FSFile::Open(fpath, false, true, false)));
    ASSERT_NE(f.get(), nullptr);
    std::vector<int> current_fds = GetAllOpenFDs();
    ASSERT_EQ(initial_fds.size() + 1, current_fds.size())
        << "expecting to have one more open file descriptor after "
           "FSFile::Open() call";
    std::vector<int> fd_set_diff;
    std::set_difference(current_fds.begin(), current_fds.end(),
                        initial_fds.begin(), initial_fds.end(),
                        std::back_inserter(fd_set_diff));
    ASSERT_EQ(fd_set_diff.size(), 1u)
        << "expecting to have one more open file descriptor after "
           "FSFile::Open() call";

    // openfd is the file descriptor opened by the FSFile
    int openfd = fd_set_diff.front();

    // close the fd outside the FSFile
    ASSERT_EQ(close(openfd), 0)
        << "failed to close the open file descriptor";

    EnableCaptureWarning();
    // close again, we'd expect a warning message with strerror(EBADF)
    ASSERT_NO_ERROR(f->Close());
    std::string warning_msg = CapturedMessage();
    EXPECT_THAT(warning_msg, AllOf(
        StartsWith(absl::StrFormat("[%s]", absl::LogSeverityName(kWarning))),
        HasSubstr(strerror(EBADF))));

    TDB_TEST_END
}

TEST_F(BasicTestFSFile, TestDoubleClose) {
    TDB_TEST_BEGIN

    std::string fpath = MakeTempFile();
    std::unique_ptr<FSFile> f;
    std::vector<int> initial_fds = GetAllOpenFDs();

    // open the file and find the open fd of FSFile
    ASSERT_NO_ERROR(f.reset(FSFile::Open(fpath, false, true, false)));
    ASSERT_NE(f.get(), nullptr);
    std::vector<int> current_fds = GetAllOpenFDs();
    ASSERT_EQ(initial_fds.size() + 1, current_fds.size())
        << "expecting to have one more open file descriptor after "
           "FSFile::Open() call";
    std::vector<int> fd_set_diff;
    std::set_difference(current_fds.begin(), current_fds.end(),
                        initial_fds.begin(), initial_fds.end(),
                        std::back_inserter(fd_set_diff));
    ASSERT_EQ(fd_set_diff.size(), 1u)
        << "expecting to have one more open file descriptor after "
           "FSFile::Open() call";

    // openfd is the file descriptor opened by the FSFile
    int openfd = fd_set_diff.front();

    // close the file manually
    ASSERT_NO_ERROR(f->Close());

    // and call open directly to open the same file
    int openfd2 = open(fpath.c_str(), O_RDWR);

    // We expect open(2) will reuse the same file descriptor here. If so,
    // we can reasonably test the double close. Otherwise, we log a warning
    // to stderr to warn that the double close test is not run.
    if (openfd != openfd2) {
        TestEnableLogging();
        LOG(kWarning,
            "open(2) doesn't seem to resue the same file descriptor after "
            "close(2) call. Skipping the double close test.");
        TestDisableLogging();
        return ;
    }

    // try to call Close() again on the closed file
    ASSERT_NO_ERROR(f->Close());

    // if the second Close() call calls close(2) on the same fd we had, we'd
    // be unable to call fstat() on it
    struct stat stat_buf;
    int fstat_res = fstat(openfd2, &stat_buf);
    EXPECT_EQ(fstat_res, 0)
        << "the file descriptor is probably double closed by FSFile::Close()";
    if (fstat_res != 0) {
        // errno should be EBADF because the file was double closed. If not,
        // we'd be unsure what happened, and we shouldn't rely on the test.
        if (errno != EBADF) {
            int errno_save = errno;
            TestEnableLogging();
            LOG(kWarning,
                "unexpected error: %s. Skipping the double close test.",
                strerror(errno_save));
            TestDisableLogging();
            return ;
        }

        // open the file again so that we can also test the destructor as well
        openfd2 = open(fpath.c_str(), O_RDWR);
        if (openfd != openfd2) {
            TestEnableLogging();
            LOG(kWarning,
                "open(2) doesn't seem to resue the same file descriptor after "
                "close(2) call. Skipping the double close test.");
            TestDisableLogging();
        }
    }

    // now try to destroy the FSFile and see if the underlying file descriptor
    // is double closed
    ASSERT_NO_ERROR(f.reset());
    fstat_res = fstat(openfd2, &stat_buf);
    EXPECT_EQ(fstat_res, 0)
        << "the file descriptor is probably double closed by FSFile::~FSFile()";
    if (fstat_res != 0) {
        // errno should be EBADF because the file was double closed. If not,
        // we'd be unsure what happened, and we shouldn't rely on the test.
        if (errno != EBADF) {
            int errno_save = errno;
            TestEnableLogging();
            LOG(kWarning,
                "unexpected error: %s. Skipping the double close test.",
                strerror(errno_save));
            TestDisableLogging();
            return ;
        }
    } else {
        // close our fd if no double close happened
        (void) close(openfd2);
    }

    TDB_TEST_END
}

TEST_F(BasicTestFSFile, TestRead) {
    TDB_TEST_BEGIN

    bool o_direct_requires_alignment = DoesDirectIORequiresAlignedBuffer();
    if (!o_direct_requires_alignment) {
        TestEnableLogging();
        LOG(kWarning, "Test for error handling with improperly aligned buffer "
                      "is skipped because file system does not require so "
                      "(e.g., NFS). Note that the tests on your final "
                      "will be run on a file system that requires 512-byte "
                      "aligned buffers.");
        TestDisableLogging();
    }
    std::string fpath = MakeTempFile();
    std::unique_ptr<FSFile> f;

    int fd = open(fpath.c_str(), O_RDWR);
    int errno_save = errno;
    ASSERT_NE(fd, -1) << "unable to open the newly created file: "
                      << strerror(errno_save);

    unique_malloced_ptr buf = unique_aligned_alloc(512, PAGE_SIZE);
    for (uint64_t n = 0; n < 4; ++n) {
        *((uint64_t*) buf.get()) = MAGIC + n;
        ssize_t nbytes_written = write(fd, buf.get(), PAGE_SIZE);
        errno_save = errno;
        ASSERT_EQ(nbytes_written , (ssize_t) PAGE_SIZE)
            << "unable to write to the newly created file: "
            << ((nbytes_written == -1) ? (strerror(errno_save))
                : (absl::StrFormat("partially written %u out of %u bytes",
                                    (unsigned) nbytes_written,
                                    (unsigned) PAGE_SIZE)));
    }
    (void) close(fd);

    // open the file
    ASSERT_NO_ERROR(f.reset(FSFile::Open(fpath, false, true, false)));
    ASSERT_NE(f.get(), nullptr);
    ASSERT_EQ(f->Size(), 4 * PAGE_SIZE);

    // read in random order
    for (uint64_t n : { 3, 2, 1, 0, 2, 3, 0, 1} ) {
        *((uint64_t*) buf.get()) = 0;
        off_t offset = PAGE_SIZE * n;
        EXPECT_NO_ERROR(f->Read(buf.get(), PAGE_SIZE, offset)) <<
            "read on page " << (n - 1);
        uint64_t expected_number = MAGIC + n;
        EXPECT_EQ(*((uint64_t*) buf.get()), expected_number)
            << "page " << n <<  " differs from what was written";
    }

    // test invalid parameters
    EXPECT_FATAL_ERROR(f->Read(buf.get(), PAGE_SIZE, -(off_t) PAGE_SIZE));
    EXPECT_FATAL_ERROR(f->Read(buf.get(), PAGE_SIZE, PAGE_SIZE * 4));
    EXPECT_FATAL_ERROR(f->Read(buf.get(), PAGE_SIZE, PAGE_SIZE * 4 - 512));
    // O_DIRECT requires aligned I/O so this one will have errno == EINVAL
    if (o_direct_requires_alignment) {
        EXPECT_FATAL_ERROR(f->Read(buf.get(), PAGE_SIZE - 1, 0));
    }

    TDB_TEST_END
}

TEST_F(BasicTestFSFile, TestAllocate) {
    TDB_TEST_BEGIN

    std::string fpath = MakeTempFile();
    std::unique_ptr<FSFile> f;
    unique_malloced_ptr buf = unique_aligned_alloc(512, PAGE_SIZE);

    ASSERT_NO_ERROR(f.reset(FSFile::Open(fpath, false, true, false)));
    ASSERT_NE(f.get(), nullptr);
    ASSERT_EQ(f->Size(), 0u);

    // allocate 10 pages
    ASSERT_NO_ERROR(f->Allocate(10 * PAGE_SIZE));
    // the file should have 10 pages now
    EXPECT_EQ(f->Size(), 10 * PAGE_SIZE);

    // open our own fd to check if the 10 pages are all zeros
    int fd = open(fpath.c_str(), O_RDWR | O_DIRECT);
    int errno_save = errno;
    ASSERT_NE(fd, -1) << "unable to open the newly created file: "
                      << strerror(errno_save);
    struct stat stat_buf;
    ASSERT_EQ(fstat(fd, &stat_buf), 0)
        << "unable to stat the file: " << strerror(errno);
    ASSERT_EQ((size_t) stat_buf.st_size, 10 * PAGE_SIZE);
    for (uint64_t n = 0; n < 10; ++n) {
        memset(buf.get(), 255, PAGE_SIZE);
        off_t offset = PAGE_SIZE * n;
        ssize_t nbytes_read = pread(fd, buf.get(), PAGE_SIZE, offset);
        EXPECT_EQ(nbytes_read, (ssize_t) PAGE_SIZE)
            << "read on page " << n << " failed: "
            << ((nbytes_read == -1)
                    ? strerror(errno)
                    : absl::StrFormat("partially read %lu bytes instead of "
                                      "%u bytes",
                                      nbytes_read,
                                      (uint32_t) PAGE_SIZE));
        size_t first_nonzero_off = std::find_if(
            (char*) buf.get(), (char*) buf.get() + PAGE_SIZE,
            [](char c) -> bool { return c != 0; }) - (char*) buf.get();
        EXPECT_EQ(first_nonzero_off, PAGE_SIZE)
            << "page " << n << " is not zeroed";
    }
    (void) close(fd);

    // test if the file size is still ok after reopen.
    ASSERT_NO_ERROR(f->Close());
    ASSERT_NO_ERROR(f->Reopen());
    EXPECT_EQ(f->Size(), 10 * PAGE_SIZE);

    // test if the file size is still ok after closing and opened as a new
    // FSFile
    EXPECT_NO_ERROR(f.reset());
    ASSERT_NO_ERROR(f.reset(FSFile::Open(fpath, false, true, false)));
    EXPECT_EQ(f->Size(), 10 * PAGE_SIZE);
    ASSERT_NO_ERROR(f->Close());

    TDB_TEST_END
}

TEST_F(BasicTestFSFile, TestWrite) {
    TDB_TEST_BEGIN

    bool o_direct_requires_alignment = DoesDirectIORequiresAlignedBuffer();
    if (!o_direct_requires_alignment) {
        TestEnableLogging();
        LOG(kWarning, "Test for error handling with improperly aligned buffer "
                      "is skipped because file system does not require so "
                      "(e.g., NFS). Note that the tests on your final "
                      "will be run on a file system that requires 512-byte "
                      "aligned buffers.");
        TestDisableLogging();
    }
    std::string fpath = MakeTempFile();
    std::unique_ptr<FSFile> f;

    ASSERT_NO_ERROR(f.reset(FSFile::Open(fpath, false, true, false)));
    ASSERT_NE(f.get(), nullptr);
    ASSERT_EQ(f->Size(), 0u);

    // To check the contents of the file, we open another fd instead of going
    // through FSFile::Read().
    int fd = open(fpath.c_str(), O_RDWR | O_DIRECT);
    int errno_save = errno;
    ASSERT_NE(fd, -1) << "unable to open the newly created file: "
                      << strerror(errno_save);


    unique_malloced_ptr buf = unique_aligned_alloc(512, PAGE_SIZE);
    memset(buf.get(), 0, PAGE_SIZE);

    // write on an empty file should always fail
    EXPECT_FATAL_ERROR(f->Write(buf.get(), PAGE_SIZE, 0));

    // allocate 4 pages
    ASSERT_NO_ERROR(f->Allocate(4 * PAGE_SIZE));
    EXPECT_EQ(f->Size(), 4 * PAGE_SIZE);

    // test a few more invalid writes
    EXPECT_FATAL_ERROR(f->Write(buf.get(), PAGE_SIZE, -(off_t) PAGE_SIZE));
    EXPECT_FATAL_ERROR(f->Write(buf.get(), PAGE_SIZE, PAGE_SIZE * 4));
    EXPECT_FATAL_ERROR(f->Write(buf.get(), PAGE_SIZE, PAGE_SIZE * 4 - 512));
    // O_DIRECT requires aligned I/O so this one will have errno == EINVAL
    if (o_direct_requires_alignment) {
        EXPECT_FATAL_ERROR(f->Write(buf.get(), PAGE_SIZE - 1, 0));
    }

    // make sure the file is still all zero and has exactly four pages
    struct stat stat_buf;
    ASSERT_EQ(fstat(fd, &stat_buf), 0)
        << "unable to stat the file: " << strerror(errno);
    EXPECT_EQ((size_t) stat_buf.st_size, 4 * PAGE_SIZE);
    for (uint64_t n : { 3, 1, 2, 0} ) {
        *((uint64_t*) buf.get()) = 12345;
        off_t offset = PAGE_SIZE * n;
        ssize_t nbytes_read = pread(fd, buf.get(), PAGE_SIZE, offset);
        EXPECT_EQ(nbytes_read, (ssize_t) PAGE_SIZE)
            << "read on page " << n << " failed: "
            << ((nbytes_read == -1)
                    ? strerror(errno)
                    : absl::StrFormat("partially read %lu bytes instead of "
                                      "%u bytes",
                                      nbytes_read,
                                      (uint32_t) PAGE_SIZE));
        uint64_t expected_number = 0;
        EXPECT_EQ(*((uint64_t*) buf.get()), expected_number)
            << "page " << n <<  " was overwritten "
            "even if FSFile::Write() failed";
    }

    // write in random order
    for (uint64_t n : { 0, 2, 3, 1} ) {
        *((uint64_t *) buf.get()) = MAGIC + n;
        off_t offset = PAGE_SIZE * n;
        ASSERT_NO_ERROR(f->Write(buf.get(), PAGE_SIZE, offset)) <<
            "write on page " << n;
    }

    // make sure the contents are right when we read it back
    for (uint64_t n : { 3, 2, 1, 0, 2, 3, 0, 1} ) {
        *((uint64_t*) buf.get()) = 0;
        off_t offset = PAGE_SIZE * n;
        ssize_t nbytes_read = pread(fd, buf.get(), PAGE_SIZE, offset);
        EXPECT_EQ(nbytes_read, (ssize_t) PAGE_SIZE)
            << "read on page " << n << " failed: "
            << ((nbytes_read == -1)
                    ? strerror(errno)
                    : absl::StrFormat("partially read %lu bytes instead of "
                                      "%u bytes",
                                      nbytes_read,
                                      (uint32_t) PAGE_SIZE));
        uint64_t expected_number = MAGIC + n;
        EXPECT_EQ(*((uint64_t*) buf.get()), expected_number)
            << "page " << n <<  " differs from what was written";
    }

    // overwrite a few pages
    for (uint64_t n : {1, 3}) {
        *((uint64_t *) buf.get()) = MAGIC - n;
        off_t offset = PAGE_SIZE * n;
        ASSERT_NO_ERROR(f->Write(buf.get(), PAGE_SIZE, offset))
            << "write on page " << n;
    }

    // check again to make sure the pages are right
    for (uint64_t n : { 3, 2, 1, 0, 2, 3, 0, 1} ) {
        *((uint64_t*) buf.get()) = 0;
        off_t offset = PAGE_SIZE * n;
        ssize_t nbytes_read = pread(fd, buf.get(), PAGE_SIZE, offset);
        EXPECT_EQ(nbytes_read, (ssize_t) PAGE_SIZE)
            << "read on page " << n << " failed: "
            << ((nbytes_read == -1)
                    ? strerror(errno)
                    : absl::StrFormat("partially read %lu bytes instead of "
                                      "%u bytes",
                                      nbytes_read,
                                      (uint32_t) PAGE_SIZE));
        uint64_t expected_number = (n & 1) ? (MAGIC - n) : (MAGIC + n);
        EXPECT_EQ(*((uint64_t*) buf.get()), expected_number)
            << "page " << n <<  " differs from what was written";
    }

    // close and open the file again
    ASSERT_NO_ERROR(f->Close());
    ASSERT_TRUE(f->Reopen());

    // test again with invalid arguments to write
    EXPECT_FATAL_ERROR(f->Write(buf.get(), PAGE_SIZE, -(off_t) PAGE_SIZE));
    EXPECT_FATAL_ERROR(f->Write(buf.get(), PAGE_SIZE, PAGE_SIZE * 4));
    EXPECT_FATAL_ERROR(f->Write(buf.get(), PAGE_SIZE, PAGE_SIZE * 4 - 512));
    // O_DIRECT requires aligned I/O so this one will have errno == EINVAL
    if (o_direct_requires_alignment) {
        EXPECT_FATAL_ERROR(f->Write(buf.get(), PAGE_SIZE - 1, 0));
    }

    // invalid writes shouldn't change the file
    EXPECT_EQ(f->Size(), 4 * PAGE_SIZE);
    ASSERT_EQ(fstat(fd, &stat_buf), 0)
        << "unable to stat the file: " << strerror(errno);
    EXPECT_EQ((size_t) stat_buf.st_size, 4 * PAGE_SIZE);

    // check one last time to make sure the pages are right
    for (uint64_t n : { 3, 2, 1, 0, 2, 3, 0, 1} ) {
        *((uint64_t*) buf.get()) = 0;
        off_t offset = PAGE_SIZE * n;
        ssize_t nbytes_read = pread(fd, buf.get(), PAGE_SIZE, offset);
        EXPECT_EQ(nbytes_read, (ssize_t) PAGE_SIZE)
            << "read on page " << n << " failed: "
            << ((nbytes_read == -1)
                    ? strerror(errno)
                    : absl::StrFormat("partially read %lu bytes instead of "
                                      "%u bytes",
                                      nbytes_read,
                                      (uint32_t) PAGE_SIZE));
        uint64_t expected_number = (n & 1) ? (MAGIC - n) : (MAGIC + n);
        EXPECT_EQ(*((uint64_t*) buf.get()), expected_number)
            << "page " << n <<  " differs from what was written";
    }

    (void) close(fd);
    EXPECT_NO_ERROR(f->Close());

    TDB_TEST_END
}

TEST_F(BasicTestFSFile, TestDelete) {
    TDB_TEST_BEGIN
    std::unique_ptr<FSFile> f;

    // delete a file after close
    std::string fpath = MakeTempFile(true);
    ASSERT_NO_ERROR(f.reset(FSFile::Open(fpath, false, true, false)));
    ASSERT_NE(f.get(), nullptr);
    ASSERT_NO_ERROR(f->Close());
    EXPECT_NO_ERROR(f->Delete());
    EXPECT_FALSE(file_exists(fpath.c_str()));

    // delete a file while it is still open
    fpath = MakeTempFile(true);
    ASSERT_NO_ERROR(f.reset(FSFile::Open(fpath, false, true, false)));
    ASSERT_NE(f.get(), nullptr);

    // open it through another fd so that we can test if the FSFile::Delete()
    // implementation does not close the file prematurely
    int fd = open(fpath.c_str(), O_RDWR | O_DIRECT);
    int errno_save = errno;
    ASSERT_NE(fd, -1) << "unable to open the newly created file: "
                      << strerror(errno_save);

    // the file is unlinked, but still open
    EXPECT_NO_ERROR(f->Delete());
    EXPECT_FALSE(file_exists(fpath.c_str()));

    unique_malloced_ptr buf = unique_aligned_alloc(512, PAGE_SIZE);
    memset(buf.get(), 0, PAGE_SIZE);
    *((uint64_t*) buf.get()) = MAGIC;
    ASSERT_NO_ERROR(f->Allocate(PAGE_SIZE));
    ASSERT_NO_ERROR(f->Write(buf.get(), PAGE_SIZE, 0));
    EXPECT_EQ(f->Size(), PAGE_SIZE);

    struct stat stat_buf;
    ASSERT_EQ(fstat(fd, &stat_buf), 0)
        << "unable to stat the file: " << strerror(errno);
    EXPECT_EQ((size_t) stat_buf.st_size, PAGE_SIZE);

    // read the page back to check whether the magic number is written
    *((uint64_t*) buf.get()) = 0;
    ssize_t nbytes_read = pread(fd, buf.get(), PAGE_SIZE, 0);
    ASSERT_EQ((size_t) nbytes_read, PAGE_SIZE)
            << "read on page 0 failed: "
            << ((nbytes_read == -1)
                    ? strerror(errno)
                    : absl::StrFormat("partially read %lu bytes instead of "
                                      "%u bytes",
                                      nbytes_read,
                                      (uint32_t) PAGE_SIZE));
    uint64_t expected_number = MAGIC;
    EXPECT_EQ(*((uint64_t*) buf.get()), expected_number)
        << "first uint64_t on page 0 doesn't match the magic number";

    ASSERT_EQ(close(fd), 0);
    ASSERT_NO_ERROR(f->Close());
    EXPECT_FALSE(file_exists(fpath.c_str()));
    TDB_TEST_END
}

TEST_F(BasicTestFSFile, TestDeleteFailureWarning) {
    TDB_TEST_BEGIN
    std::unique_ptr<FSFile> f;

    std::string fpath = MakeTempFile(true);
    ASSERT_NO_ERROR(f.reset(FSFile::Open(fpath, false, true, false)));
    ASSERT_NE(f.get(), nullptr);
    ASSERT_NO_ERROR(f->Close());

    // delete the file using unlink(2) after we close it in FSFile
    ASSERT_EQ(unlink(fpath.c_str()), 0)
        << "unable to unlink the file: " << strerror(errno);

    // FSFile::Delete() will fail now with ENOENT, but it shouldn't throws
    // any error/fatal error
    EnableCaptureWarning();
    EXPECT_NO_ERROR(f->Delete());
    std::string warning_msg = CapturedMessage();
    EXPECT_THAT(warning_msg, AllOf(
        StartsWith(absl::StrFormat("[%s]", absl::LogSeverityName(kWarning))),
        HasSubstr(strerror(ENOENT))));

    TDB_TEST_END
}

}   // namespace taco

