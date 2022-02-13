#include "base/TDBNonDBTest.h"

#include <absl/flags/flag.h>

#include "utils/fsutils.h"

ABSL_FLAG(std::string, tmpdir, BUILDDIR "/tmp",
          "The temporary directory where our test data should be placed. "
          "This directory must exists at the time the tests are run.");

ABSL_FLAG(bool, keep_tmps, false,
          "Whether to keep the temporary files and directories after the test "
          "finishes");

namespace taco {

// defined in base/TDBEnv.cpp
extern bool g_unlink_failure;
extern bool g_unexpected_user_unlink_failure;


void
TDBNonDBTest::SetUp() {
    TDB_TEST_BEGIN
    TDB_TEST_END
}

void
TDBNonDBTest::TearDown() {
    TDB_TEST_BEGIN
    if (m_oss.get()) {
        // restores the default settings for logging
        ClearSecondaryLogOutput();
    }

    if (!absl::GetFlag(FLAGS_keep_tmps)) {
        for (size_t i = 0; i < m_tmpfiles.size(); ++i) {
            const std::string &filepath = m_tmpfiles[i];
            if (unlink(filepath.c_str()) == 0) {
                if (m_tmpfiles_expect_removed[i]) {
                    g_unexpected_user_unlink_failure = true;
                }
            } else {
                if (!m_tmpfiles_expect_removed[i]) {
                    g_unlink_failure = true;
                }
            }
        }

        for (size_t i = 0; i < m_tmpdirs.size(); ++i) {
            const std::string &dirpath = m_tmpdirs[i];
            bool success = true;
            try {
                // remove_dir may throws fatal error if some operations failed
                // that's not a concern here
                remove_dir(dirpath.c_str());
            } catch (const TDBError &e) {
                success = false;
            }
            if (success) {
                if (m_tmpdirs_expect_removed[i]) {
                    g_unexpected_user_unlink_failure = true;
                }
            } else {
                if (!m_tmpdirs_expect_removed[i]) {
                    g_unlink_failure = true;
                }
            }
        }
    }

    TDB_TEST_END
}

void
TDBNonDBTest::CreateOrClearOutputStream() {
    if (!m_oss.get()) {
        m_oss = absl::make_unique<std::ostringstream>();
    } else {
        m_oss->str("");
        m_oss->clear();
    }
}

void
TDBNonDBTest::EnableCaptureWarning() {
    CreateOrClearOutputStream();
    SetSecondaryLogOutput(m_oss.get(), kWarning);
}

void
TDBNonDBTest::EnableCaptureInfo() {
    CreateOrClearOutputStream();
    SetSecondaryLogOutput(m_oss.get(), kInfo);
}

void
TDBNonDBTest::DisableCaptureLog() {
    ClearSecondaryLogOutput();
    if (m_oss.get()) {
        m_oss.reset();
    }
}

std::string
TDBNonDBTest::CapturedMessage() {
    if (m_oss.get()) {
        std::string &&s = m_oss->str();
        CreateOrClearOutputStream();
        return s;
    }
    return std::string();
}

std::string
TDBNonDBTest::MakeTempFile(bool expect_removed) {
    static std::string tmpfile_template =
        absl::GetFlag(FLAGS_tmpdir) + "/tmpf.";
    std::string &&fname = mktempfile(tmpfile_template);
    m_tmpfiles.push_back(fname);
    m_tmpfiles_expect_removed.push_back(expect_removed);
    return fname;
}

std::string
TDBNonDBTest::MakeTempDir(bool expect_removed) {
    static std::string tmpdir_template =
        absl::GetFlag(FLAGS_tmpdir) + "/tmpd.";
    std::string &&dname = mktempdir(tmpdir_template);
    m_tmpdirs.push_back(dname);
    m_tmpdirs_expect_removed.push_back(expect_removed);
    return dname;
}

}   // namespace taco
