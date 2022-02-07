#ifndef TESTS_BASE_TDBNONDBTEST_H
#define TESTS_BASE_TDBNONDBTEST_H

#include "base/tdb_test.h"

#include <sstream>

namespace taco {

/*!
 * The test fixture for tests that do not require DB access, or non-standard
 * DB access.
 */
class TDBNonDBTest: public ::testing::Test {
protected:
    void SetUp() override;

    void TearDown() override;

    template<class F>
    void ExpectNoUncaughtException(F &&f) {
        TDBError e = R(f);
        if (e.GetSeverity() != kNoError) {
            FAIL() << "uncaught test exception: " << e.GetMessage();
        }
    }

    //! Enables capturing of warning messages.
    void EnableCaptureWarning();

    //! Enables capturing of both info and warning messages.
    void EnableCaptureInfo();

    //! Disables all log message capturing.
    void DisableCaptureLog();

    /*!
     * Returns and clears the captured messages. If log message capturing is
     * not enabled, this returns an empty string.
     */
    std::string CapturedMessage();

    /*!
     * Creates a temporary file that will be automatically deleted after the
     * test (unless `--keep_tmps' flag is set) It returns its full path. The
     * created file has permission 0600 and is owned by the current user.
     *
     * If `expect_removed == false' (by default), a warning will be emitted at
     * the end of the test if the file can't be removed. If it is `true', a
     * a warning will be emitted if the file still exists and can be removed.
     */
    std::string MakeTempFile(bool expect_removed = false);

    /*!
     * Creates a temporary directory that will be automatically deleted after
     * the test (unless `--keep_tmps' flag is set). It returns its full path.
     * The created directory has permission 0700 and is owned by the current
     * user.
     *
     * If `expect_removed == false' (by default), a warning will be emitted at
     * the end of the test if the file can't be removed. If it is `true', a
     * a warning will be emitted if the file still exists and can be removed.
     */
    std::string MakeTempDir(bool expect_removed = false);

private:
    /*!
     * Creates the output stream if it doesn't exist or clears it.
     */
    void CreateOrClearOutputStream();

    std::unique_ptr<std::ostringstream> m_oss;

    std::vector<std::string> m_tmpdirs;

    std::vector<bool>       m_tmpdirs_expect_removed;

    std::vector<std::string> m_tmpfiles;

    std::vector<bool>       m_tmpfiles_expect_removed;
};

/*!
 * TDB_TEST_BEGIN and TDB_TEST_END wraps a block of testing code where we do
 * not expect any uncaught errors/unknown exceptions, e.g.,
 *
 * TEST_F(TDBNonDBTest, TestXXX) {
 *  TDB_TEST_BEGIN
 *  // some test
 *  TDB_TEST_END
 * }
 *
 * Usually you should always surround a test function body with TDB_TEST_BEGIN
 * and TDB_TEST_END unless you're sure there're none.
 */
#define TDB_TEST_BEGIN ExpectNoUncaughtException([&](){
#define TDB_TEST_END });

}    // namespace taco

#endif      // TESTS_BASE_TDBNONDBTEST_H
