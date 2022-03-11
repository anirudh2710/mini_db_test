#include "base/TDBDBTest.h"

#include <absl/flags/flag.h>

ABSL_FLAG(size_t, buffer_pool_size, 4096,
          "the default buffer pool size if it is not overriden by the test");

namespace taco {

std::string g_test_existing_db_path = "";

void
TDBDBTest::SetUp() {
    TDBNonDBTest::SetUp();

    if (g_db->is_open()) {
        EXPECT_NO_ERROR(g_db->close());
    }

    if (g_db->is_open()) {
        throw TDBTestSetUpFailure(
            "TDBDBTest::SetUp() failed to close the previously open database");
    }

    bool create = false;
    std::string datadir = g_test_existing_db_path;
    if (datadir.empty()) {
        datadir = MakeTempDir();
        create = true;
    } else {
        TestEnableLogging();
        LOG(kInfo, "loading existing db from %s", datadir);
        TestDisableLogging();
    }

    size_t buffer_size = GetBufferPoolSize();
    try {
        g_db->open(datadir, buffer_size, create, false);
    } catch (const TDBError &e) {
        if (create) {
            throw TDBTestSetUpFailure("failed to create a new database at " +
                                      datadir + ":\n" + e.GetMessage());
        } else {
            throw TDBTestSetUpFailure(
                "failed to open the existing database at " +
                datadir + ":\n" + e.GetMessage());
        }
    }

    if (!g_db->is_open()) {
        // the previous assert might have failed
        throw TDBTestSetUpFailure(
            "TDBDBTest::SetUp() failed to open a new database");
    }
}

void
TDBDBTest::TearDown() {
    ASSERT_NO_ERROR(g_db->close());
    TDBNonDBTest::TearDown();
}

size_t
TDBDBTest::GetBufferPoolSize() {
    return absl::GetFlag(FLAGS_buffer_pool_size);
}

}    // namespace taco
