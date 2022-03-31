#ifndef TESTS_BASE_TDBDBTEST_H
#define TESTS_BASE_TDBDBTEST_H

#include "base/TDBNonDBTest.h"

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>

namespace taco {

extern std::string g_test_existing_db_path;

class TDBDBTest: public TDBNonDBTest {
protected:
    // Some tests might want to override a few flags that may affect the
    // behavior of Database::open() before calling SetUp() here (e.g., basic
    // tests that will initialize their own instances of buffer manager and/or
    // catcache).
    void SetUp() override;

    void TearDown() override;

    // Returns the buffer pool size set on the command line by default.
    // Override this if the test wants to specify its own buffer pool size.
    virtual size_t GetBufferPoolSize();
};

}    // namespace taco

#endif    // TESTS_BASE_TDBDBTEST_H
