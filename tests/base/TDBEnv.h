#ifndef TESTS_BASE_TDBENV_H
#define TESTS_BASE_TDBENV_H

#include "base/tdb_test.h"

namespace taco {

/*!
 * The global initializations that should only run once across all tests. This
 * is set up in our own main function.
 */
class TDBEnv: public ::testing::Environment {
public:
    ~TDBEnv() override;

    void SetUp() override;

    void TearDown() override;
};

}   // namespace taco

#endif      // TESTS_BASE_TDBENV_H
