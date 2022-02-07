#include "base/TDBDBTest.h"

namespace taco {

using BasicTestRepoCompilesAndRuns = TDBDBTest;

TEST_F(BasicTestRepoCompilesAndRuns, TestShouldAlwaysSucceed) {
    SUCCEED();
}

}   // namespace taco
