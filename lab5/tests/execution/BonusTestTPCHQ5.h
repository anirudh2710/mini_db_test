#ifndef TESTS_EXECUTION_BONUSTESTTPCHQ5_H
#define TESTS_EXECUTION_BONUSTESTTPCHQ5_H

#include "execution/TPCHTest.h"

namespace taco {

// parameter: REGION, DATE
class BonusTestTPCHQ5: public TPCHTest,
    public ::testing::WithParamInterface<std::pair<std::string, std::string>> {
protected:
    P GetPlan();

    std::string
    AnsSubDir() const override {
        return "q5";
    }
};

}    // namespace taco

#endif      // TESTS_EXECUTION_BONUSTESTTPCHQ5_H
