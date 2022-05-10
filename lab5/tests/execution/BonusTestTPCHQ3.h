#ifndef TESTS_EXECUTION_BONUSTESTTPCHQ3_H
#define TESTS_EXECUTION_BONUSTESTTPCHQ3_H

#include "execution/TPCHTest.h"

namespace taco {

// parameter: SEGMENT, DATE
class BonusTestTPCHQ3: public TPCHTest,
    public ::testing::WithParamInterface<std::pair<std::string, std::string>> {
protected:
    P GetPlan();

    std::string
    AnsSubDir() const override {
        return "q3";
    }
};

}

#endif      // TESTS_EXECUTION_BONUSTESTTPCHQ3_H
