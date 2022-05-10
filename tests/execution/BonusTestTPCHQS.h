#ifndef TESTS_EXECUTION_BONUSTESTTPCHQS_H
#define TESTS_EXECUTION_BONUSTESTTPCHQS_H

#include "execution/TPCHTest.h"

namespace taco {

// parameter: INTERVAL
class BonusTestTPCHQS: public TPCHTest,
    public ::testing::WithParamInterface<int32_t> {
protected:
    P GetPlan();

    std::string
    AnsSubDir() const override {
        return "qs";
    }
};

}

#endif      // TESTS_EXECUTION_BONUSTESTTPCHQS_H
