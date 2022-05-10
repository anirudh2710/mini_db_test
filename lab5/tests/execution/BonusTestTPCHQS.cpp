#include "execution/BonusTestTPCHQS.h"

#include <absl/strings/str_replace.h>

namespace taco {

TEST_P(BonusTestTPCHQS, Run) {
    TDB_TEST_BEGIN

    P plan = GetPlan();
    RunPlan(plan.get());

    TDB_TEST_END
}

static const std::vector<int32_t> params = {5, 10, 30, 60, 90};

INSTANTIATE_TEST_SUITE_P(
    All,
    BonusTestTPCHQS,
    ::testing::ValuesIn(params),
    [](const ::testing::TestParamInfo<BonusTestTPCHQS::ParamType>& info) {
        return std::to_string(info.param);
    }
);
}   // namespace taco

