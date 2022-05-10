#include "execution/BonusTestTPCHQ5.h"

#include <absl/strings/str_replace.h>

namespace taco {

TEST_P(BonusTestTPCHQ5, Run) {
    TDB_TEST_BEGIN

    P plan = GetPlan();
    RunPlan(plan.get());

    TDB_TEST_END
}

static const std::vector<std::pair<std::string, std::string>> params = {
    // Sample test cases.
    {"AFRICA", "1993-01-01"},
    {"AMERICA", "1994-01-01"},
    {"ASIA", "1995-01-01"},
    {"EUROPE", "1997-01-01"},
    {"MIDDLE EAST", "1998-06-01"},
};

INSTANTIATE_TEST_SUITE_P(
    All,
    BonusTestTPCHQ5,
    ::testing::ValuesIn(params),
    [](const ::testing::TestParamInfo<BonusTestTPCHQ5::ParamType>& info) {
        return absl::StrCat(
            absl::StrReplaceAll(info.param.first, {{" ", ""}}),
            "_", absl::StrReplaceAll(info.param.second, {{"-", ""}}));
    }
);
}   // namespace taco

