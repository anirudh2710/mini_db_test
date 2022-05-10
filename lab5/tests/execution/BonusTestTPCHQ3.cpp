#include "execution/BonusTestTPCHQ3.h"

#include <absl/strings/str_replace.h>

namespace taco {

TEST_P(BonusTestTPCHQ3, Run) {
    TDB_TEST_BEGIN

    P plan = GetPlan();
    RunPlan(plan.get());

    TDB_TEST_END
}

static const std::vector<std::pair<std::string, std::string>> params = {
    // Sample test cases.
    {"MACHINERY", "1993-03-10"},
    {"BUILDING", "1995-03-10"},
    {"AUTOMOBILE", "1995-03-10"},
    {"FURNITURE", "1998-03-10"},
    {"HOUSEHOLD", "1998-03-10"}
};

INSTANTIATE_TEST_SUITE_P(
    All,
    BonusTestTPCHQ3,
    ::testing::ValuesIn(params),
    [](const ::testing::TestParamInfo<BonusTestTPCHQ3::ParamType>& info) {
        return absl::StrCat(
            info.param.first, "_",
            absl::StrReplaceAll(info.param.second, {{"-", ""}}));
    }
);
}   // namespace taco

