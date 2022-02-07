#include "base/TDBEnv.h"

#include <absl/flags/parse.h>
#include <absl/flags/usage.h>
#include <absl/flags/usage_config.h>

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);

    // parse our own flags
    absl::SetProgramUsageMessage(
        "\n"
        "see the help message from gtest and absl flags for the"
        " list of accepted flags"
    );

    //XXX designated initializer syntax is not available until c++20
    //and there's no gcc support with -std=gnu++11 at version 7.2.0
    absl::FlagsUsageConfig usage_conf;
    usage_conf.contains_help_flags = taco::FilePathIsTDBFilePath;
    absl::SetFlagsUsageConfig(usage_conf);
    absl::ParseCommandLine(argc, argv);

    // perform tdb's global initialization
    testing::AddGlobalTestEnvironment(new taco::TDBEnv);
    return RUN_ALL_TESTS();
}
