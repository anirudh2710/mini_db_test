#include "base/TDBEnv.h"

#include <sys/types.h>
#include <sys/stat.h>

#include <absl/flags/flag.h>

ABSL_FLAG(bool, disable_logs, true,
          "Whether to disable the log printing to stderr");

namespace taco {

bool g_unlink_failure = false;
bool g_unexpected_user_unlink_failure = false;

TDBEnv::~TDBEnv() {}

void
TDBEnv::SetUp() {
    // We always want to override the umask settings in the tests, as we assume
    // the open(2) calls will honor whatever mode we pass to it.
    umask(0);

    try {
        if (absl::GetFlag(FLAGS_disable_logs)) {
            DisableLogPrint();
        }

        // static initialization of the database
        Database::init_global();
    } catch (const TDBError &e) {
        FAIL() << "unexpected error: " << e.GetMessage();
    } catch (...) {
        FAIL() << "unexpected unknown exception";
    }
}

void
TDBEnv::TearDown() {
    // enable all messages at the end so that we can show any warning messages
    // we may need to print
    SetLogPrintMinSeverity(kInfo);

    try {
        g_db->close();
    } catch (const TDBError &e) {
        FAIL() << "unexpected error: " << e.GetMessage();
    } catch (...) {
        FAIL() << "unexpected unknown exception";
    }

    if (g_unlink_failure) {
        LOG(kWarning,
            "some temporary files/directories were not successfully removed");
    }

    if (g_unexpected_user_unlink_failure) {
        LOG(kWarning,
            "some temporary files/directories expected to be removed by the "
            "tests still exist");

    }
}

void
TestEnableLogging() {
    if (absl::GetFlag(FLAGS_disable_logs)) {
        SetLogPrintMinSeverity(kInfo);
    }
}

void
TestDisableLogging() {
    if (absl::GetFlag(FLAGS_disable_logs)) {
        DisableLogPrint();
    }
}

}   // namespace taco
