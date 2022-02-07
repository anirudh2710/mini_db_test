#include "base/logging.h"

namespace taco {

/*!
 * The minimum log severity that we'll print it to stderr. This is set to kINFO
 * by default. In the tests, this will be set to kFatal + 1, and thus disable
 * the printing.
 */
static int g_min_log_severity = (int) kInfo;

/*!
 * The output stream the log prints to.
 */
static std::ostream *g_log_out = &std::cerr;

/*!
 * The secondary output stream the log additionally prints to.
 */
static std::ostream *g_secondary_log_out = nullptr;

/*!
 * The minimum log severity that we'll additionally print to the secondary
 * outptu stream.
 */
static int g_secondary_min_log_severity = ((int) kFatal) + 1;

void
LogError(LogSeverity level, std::string &&str) {
    // log the error. the formatting is done in the LOG macro.
    if ((int) level >= g_min_log_severity)
        *g_log_out << str << std::endl;

    if ((int) level >= g_secondary_min_log_severity) {
        *g_secondary_log_out << str << std::endl;
    }

    // Anything with a severity level greater than error is a throwable error
    // The top-level exception handler in main or gtest should determine its
    // action upon discovering an error.
    if (level >= kError) {
        throw TDBError(level, std::move(str));
    }
}

void
SetLogOutput(std::ostream *log_out) {
    g_log_out = log_out;
}

void
RestoreLogOutput() {
    g_log_out = &std::cerr;
}

void
SetLogPrintMinSeverity(LogSeverity min_severity) {
    g_min_log_severity = (int) min_severity;
}

void
DisableLogPrint() {
    g_min_log_severity = ((int) kFatal) + 1;
}

void
SetSecondaryLogOutput(std::ostream *log_out, LogSeverity min_severity) {
    g_secondary_log_out = log_out;
    g_secondary_min_log_severity = (int) min_severity;
}

void
ClearSecondaryLogOutput() {
    g_secondary_min_log_severity = ((int) kFatal) + 1;
    g_secondary_log_out = nullptr;
}

}       // namespace taco
