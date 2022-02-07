/*!
 * @file
 * Logging component for TDB. This file does not include tdb.h at the beginning
 * as it is always included into the project indirectly from base/tdb_base.h.
 *
 * This file does not have a header guard intentionally. This must be included
 * indirectly from base/tdb_base.h. Any definition in this file will be put in
 * namespace taco.
 */
#include "utils/misc.h"

#include <iostream>
#include <absl/base/log_severity.h>
#include <absl/strings/str_format.h>

namespace taco {

using absl::LogSeverity;
constexpr LogSeverity kInfo = LogSeverity::kInfo;
constexpr LogSeverity kWarning = LogSeverity::kWarning;
constexpr LogSeverity kError = LogSeverity::kError;
constexpr LogSeverity kFatal = LogSeverity::kFatal;

constexpr const size_t log_msg_buf_size = 8192;

// "[log_severity] " is at most of length 10, and there're '\n' and '\0' at the
// end.
constexpr const size_t log_msg_max_len = log_msg_buf_size - 12;

class TDBError {
public:
    TDBError(LogSeverity level, std::string &&str):
        m_level(level),
        m_msg(std::move(str)) {}

    constexpr LogSeverity
    GetSeverity() const {
        return m_level;
    }

    const std::string&
    GetMessage() const {
        return m_msg;
    }

// If in gtest, define an implicit conversion from TDBError to
// absl::string_view so that we can match it against string matchers.
#ifdef TDB_IN_TEST
    operator std::string() const {
        return m_msg;
    }
#endif

private:
    LogSeverity m_level;
    std::string m_msg;
};

/*!
 * Logs a message (which may not be an error despite what the name suggests).
 * This function is used by LOG() macro, and usually no one should directly
 * call LogError().
 */
void LogError(LogSeverity severity, std::string &&msg);

/*!
 * Sets the output stream where the log messages should be printed to.
 */
void SetLogOutput(std::ostream *log_out);

/*!
 * Restores the log output stream to std::cerr.
 */
void RestoreLogOutput();

/*!
 * Sets the minimum severity level of a log message to be printed.
 */
void SetLogPrintMinSeverity(LogSeverity min_severity);

/*!
 * Disable all log message printing. Note that, even if log message printing
 * is disabled, a log message at severity kError or a kFatal will stll cause
 * a TDBError exception to be thrown.
 */
void DisableLogPrint();

/*!
 * Sets the secondary output stream where logs messages additionally prints
 * with at least the specified severity.
 */
void SetSecondaryLogOutput(std::ostream *log_out, LogSeverity min_severity);

/*!
 * Clears the secondary output stream where logs messages additionally prints.
 */
void ClearSecondaryLogOutput();

/*!
 * LOG(LogSeverity level, const char *fmt, ...)
 *
 * Note: fmt must be a string literal or an absl::string_view. The level
 * variable is double evaluated.
 */
#ifdef TDB_IN_TEST
#define LOG(level, ...) \
    do { \
        ::taco::LogError(level, absl::StrFormat("[%s] %s:%d: " \
            CAR(__VA_ARGS__), \
            absl::LogSeverityName(level),\
            ::taco::StripSourcePath(__FILE__), __LINE__\
            IF_NONEMPTY_COMMA(CADR(__VA_ARGS__), ) CDR(__VA_ARGS__))); \
    } while (0)
#else
// don't log the file location when not in test
#define LOG(level, ...) \
    do { \
        ::taco::LogError(level, absl::StrFormat("[%s] " \
            CAR(__VA_ARGS__), \
            absl::LogSeverityName(level) \
            IF_NONEMPTY_COMMA(CADR(__VA_ARGS__), ) CDR(__VA_ARGS__))); \
    } while (0)
#endif  // TDB_IN_TEST



}   // namespace taco

