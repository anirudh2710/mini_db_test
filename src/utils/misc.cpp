// utils/misc.h does not include tdb.h by default, so it must be explicitly
// included as opposed to other source files where it's usually implicitly
// included
#include "tdb.h"

#include "utils/misc.h"

#include <fstream>
#include <cstdio>
#include <cinttypes>

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

namespace taco {

bool
FilePathIsTDBFilePath(absl::string_view filepath) {
    static std::string srcpath = std::string(SRCDIR).substr(1);
    static std::string external_srcpath = srcpath + "/external";
    return absl::StartsWith(filepath, srcpath) &&
        !absl::StartsWith(filepath, external_srcpath);
}

absl::string_view
StripSourcePath(absl::string_view path) {
    static std::string srcdir = absl::StrCat(SRCDIR, "/src/");
    static std::string includedir = absl::StrCat(SRCDIR, "/include/");
    static std::string generated_sourcedir =
        absl::StrCat(BUILDDIR, "/generated_source/");
    static std::string testdir = absl::StrCat(SRCDIR, "/tests/");

    if (absl::StartsWith(path, srcdir)) {
        return path.substr(srcdir.length());
    }

    if (absl::StartsWith(path, includedir)) {
        return path.substr(includedir.length());
    }

    if (absl::StartsWith(path, testdir)) {
        return path.substr(testdir.length());
    }

    if (absl::StartsWith(path, generated_sourcedir)) {
        return path.substr(generated_sourcedir.length());
    }

    return path;
}

uint64_t
GetCurrentDataSize() {
#ifdef USE_JEMALLOC
    static bool jemalloc_has_stats = true;
    if (jemalloc_has_stats) {
        size_t allocated;
        size_t allocated_sz = sizeof(size_t);
        uint64_t epoch = 1;
        size_t epoch_sz = sizeof(uint64_t);
        if (// passing epoch refreshes the data
            !mallctl("epoch",
                     (void*) &epoch, &epoch_sz,
                     (void*) &epoch, epoch_sz) &&
            !mallctl("stats.allocated",
                     (void*) &allocated, &allocated_sz,
                      nullptr, 0)) {
            return (uint64_t) allocated;
        } else {
            if (errno == ENOENT) {
                // jemalloc was probably not compiled with stats support.
                // fall back to VmData size
                jemalloc_has_stats = false;
                LOG(kWarning, "jemalloc is not compiled with stats");
            } else {
                LOG(kFatal, "mallctl failed: %s", strerror(errno));
            }
        }
    }
#endif

    // fall back to find VmData from /proc/self/status
    std::ifstream fin("/proc/self/status");
    std::string line;
    uint64_t vmdata;
    while (std::getline(fin, line)) {
        if (sscanf(line.c_str(), "VmData:\t%" SCNu64 "\tkB", &vmdata) == 1) {
            return vmdata * 1024;
        }
    }
    LOG(kFatal, "unable to find VmData from /proc/self/status");
    return 0;
}

}   // namespace taco
