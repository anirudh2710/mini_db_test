#include "storage/FSFile.h"

#include <fcntl.h>

#include <absl/flags/flag.h>


ABSL_FLAG(bool, test_never_call_fallocate, false,
          "If enabled, fallocate_zerofill_fast() will always return false "
          "without fallocate(2) even if it exists. This is used for testing "
          "only.");

namespace taco {

#ifdef HAVE_FALLOCATE
/*!
 * This flag is used to hint whether fallocate works in this system.  This
 * is merely an approximation as it might be set to false if fallocate is
 * first called in a file system that does not support
 * FALLOC_FL_ZERO_RANGE. Then we will still fall back to the default method
 * of writing zeros.
 */
static atomic_bool fallocate_works(true);
#endif

bool
fallocate_zerofill_fast(int fd, off_t offset, off_t len) {
#ifdef HAVE_FALLOCATE
    errno = 0;
    if (!absl::GetFlag(FLAGS_test_never_call_fallocate) &&
        fallocate_works.load(memory_order_relaxed)) {
        int res = fallocate(fd, FALLOC_FL_ZERO_RANGE, offset, len);
        if (res == 0) {
            // done
            return true;
        }

        if (errno == EOPNOTSUPP) {
            // fallocate(2) doesn't seem to work on this filesystem.
            // Fall back to the default slower approach.
            // However, we don't have to force every other thread to see a
            // consistent hint immediately since it is just an approximation.
            fallocate_works.store(false, memory_order_relaxed);
        }
    }
#endif

    // no fallocate(2) or a previous call set errno to EOPNOTSUPP
    return false;
}

}   // namespace taco
