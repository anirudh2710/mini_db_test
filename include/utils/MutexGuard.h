#ifndef UTILS_MUTEXGUARD_H
#define UTILS_MUTEXGUARD_H

#include "tdb.h"

#include <mutex>

#include "utils/ResourceGuard.h"

namespace taco {

struct MutexReleaseFunc {
    void operator()(std::mutex* m) const {
        m->unlock();
    }
};

/*!
 * MutexGuard is similar to std::lock_guard but it stores a pointer to
 * a mutex and allows one to store a nullptr instead.
 */
class MutexGuard: public ResourceGuard<std::mutex*, MutexReleaseFunc,
                                       std::mutex*, nullptr> {
public:
    MutexGuard():
        ResourceGuard() {}

    MutexGuard(std::mutex *m):
        ResourceGuard(
            // make sure we lock the mutex first before we pass it to the
            // ResourceGuard
            m ? (m->lock(), m) : nullptr
        ) {}
};

}   // namespace taco

#endif      // UTILS_MUTEXGUARD_H
