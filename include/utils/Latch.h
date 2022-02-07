#ifndef UTILS_LATCH_H
#define UTILS_LATCH_H

#include "tdb.h"

#include <thread>
#include <mutex>

namespace taco {

enum class LatchMode {
    SH, // shared latch
    EX, // exclusive latch
};

}   // namespace taco

#endif  // UTILS_LATCH_H
