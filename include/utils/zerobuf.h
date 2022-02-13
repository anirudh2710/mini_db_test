#ifndef UTILS_ZEROBUF_H
#define UTILS_ZEROBUF_H

#include "tdb.h"

namespace taco {

constexpr size_t g_zerobuf_size = 8 * PAGE_SIZE;
extern char g_zerobuf[g_zerobuf_size];

}   // namespace taco

#endif      // UTILS_ZEROBUF_H
