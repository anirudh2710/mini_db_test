#include "utils/zerobuf.h"

namespace taco {

alignas(512) char g_zerobuf[g_zerobuf_size];

}    // namespace taco

