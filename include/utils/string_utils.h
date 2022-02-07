#ifndef UTILS_STRING_UTILS_H
#define UTILS_STRING_UTILS_H

#include "tdb.h"

#include <string.h>

namespace taco {

inline void
str_append_spaces(std::string &buf, int num_spaces)
{
    if (num_spaces == 0) return ;
    buf.resize(buf.size() + num_spaces, ' ');
}

inline int
string_compare_ci(const absl::string_view &s1, const absl::string_view &s2) {
    size_t n = std::min(s1.size(), s2.size());
    int res = strncasecmp(s1.data(), s2.data(), n);
    if (res == 0) {
        if (s1.size() < s2.size())
            return -1;
        if (s1.size() > s2.size())
            return 1;
        return 0;
    }
    return res;
}

inline bool
string_equal_ci(const absl::string_view &s1, const absl::string_view &s2) {
    if (s1.size() != s2.size())
        return false;
    int res = strncasecmp(s1.data(), s2.data(), s1.size());
    return res == 0;
}

}   // namespace taco

#endif      //UTILS_STRING_UTILS_H
