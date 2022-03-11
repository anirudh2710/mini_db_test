#ifndef UTILS_FSUTILS_H
#define UTILS_FSUTILS_H

#include "tdb.h"

// In utils/pgmkdirp.cpp. This is outside any namespace.
int pg_mkdir_p(char *path, int omode);

namespace taco {

bool file_exists(const char *path);
bool regular_file_exists(const char *path);
bool dir_exists(const char *path);
void remove_dir(const char *path);
bool dir_empty(const char *path);
void copy_dir(const char *srcpath, const char *dstpath);
std::string mktempfile(std::string prefix);
std::string mktempdir(std::string prefix);

}   // namespace taco

#endif      // UTILS_FSUTILS_H
