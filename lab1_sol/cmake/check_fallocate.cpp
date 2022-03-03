// We should have _GNU_SOURCE defined by default in both g++/clang++.
#include <fcntl.h>

int main() {
    int ret = fallocate(0, FALLOC_FL_ZERO_RANGE, 0, 100);
    return 0;
}

