CSE462/562: Database Systems Course Project
====================================

Welcome to CSE462/562. In this semester long course project, you will build a
mini database systems in the TDB (Taco-DB) framework. This readme contains
a brief description of how to set up the build environment, compile and test
the system. Please refer to the [course
website](https://cse.buffalo.edu/~zzhao35/teaching/cse562_spring22) for
lab-specific instructions.

We have extensively tested the source code in the lab distribution but there
could inevitably be bugs as in any project. To report a BUG, please post a
**private** message in the Piazza folder bug_report to all TA and the
instructor. It will made public once validated. Please refer to the course
website for the policy of getting extra credit for successful reporting of
bugs.

# Dependencies and system requirements

git, cmake >= 3.13, pkg-config (0.29 recommended; <= 0.25 will make the build
extremely slow), c11 and c++11 compilers, e.g., gcc >= 7 or clang (not tested),
make, autoconf (for jemalloc), python3

We recommend using recent Linux distributions on x86_64 platform. It's known to
work on Ubuntu 20.04 and Fedora 35 with gcc.  It might not work as intended or
is known to not work on other systems even if it compiles. The following is a
non-exhaustive list:

    -OS: Mac OS, Windows WSL1, Cygwin, MSYS, Win32

    -Non-x86_64: Apple laptops with M1 processor,
    Microsoft Surface Pro X with SQ1 or SQ2

# How to build

To create a debug build in build/ directory, run

    cmake -B build .

On the first build, it will also build the [jemalloc](http://jemalloc.net/)
library and install it in **external/install** directory.

To create a release build in build.release/ directory, run

    cmake -DCMAKE_BUILD_TYPE=Release -B build.release .

# How to test your implementation

We use the [GoogleTest](http://google.github.io/googletest/) framework with its
community supported integration
[ctest](https://cmake.org/cmake/help/latest/manual/ctest.1.html). For advanced usages,
please review those documentations.

To run all test, in your build directory:

    ctest

To run an individual test, you may either use

    ctest -V -R <some_test_name_regex>

or directly run

    ./tests/path-to-some-test

To list the project specific flags, run

    ./tests/path-to-some-test --help

