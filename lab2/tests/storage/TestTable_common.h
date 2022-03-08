#ifndef STORAGE_TESTTABLE_COMMON_H
#define STORAGE_TESTTABLE_COMMON_H

#include "base/tdb_test.h"

#include "catalog/systables/initoids.h"

namespace taco {

static const Oid s_A_ncols = 2;
static const std::vector<Oid> s_A_coltypids = {
    initoids::TYP_UINT4, // int4 is not available in bootstrap catcache...
    initoids::TYP_VARCHAR
};
static const std::vector<uint64_t> s_A_coltypparam = {
    0, 26
};
static const std::vector<bool> s_A_colisnullable = { false, false };

};

#endif      // STORAGE_TESTTABLE_COMMON_H
