/*!
 * @file
 *
 * This files provides built-in support functions for `VARCHAR(n)' type. When
 * we are building with fixed-length data page only, a `VARCHAR(n)' datum is
 * represented as a `Varchar' struct. Otherwise, a `VARCHAR(n)' datum of length
 * `l' is stored as an array of bytes of length `l'.
 *
 */
#ifndef UTILS_TYPSUPP_VARCHAR_H
#define UTILS_TYPSUPP_VARCHAR_H

#include "tdb.h"

#include "utils/builtin_funcs.h"
#include "utils/string_utils.h"

namespace taco {

absl::string_view
varchar_to_string_view(DatumRef datum) {
    return datum.GetVarlenAsStringView();
}

BUILTIN_RETTYPE(VARCHAR)
BUILTIN_FUNC(VARCHAR_in, 860)
BUILTIN_ARGTYPE(__STRING)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    uint64_t max_size = FMGR_TYPPARAM();
    size_t size = str.length();

    // remove trailing space if the given string is longer than the maximum
    // length
    while (size > max_size && str[size - 1] == ' ')
        --size;
    if (size > max_size) {
        LOG(kError, "the length %lu of the given string exceeds "
                    "specified maximum %lu", size, max_size);
    }

    auto buffer = unique_malloc(size);
    memcpy(buffer.get(), str.data(), size);
    return Datum::FromVarlenBytes(std::move(buffer), size);
}

BUILTIN_RETTYPE(__STRING)
BUILTIN_FUNC(VARCHAR_out, 861)
BUILTIN_ARGTYPE(VARCHAR)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = varchar_to_string_view(FMGR_ARG(0));

    // must make a copy in the output function
    auto buffer = unique_malloc(str.size());
    memcpy(buffer.get(), str.data(), str.size());
    return Datum::FromVarlenBytes(std::move(buffer), str.size());
}

BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(VARCHAR_length, 863)
BUILTIN_ARGTYPE(VARCHAR)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = varchar_to_string_view(FMGR_ARG(0));
    return Datum::From((int16_t) str.size());
}

BUILTIN_RETTYPE(VARCHAR)
BUILTIN_FUNC(VARCHAR_concat, 864)
BUILTIN_ARGTYPE(VARCHAR, VARCHAR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 = varchar_to_string_view(FMGR_ARG(0));
    absl::string_view &&str1 = varchar_to_string_view(FMGR_ARG(1));
    size_t new_sz = str0.size() + str1.size();
    if (new_sz > (size_t) std::numeric_limits<FieldOffset>::max()) {
        LOG(kError, "string too long: %lu", new_sz);
    }

    char *str = (char*) malloc(new_sz);
    memcpy(str, str0.data(), str0.size());
    memcpy(str + str0.size(), str1.data(), str1.size());
    return Datum::FromVarlenBytes(unique_malloced_ptr(str), new_sz);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(VARCHAR_eq, 865)
BUILTIN_ARGTYPE(VARCHAR, VARCHAR)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 = varchar_to_string_view(FMGR_ARG(0));
    absl::string_view &&str1 = varchar_to_string_view(FMGR_ARG(1));
    bool res = str0 == str1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(VARCHAR_ne, 866)
BUILTIN_ARGTYPE(VARCHAR, VARCHAR)
BUILTIN_OPR(NE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 = varchar_to_string_view(FMGR_ARG(0));
    absl::string_view &&str1 = varchar_to_string_view(FMGR_ARG(1));
    bool res = !(str0 == str1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(VARCHAR_lt, 867)
BUILTIN_ARGTYPE(VARCHAR, VARCHAR)
BUILTIN_OPR(LT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 = varchar_to_string_view(FMGR_ARG(0));
    absl::string_view &&str1 = varchar_to_string_view(FMGR_ARG(1));
    bool res = (str0 < str1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(VARCHAR_le, 868)
BUILTIN_ARGTYPE(VARCHAR, VARCHAR)
BUILTIN_OPR(LE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 = varchar_to_string_view(FMGR_ARG(0));
    absl::string_view &&str1 = varchar_to_string_view(FMGR_ARG(1));
    bool res = (str0 <= str1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(VARCHAR_gt, 869)
BUILTIN_ARGTYPE(VARCHAR, VARCHAR)
BUILTIN_OPR(GT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 = varchar_to_string_view(FMGR_ARG(0));
    absl::string_view &&str1 = varchar_to_string_view(FMGR_ARG(1));
    bool res = (str0 > str1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(VARCHAR_ge, 870)
BUILTIN_ARGTYPE(VARCHAR, VARCHAR)
BUILTIN_OPR(GE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 = varchar_to_string_view(FMGR_ARG(0));
    absl::string_view &&str1 = varchar_to_string_view(FMGR_ARG(1));
    bool res = (str0 >= str1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(CHAR)
BUILTIN_FUNC(VARCHAR_to_CHAR, 871)
BUILTIN_ARGTYPE(VARCHAR)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint64_t max_size = FMGR_TYPPARAM();

    absl::string_view &&str = varchar_to_string_view(FMGR_ARG(0));
    if (max_size == 0) {
        max_size = str.size();
    } else {
        if (str.size() > max_size) {
            str = str.substr(0, max_size);
        }
    }

    auto buffer = unique_malloc(max_size);
    memcpy(buffer.get(), str.data(), str.size());
    // fill the trailing spaces
    if (str.size() < max_size)
        memset((char*) buffer.get() + str.size(), ' ', max_size - str.size());
    return Datum::FromVarlenBytes(std::move(buffer), max_size);
}

// compares a varchar to an internal string
// this is mostly used by the catalog cache and tests
BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(VARCHAR___STRING_eq, 872)
BUILTIN_ARGTYPE(VARCHAR, __STRING)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 = varchar_to_string_view(FMGR_ARG(0));
    absl::string_view &&str1 = FMGR_ARG(1).GetVarlenAsStringView();
    bool res = str0 == str1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(VARCHAR_eq_ci, 873)
BUILTIN_ARGTYPE(VARCHAR, VARCHAR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 = varchar_to_string_view(FMGR_ARG(0));
    absl::string_view &&str1 = varchar_to_string_view(FMGR_ARG(1));
    return Datum::From(string_equal_ci(str0, str1));
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(VARCHAR_ne_ci, 874)
BUILTIN_ARGTYPE(VARCHAR, VARCHAR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 = varchar_to_string_view(FMGR_ARG(0));
    absl::string_view &&str1 = varchar_to_string_view(FMGR_ARG(1));
    return Datum::From(!string_equal_ci(str0, str1));
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(VARCHAR_lt_ci, 875)
BUILTIN_ARGTYPE(VARCHAR, VARCHAR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 = varchar_to_string_view(FMGR_ARG(0));
    absl::string_view &&str1 = varchar_to_string_view(FMGR_ARG(1));
    int res = string_compare_ci(str0, str1);
    return Datum::From(res < 0);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(VARCHAR_le_ci, 876)
BUILTIN_ARGTYPE(VARCHAR, VARCHAR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 = varchar_to_string_view(FMGR_ARG(0));
    absl::string_view &&str1 = varchar_to_string_view(FMGR_ARG(1));
    int res = string_compare_ci(str0, str1);
    return Datum::From(res <= 0);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(VARCHAR_gt_ci, 877)
BUILTIN_ARGTYPE(VARCHAR, VARCHAR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 = varchar_to_string_view(FMGR_ARG(0));
    absl::string_view &&str1 = varchar_to_string_view(FMGR_ARG(1));
    int res = string_compare_ci(str0, str1);
    return Datum::From(res > 0);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(VARCHAR_ge_ci, 878)
BUILTIN_ARGTYPE(VARCHAR, VARCHAR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 = varchar_to_string_view(FMGR_ARG(0));
    absl::string_view &&str1 = varchar_to_string_view(FMGR_ARG(1));
    int res = string_compare_ci(str0, str1);
    return Datum::From(res >= 0);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(VARCHAR___STRING_eq_ci, 879)
BUILTIN_ARGTYPE(VARCHAR, __STRING)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 = varchar_to_string_view(FMGR_ARG(0));
    absl::string_view &&str1 = FMGR_ARG(1).GetVarlenAsStringView();
    return Datum::From(string_equal_ci(str0, str1));
}

}

#endif      // UTILS_TYPSUPP_VARCHAR_H
