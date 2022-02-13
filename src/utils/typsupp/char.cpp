#ifndef UTILS_TYPSUPP_VARCHAR_H
#define UTILS_TYPSUPP_VARCHAR_H

#include "tdb.h"

#include "utils/builtin_funcs.h"
#include "utils/string_utils.h"

namespace taco {

static absl::string_view
remove_trailing_space(absl::string_view s) {
    size_t i = s.length();
    while (i > 0 && s[i - 1] == ' ') --i;
    return s.substr(0, i);
}

BUILTIN_RETTYPE(CHAR)
BUILTIN_FUNC(CHAR_in, 890)
BUILTIN_ARGTYPE(__STRING)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    uint64_t max_size = FMGR_TYPPARAM();
    size_t size = str.length();

    while (size > max_size && str[size - 1] == ' ') {
        --size;
    }
    if (size > max_size) {
        LOG(kError, "the length %lu of the given string exceeds "
                    "specified maximum %lu", size, max_size);
    }

    auto buffer = unique_malloc(max_size);
    memcpy(buffer.get(), str.data(), size);
    if (size < max_size) {
        memset((char*) buffer.get() + size, ' ', max_size - size);
    }
    return Datum::FromVarlenBytes(std::move(buffer), max_size);
}

BUILTIN_RETTYPE(__STRING)
BUILTIN_FUNC(CHAR_out, 891)
BUILTIN_ARGTYPE(CHAR)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    auto buffer = unique_malloc(str.size());
    memcpy(buffer.get(), str.data(), str.size());
    return Datum::FromVarlenBytes(std::move(buffer), str.size());
}

BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(CHAR_typlen, 892)
BUILTIN_ARGTYPE(UINT8)
{
    if (FMGR_ARG(0).isnull()) {
        LOG(kError, "NULL type parameter");
    }

    uint64_t max_size = FMGR_ARG(0).GetUInt64();
    if (max_size > (uint64_t) std::numeric_limits<FieldOffset>::max()) {
        LOG(kError, "CHAR does not support maximum length of %lu", max_size);
    }
    return Datum::From((int16_t) max_size);
}

BUILTIN_RETTYPE(VARCHAR)
BUILTIN_FUNC(CHAR_to_VARCHAR, 893)
BUILTIN_ARGTYPE(CHAR)
BUILTIN_OPR(IMPLICIT_CAST, CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    uint64_t max_size = FMGR_TYPPARAM();

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    absl::string_view &&str_trunc = remove_trailing_space(str);
    if (max_size == 0) {
        // TODO implicit cast not implemented yet
        // we will use 0 to denote implicit casts inserted before function
        // calls
        max_size = str_trunc.size();
    } else {
        // otherwise, it is silently truncated to the max size
        if (str_trunc.size() > max_size) {
            str_trunc = str_trunc.substr(0, max_size);
        }
    }

    auto buffer = unique_malloc(str_trunc.size());
    memcpy(buffer.get(), str_trunc.data(), str_trunc.size());
    return Datum::FromVarlenBytes(std::move(buffer), str_trunc.size());
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(CHAR_eq, 894)
BUILTIN_ARGTYPE(CHAR, CHAR)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 =
        remove_trailing_space(FMGR_ARG(0).GetVarlenAsStringView());
    absl::string_view &&str1 =
        remove_trailing_space(FMGR_ARG(1).GetVarlenAsStringView());
    bool res = str0 == str1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(CHAR_ne, 895)
BUILTIN_ARGTYPE(CHAR, CHAR)
BUILTIN_OPR(NE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 =
        remove_trailing_space(FMGR_ARG(0).GetVarlenAsStringView());
    absl::string_view &&str1 =
        remove_trailing_space(FMGR_ARG(1).GetVarlenAsStringView());
    bool res = !(str0 == str1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(CHAR_lt, 896)
BUILTIN_ARGTYPE(CHAR, CHAR)
BUILTIN_OPR(LT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 =
        remove_trailing_space(FMGR_ARG(0).GetVarlenAsStringView());
    absl::string_view &&str1 =
        remove_trailing_space(FMGR_ARG(1).GetVarlenAsStringView());
    bool res = (str0 < str1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(CHAR_le, 897)
BUILTIN_ARGTYPE(CHAR, CHAR)
BUILTIN_OPR(LE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 =
        remove_trailing_space(FMGR_ARG(0).GetVarlenAsStringView());
    absl::string_view &&str1 =
        remove_trailing_space(FMGR_ARG(1).GetVarlenAsStringView());
    bool res = (str0 <= str1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(CHAR_gt, 898)
BUILTIN_ARGTYPE(CHAR, CHAR)
BUILTIN_OPR(GT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 =
        remove_trailing_space(FMGR_ARG(0).GetVarlenAsStringView());
    absl::string_view &&str1 =
        remove_trailing_space(FMGR_ARG(1).GetVarlenAsStringView());
    bool res = (str0 > str1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(CHAR_ge, 899)
BUILTIN_ARGTYPE(CHAR, CHAR)
BUILTIN_OPR(GE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 =
        remove_trailing_space(FMGR_ARG(0).GetVarlenAsStringView());
    absl::string_view &&str1 =
        remove_trailing_space(FMGR_ARG(1).GetVarlenAsStringView());
    bool res = (str0 >= str1);
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(CHAR_eq_ci, 900)
BUILTIN_ARGTYPE(CHAR, CHAR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 =
        remove_trailing_space(FMGR_ARG(0).GetVarlenAsStringView());
    absl::string_view &&str1 =
        remove_trailing_space(FMGR_ARG(1).GetVarlenAsStringView());
    return Datum::From(string_equal_ci(str0, str1));
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(CHAR_ne_ci, 901)
BUILTIN_ARGTYPE(CHAR, CHAR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 =
        remove_trailing_space(FMGR_ARG(0).GetVarlenAsStringView());
    absl::string_view &&str1 =
        remove_trailing_space(FMGR_ARG(1).GetVarlenAsStringView());
    return Datum::From(!string_equal_ci(str0, str1));
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(CHAR_lt_ci, 902)
BUILTIN_ARGTYPE(CHAR, CHAR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 =
        remove_trailing_space(FMGR_ARG(0).GetVarlenAsStringView());
    absl::string_view &&str1 =
        remove_trailing_space(FMGR_ARG(1).GetVarlenAsStringView());
    int res = string_compare_ci(str0, str1);
    return Datum::From(res < 0);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(CHAR_le_ci, 903)
BUILTIN_ARGTYPE(CHAR, CHAR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 =
        remove_trailing_space(FMGR_ARG(0).GetVarlenAsStringView());
    absl::string_view &&str1 =
        remove_trailing_space(FMGR_ARG(1).GetVarlenAsStringView());
    int res = string_compare_ci(str0, str1);
    return Datum::From(res <= 0);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(CHAR_gt_ci, 904)
BUILTIN_ARGTYPE(CHAR, CHAR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 =
        remove_trailing_space(FMGR_ARG(0).GetVarlenAsStringView());
    absl::string_view &&str1 =
        remove_trailing_space(FMGR_ARG(1).GetVarlenAsStringView());
    int res = string_compare_ci(str0, str1);
    return Datum::From(res > 0);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(CHAR_ge_ci, 905)
BUILTIN_ARGTYPE(CHAR, CHAR)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str0 =
        remove_trailing_space(FMGR_ARG(0).GetVarlenAsStringView());
    absl::string_view &&str1 =
        remove_trailing_space(FMGR_ARG(1).GetVarlenAsStringView());
    int res = string_compare_ci(str0, str1);
    return Datum::From(res >= 0);
}

}   // namespace taco

#endif      // UTILS_TYPSUPP_CHAR_H
