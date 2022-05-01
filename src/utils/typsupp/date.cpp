#include "tdb.h"

#include <absl/time/civil_time.h>

#include "utils/builtin_funcs.h"
#include "utils/pg_datetime.h"

namespace taco {

BUILTIN_RETTYPE(DATE)
BUILTIN_FUNC(DATE_in, 940)
BUILTIN_ARGTYPE(__STRING)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    //XXX only support ISO 8601 date format for now
    absl::CivilDay d;
    if (!absl::ParseCivilTime(str, &d)) {
        LOG(kError, "unrecognized date value: %s", str);
    }

    //XXX no bound checking for the dates for now
    int jday = date2j(d.year(), d.month(), d.day());
    return Datum::From((int32_t) jday);
}

BUILTIN_RETTYPE(__STRING)
BUILTIN_FUNC(DATE_out, 941)
BUILTIN_ARGTYPE(DATE)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    int32_t jday = FMGR_ARG(0).GetInt32();
    int32_t year, month, day;
    j2date(jday, &year, &month, &day);

    absl::CivilDay d(year, month, day);
    std::string out = absl::FormatCivilTime(d);
    auto buffer = unique_malloc(out.length());
    memcpy(buffer.get(), out.c_str(), out.length());
    return Datum::FromVarlenBytes(std::move(buffer), out.length());
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(DATE_eq, 942)
BUILTIN_ARGTYPE(DATE, DATE)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t jday0 = FMGR_ARG(0).GetInt32();
    int32_t jday1 = FMGR_ARG(1).GetInt32();
    bool res = jday0 == jday1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(DATE_ne, 943)
BUILTIN_ARGTYPE(DATE, DATE)
BUILTIN_OPR(NE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t jday0 = FMGR_ARG(0).GetInt32();
    int32_t jday1 = FMGR_ARG(1).GetInt32();
    bool res = jday0 != jday1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(DATE_lt, 944)
BUILTIN_ARGTYPE(DATE, DATE)
BUILTIN_OPR(LT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t jday0 = FMGR_ARG(0).GetInt32();
    int32_t jday1 = FMGR_ARG(1).GetInt32();
    bool res = jday0 < jday1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(DATE_le, 945)
BUILTIN_ARGTYPE(DATE, DATE)
BUILTIN_OPR(LE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t jday0 = FMGR_ARG(0).GetInt32();
    int32_t jday1 = FMGR_ARG(1).GetInt32();
    bool res = jday0 <= jday1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(DATE_gt, 946)
BUILTIN_ARGTYPE(DATE, DATE)
BUILTIN_OPR(GT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t jday0 = FMGR_ARG(0).GetInt32();
    int32_t jday1 = FMGR_ARG(1).GetInt32();
    bool res = jday0 > jday1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(DATE_ge, 947)
BUILTIN_ARGTYPE(DATE, DATE)
BUILTIN_OPR(GE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t jday0 = FMGR_ARG(0).GetInt32();
    int32_t jday1 = FMGR_ARG(1).GetInt32();
    bool res = jday0 >= jday1;
    return Datum::From(res);
}

BUILTIN_RETTYPE(DATE)
BUILTIN_FUNC(DATE_adddays, 948)
BUILTIN_ARGTYPE(DATE, INT4)
BUILTIN_OPR(ADD)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t jday0 = FMGR_ARG(0).GetInt32();
    int32_t d = FMGR_ARG(1).GetInt32();
    return Datum::From(jday0 + d);
}

BUILTIN_RETTYPE(DATE)
BUILTIN_FUNC(DATE_subdays, 949)
BUILTIN_ARGTYPE(DATE, INT4)
BUILTIN_OPR(SUB)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    int32_t jday0 = FMGR_ARG(0).GetInt32();
    int32_t d = FMGR_ARG(1).GetInt32();
    return Datum::From(jday0 - d);
}

}   // namespace taco

