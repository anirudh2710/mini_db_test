#ifndef UTILS_TYPSUPP_BOOL_H
#define UTILS_TYPSUPP_BOOL_H


#include "tdb.h"

#include <cinttypes>

#include <absl/strings/str_cat.h>
#include <absl/strings/numbers.h>

#include "utils/builtin_funcs.h"

namespace taco {

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(BOOL_in, 770)
BUILTIN_ARGTYPE(__STRING)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    absl::string_view &&str = FMGR_ARG(0).GetVarlenAsStringView();
    bool val;
    if (!absl::SimpleAtob(str, &val)) {
        LOG(kError, "cannot parse \"%s\" as a BOOL", str);
    }
    return Datum::From(val);
}

BUILTIN_RETTYPE(__STRING)
BUILTIN_FUNC(BOOL_out, 771)
BUILTIN_ARGTYPE(BOOL)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    if (FMGR_ARG(0).GetBool()) {
        return Datum::FromCString("true");
    }
    return Datum::FromCString("false");
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(BOOL_not, 772)
BUILTIN_ARGTYPE(BOOL)
BUILTIN_OPR(NOT)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From(!FMGR_ARG(0).GetBool());
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(BOOL_or, 773)
BUILTIN_ARGTYPE(BOOL, BOOL)
BUILTIN_OPR(OR)
{
    if (FMGR_ARG(0).isnull()) {
        if (FMGR_ARG(1).isnull() || !FMGR_ARG(1).GetBool()) {
            return Datum::FromNull();
        }
        return Datum::From(true);
    }

    if (FMGR_ARG(0).GetBool()) {
        return Datum::From(true);
    }
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }
    return Datum::From(FMGR_ARG(1).GetBool());
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(BOOL_and, 774)
BUILTIN_ARGTYPE(BOOL, BOOL)
BUILTIN_OPR(AND)
{
    if (FMGR_ARG(0).isnull()) {
        if (FMGR_ARG(1).isnull() || FMGR_ARG(1).GetBool()) {
            return Datum::FromNull();
        }
        return Datum::From(false);
    }

    if (!FMGR_ARG(0).GetBool()) {
        return Datum::From(false);
    }
    if (FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }
    return Datum::From(FMGR_ARG(1).GetBool());
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(BOOL_eq, 776)
BUILTIN_ARGTYPE(BOOL, BOOL)
BUILTIN_OPR(EQ)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From(FMGR_ARG(0).GetBool() == FMGR_ARG(1).GetBool());
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(BOOL_ne, 777)
BUILTIN_ARGTYPE(BOOL, BOOL)
BUILTIN_OPR(NE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From(FMGR_ARG(0).GetBool() != FMGR_ARG(1).GetBool());
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(BOOL_lt, 778)
BUILTIN_ARGTYPE(BOOL, BOOL)
BUILTIN_OPR(LT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From(FMGR_ARG(0).GetBool() < FMGR_ARG(1).GetBool());
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(BOOL_le, 779)
BUILTIN_ARGTYPE(BOOL, BOOL)
BUILTIN_OPR(LE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From(FMGR_ARG(0).GetBool() <= FMGR_ARG(1).GetBool());
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(BOOL_gt, 780)
BUILTIN_ARGTYPE(BOOL, BOOL)
BUILTIN_OPR(GT)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From(FMGR_ARG(0).GetBool() > FMGR_ARG(1).GetBool());
}

BUILTIN_RETTYPE(BOOL)
BUILTIN_FUNC(BOOL_ge, 781)
BUILTIN_ARGTYPE(BOOL, BOOL)
BUILTIN_OPR(GE)
{
    if (FMGR_ARG(0).isnull() || FMGR_ARG(1).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From(FMGR_ARG(0).GetBool() >= FMGR_ARG(1).GetBool());
}

}   // namespace taco

#endif      // UTILS_TYPSUPP_BOOL_H
