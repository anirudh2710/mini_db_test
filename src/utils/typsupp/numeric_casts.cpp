#include "tdb.h"

#include <utils/builtin_funcs.h>

namespace taco {



BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(INT2_to_INT1, 1100)
BUILTIN_ARGTYPE(INT2)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int8_t)(FMGR_ARG(0).GetInt16()));
}



BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(INT4_to_INT1, 1101)
BUILTIN_ARGTYPE(INT4)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int8_t)(FMGR_ARG(0).GetInt32()));
}



BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(INT8_to_INT1, 1102)
BUILTIN_ARGTYPE(INT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int8_t)(FMGR_ARG(0).GetInt64()));
}



BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(INT1_to_INT2, 1103)
BUILTIN_ARGTYPE(INT1)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int16_t)(FMGR_ARG(0).GetInt8()));
}



BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(INT4_to_INT2, 1104)
BUILTIN_ARGTYPE(INT4)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int16_t)(FMGR_ARG(0).GetInt32()));
}



BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(INT8_to_INT2, 1105)
BUILTIN_ARGTYPE(INT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int16_t)(FMGR_ARG(0).GetInt64()));
}



BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(INT1_to_INT4, 1106)
BUILTIN_ARGTYPE(INT1)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int32_t)(FMGR_ARG(0).GetInt8()));
}



BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(INT2_to_INT4, 1107)
BUILTIN_ARGTYPE(INT2)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int32_t)(FMGR_ARG(0).GetInt16()));
}



BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(INT8_to_INT4, 1108)
BUILTIN_ARGTYPE(INT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int32_t)(FMGR_ARG(0).GetInt64()));
}



BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(INT1_to_INT8, 1109)
BUILTIN_ARGTYPE(INT1)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int64_t)(FMGR_ARG(0).GetInt8()));
}



BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(INT2_to_INT8, 1110)
BUILTIN_ARGTYPE(INT2)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int64_t)(FMGR_ARG(0).GetInt16()));
}



BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(INT4_to_INT8, 1111)
BUILTIN_ARGTYPE(INT4)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int64_t)(FMGR_ARG(0).GetInt32()));
}



BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(UINT1_to_INT1, 1112)
BUILTIN_ARGTYPE(UINT1)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int8_t)(FMGR_ARG(0).GetUInt8()));
}



BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(UINT2_to_INT1, 1113)
BUILTIN_ARGTYPE(UINT2)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int8_t)(FMGR_ARG(0).GetUInt16()));
}



BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(UINT4_to_INT1, 1114)
BUILTIN_ARGTYPE(UINT4)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int8_t)(FMGR_ARG(0).GetUInt32()));
}



BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(UINT8_to_INT1, 1115)
BUILTIN_ARGTYPE(UINT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int8_t)(FMGR_ARG(0).GetUInt64()));
}



BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(UINT1_to_INT2, 1116)
BUILTIN_ARGTYPE(UINT1)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int16_t)(FMGR_ARG(0).GetUInt8()));
}



BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(UINT2_to_INT2, 1117)
BUILTIN_ARGTYPE(UINT2)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int16_t)(FMGR_ARG(0).GetUInt16()));
}



BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(UINT4_to_INT2, 1118)
BUILTIN_ARGTYPE(UINT4)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int16_t)(FMGR_ARG(0).GetUInt32()));
}



BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(UINT8_to_INT2, 1119)
BUILTIN_ARGTYPE(UINT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int16_t)(FMGR_ARG(0).GetUInt64()));
}



BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(UINT1_to_INT4, 1120)
BUILTIN_ARGTYPE(UINT1)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int32_t)(FMGR_ARG(0).GetUInt8()));
}



BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(UINT2_to_INT4, 1121)
BUILTIN_ARGTYPE(UINT2)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int32_t)(FMGR_ARG(0).GetUInt16()));
}



BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(UINT4_to_INT4, 1122)
BUILTIN_ARGTYPE(UINT4)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int32_t)(FMGR_ARG(0).GetUInt32()));
}



BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(UINT8_to_INT4, 1123)
BUILTIN_ARGTYPE(UINT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int32_t)(FMGR_ARG(0).GetUInt64()));
}



BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(UINT1_to_INT8, 1124)
BUILTIN_ARGTYPE(UINT1)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int64_t)(FMGR_ARG(0).GetUInt8()));
}



BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(UINT2_to_INT8, 1125)
BUILTIN_ARGTYPE(UINT2)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int64_t)(FMGR_ARG(0).GetUInt16()));
}



BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(UINT4_to_INT8, 1126)
BUILTIN_ARGTYPE(UINT4)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int64_t)(FMGR_ARG(0).GetUInt32()));
}



BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(UINT8_to_INT8, 1127)
BUILTIN_ARGTYPE(UINT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int64_t)(FMGR_ARG(0).GetUInt64()));
}



BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(INT1_to_UINT1, 1128)
BUILTIN_ARGTYPE(INT1)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint8_t)(FMGR_ARG(0).GetInt8()));
}



BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(INT2_to_UINT1, 1129)
BUILTIN_ARGTYPE(INT2)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint8_t)(FMGR_ARG(0).GetInt16()));
}



BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(INT4_to_UINT1, 1130)
BUILTIN_ARGTYPE(INT4)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint8_t)(FMGR_ARG(0).GetInt32()));
}



BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(INT8_to_UINT1, 1131)
BUILTIN_ARGTYPE(INT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint8_t)(FMGR_ARG(0).GetInt64()));
}



BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(INT1_to_UINT2, 1132)
BUILTIN_ARGTYPE(INT1)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint16_t)(FMGR_ARG(0).GetInt8()));
}



BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(INT2_to_UINT2, 1133)
BUILTIN_ARGTYPE(INT2)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint16_t)(FMGR_ARG(0).GetInt16()));
}



BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(INT4_to_UINT2, 1134)
BUILTIN_ARGTYPE(INT4)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint16_t)(FMGR_ARG(0).GetInt32()));
}



BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(INT8_to_UINT2, 1135)
BUILTIN_ARGTYPE(INT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint16_t)(FMGR_ARG(0).GetInt64()));
}



BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(INT1_to_UINT4, 1136)
BUILTIN_ARGTYPE(INT1)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint32_t)(FMGR_ARG(0).GetInt8()));
}



BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(INT2_to_UINT4, 1137)
BUILTIN_ARGTYPE(INT2)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint32_t)(FMGR_ARG(0).GetInt16()));
}



BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(INT4_to_UINT4, 1138)
BUILTIN_ARGTYPE(INT4)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint32_t)(FMGR_ARG(0).GetInt32()));
}



BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(INT8_to_UINT4, 1139)
BUILTIN_ARGTYPE(INT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint32_t)(FMGR_ARG(0).GetInt64()));
}



BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(INT1_to_UINT8, 1140)
BUILTIN_ARGTYPE(INT1)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint64_t)(FMGR_ARG(0).GetInt8()));
}



BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(INT2_to_UINT8, 1141)
BUILTIN_ARGTYPE(INT2)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint64_t)(FMGR_ARG(0).GetInt16()));
}



BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(INT4_to_UINT8, 1142)
BUILTIN_ARGTYPE(INT4)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint64_t)(FMGR_ARG(0).GetInt32()));
}



BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(INT8_to_UINT8, 1143)
BUILTIN_ARGTYPE(INT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint64_t)(FMGR_ARG(0).GetInt64()));
}



BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT2_to_UINT1, 1144)
BUILTIN_ARGTYPE(UINT2)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint8_t)(FMGR_ARG(0).GetUInt16()));
}



BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT4_to_UINT1, 1145)
BUILTIN_ARGTYPE(UINT4)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint8_t)(FMGR_ARG(0).GetUInt32()));
}



BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(UINT8_to_UINT1, 1146)
BUILTIN_ARGTYPE(UINT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint8_t)(FMGR_ARG(0).GetUInt64()));
}



BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(UINT1_to_UINT2, 1147)
BUILTIN_ARGTYPE(UINT1)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint16_t)(FMGR_ARG(0).GetUInt8()));
}



BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(UINT4_to_UINT2, 1148)
BUILTIN_ARGTYPE(UINT4)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint16_t)(FMGR_ARG(0).GetUInt32()));
}



BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(UINT8_to_UINT2, 1149)
BUILTIN_ARGTYPE(UINT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint16_t)(FMGR_ARG(0).GetUInt64()));
}



BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(UINT1_to_UINT4, 1150)
BUILTIN_ARGTYPE(UINT1)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint32_t)(FMGR_ARG(0).GetUInt8()));
}



BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(UINT2_to_UINT4, 1151)
BUILTIN_ARGTYPE(UINT2)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint32_t)(FMGR_ARG(0).GetUInt16()));
}



BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(UINT8_to_UINT4, 1152)
BUILTIN_ARGTYPE(UINT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint32_t)(FMGR_ARG(0).GetUInt64()));
}



BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT1_to_UINT8, 1153)
BUILTIN_ARGTYPE(UINT1)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint64_t)(FMGR_ARG(0).GetUInt8()));
}



BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT2_to_UINT8, 1154)
BUILTIN_ARGTYPE(UINT2)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint64_t)(FMGR_ARG(0).GetUInt16()));
}



BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(UINT4_to_UINT8, 1155)
BUILTIN_ARGTYPE(UINT4)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint64_t)(FMGR_ARG(0).GetUInt32()));
}



BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(FLOAT_to_INT1, 1156)
BUILTIN_ARGTYPE(FLOAT)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int8_t)(FMGR_ARG(0).GetFloat()));
}

BUILTIN_RETTYPE(FLOAT)
BUILTIN_FUNC(INT1_to_FLOAT, 1157)
BUILTIN_ARGTYPE(INT1)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((float)(FMGR_ARG(0).GetInt8()));
}



BUILTIN_RETTYPE(INT1)
BUILTIN_FUNC(DOUBLE_to_INT1, 1158)
BUILTIN_ARGTYPE(DOUBLE)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int8_t)(FMGR_ARG(0).GetDouble()));
}

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(INT1_to_DOUBLE, 1159)
BUILTIN_ARGTYPE(INT1)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((double)(FMGR_ARG(0).GetInt8()));
}



BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(FLOAT_to_INT2, 1160)
BUILTIN_ARGTYPE(FLOAT)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int16_t)(FMGR_ARG(0).GetFloat()));
}

BUILTIN_RETTYPE(FLOAT)
BUILTIN_FUNC(INT2_to_FLOAT, 1161)
BUILTIN_ARGTYPE(INT2)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((float)(FMGR_ARG(0).GetInt16()));
}



BUILTIN_RETTYPE(INT2)
BUILTIN_FUNC(DOUBLE_to_INT2, 1162)
BUILTIN_ARGTYPE(DOUBLE)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int16_t)(FMGR_ARG(0).GetDouble()));
}

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(INT2_to_DOUBLE, 1163)
BUILTIN_ARGTYPE(INT2)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((double)(FMGR_ARG(0).GetInt16()));
}



BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(FLOAT_to_INT4, 1164)
BUILTIN_ARGTYPE(FLOAT)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int32_t)(FMGR_ARG(0).GetFloat()));
}

BUILTIN_RETTYPE(FLOAT)
BUILTIN_FUNC(INT4_to_FLOAT, 1165)
BUILTIN_ARGTYPE(INT4)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((float)(FMGR_ARG(0).GetInt32()));
}



BUILTIN_RETTYPE(INT4)
BUILTIN_FUNC(DOUBLE_to_INT4, 1166)
BUILTIN_ARGTYPE(DOUBLE)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int32_t)(FMGR_ARG(0).GetDouble()));
}

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(INT4_to_DOUBLE, 1167)
BUILTIN_ARGTYPE(INT4)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((double)(FMGR_ARG(0).GetInt32()));
}



BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(FLOAT_to_INT8, 1168)
BUILTIN_ARGTYPE(FLOAT)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int64_t)(FMGR_ARG(0).GetFloat()));
}

BUILTIN_RETTYPE(FLOAT)
BUILTIN_FUNC(INT8_to_FLOAT, 1169)
BUILTIN_ARGTYPE(INT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((float)(FMGR_ARG(0).GetInt64()));
}



BUILTIN_RETTYPE(INT8)
BUILTIN_FUNC(DOUBLE_to_INT8, 1170)
BUILTIN_ARGTYPE(DOUBLE)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((int64_t)(FMGR_ARG(0).GetDouble()));
}

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(INT8_to_DOUBLE, 1171)
BUILTIN_ARGTYPE(INT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((double)(FMGR_ARG(0).GetInt64()));
}



BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(FLOAT_to_UINT1, 1172)
BUILTIN_ARGTYPE(FLOAT)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint8_t)(FMGR_ARG(0).GetFloat()));
}

BUILTIN_RETTYPE(FLOAT)
BUILTIN_FUNC(UINT1_to_FLOAT, 1173)
BUILTIN_ARGTYPE(UINT1)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((float)(FMGR_ARG(0).GetUInt8()));
}



BUILTIN_RETTYPE(UINT1)
BUILTIN_FUNC(DOUBLE_to_UINT1, 1174)
BUILTIN_ARGTYPE(DOUBLE)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint8_t)(FMGR_ARG(0).GetDouble()));
}

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(UINT1_to_DOUBLE, 1175)
BUILTIN_ARGTYPE(UINT1)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((double)(FMGR_ARG(0).GetUInt8()));
}



BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(FLOAT_to_UINT2, 1176)
BUILTIN_ARGTYPE(FLOAT)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint16_t)(FMGR_ARG(0).GetFloat()));
}

BUILTIN_RETTYPE(FLOAT)
BUILTIN_FUNC(UINT2_to_FLOAT, 1177)
BUILTIN_ARGTYPE(UINT2)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((float)(FMGR_ARG(0).GetUInt16()));
}



BUILTIN_RETTYPE(UINT2)
BUILTIN_FUNC(DOUBLE_to_UINT2, 1178)
BUILTIN_ARGTYPE(DOUBLE)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint16_t)(FMGR_ARG(0).GetDouble()));
}

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(UINT2_to_DOUBLE, 1179)
BUILTIN_ARGTYPE(UINT2)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((double)(FMGR_ARG(0).GetUInt16()));
}



BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(FLOAT_to_UINT4, 1180)
BUILTIN_ARGTYPE(FLOAT)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint32_t)(FMGR_ARG(0).GetFloat()));
}

BUILTIN_RETTYPE(FLOAT)
BUILTIN_FUNC(UINT4_to_FLOAT, 1181)
BUILTIN_ARGTYPE(UINT4)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((float)(FMGR_ARG(0).GetUInt32()));
}



BUILTIN_RETTYPE(UINT4)
BUILTIN_FUNC(DOUBLE_to_UINT4, 1182)
BUILTIN_ARGTYPE(DOUBLE)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint32_t)(FMGR_ARG(0).GetDouble()));
}

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(UINT4_to_DOUBLE, 1183)
BUILTIN_ARGTYPE(UINT4)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((double)(FMGR_ARG(0).GetUInt32()));
}



BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(FLOAT_to_UINT8, 1184)
BUILTIN_ARGTYPE(FLOAT)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint64_t)(FMGR_ARG(0).GetFloat()));
}

BUILTIN_RETTYPE(FLOAT)
BUILTIN_FUNC(UINT8_to_FLOAT, 1185)
BUILTIN_ARGTYPE(UINT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((float)(FMGR_ARG(0).GetUInt64()));
}



BUILTIN_RETTYPE(UINT8)
BUILTIN_FUNC(DOUBLE_to_UINT8, 1186)
BUILTIN_ARGTYPE(DOUBLE)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((uint64_t)(FMGR_ARG(0).GetDouble()));
}

BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(UINT8_to_DOUBLE, 1187)
BUILTIN_ARGTYPE(UINT8)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((double)(FMGR_ARG(0).GetUInt64()));
}



BUILTIN_RETTYPE(DOUBLE)
BUILTIN_FUNC(FLOAT_to_DOUBLE, 1188)
BUILTIN_ARGTYPE(FLOAT)
BUILTIN_OPR(IMPLICIT_CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((double)(FMGR_ARG(0).GetFloat()));
}

BUILTIN_RETTYPE(FLOAT)
BUILTIN_FUNC(DOUBLE_to_FLOAT, 1189)
BUILTIN_ARGTYPE(DOUBLE)
BUILTIN_OPR(CAST)
{
    if (FMGR_ARG(0).isnull()) {
        return Datum::FromNull();
    }

    return Datum::From((float)(FMGR_ARG(0).GetDouble()));
}

}    // namespace taco

