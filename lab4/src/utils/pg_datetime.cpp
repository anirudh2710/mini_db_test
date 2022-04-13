/*!
 * @file
 *
 * This file contains date/time utilities adapted from PostgreSQL 14.1 source
 * code. See COPYRIGHT for PostgerSQL's copyright notice.
 */

#include "utils/pg_datetime.h"

int32_t
date2j(int32_t y, int32_t m, int32_t d)
{
    int32_t julian;
    int32_t century;

    if (m > 2)
    {
        m += 1;
        y += 4800;
    }
    else
    {
        m += 13;
        y += 4799;
    }

    century = y / 100;
    julian = y * 365 - 32167;
    julian += y / 4 - century + century / 4;
    julian += 7834 * m / 256 + d;

    return julian;
}                                /* date2j() */

void
j2date(int32_t jd, int32_t *year, int32_t *month, int32_t *day)
{
    uint32_t    julian;
    uint32_t    quad;
    uint32_t    extra;
    int32_t     y;

    julian = jd;
    julian += 32044;
    quad = julian / 146097;
    extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    y = julian * 4 / 1461;
    julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366))
        + 123;
    y += quad * 4;
    *year = y - 4800;
    quad = julian * 2141 / 65536;
    *day = julian - 7834 * quad / 256;
    *month = (quad + 10) % 12 + 1;
}                                /* j2date() */

