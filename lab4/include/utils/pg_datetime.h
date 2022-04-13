/*!
 * @file
 *
 * This file contains date/time utilities adapted from PostgreSQL 14.1 source
 * code. See COPYRIGHT for a copyright notice.
 */
#ifndef UTILS_PG_DATETIME_H
#define UTILS_PG_DATETIME_H

#include "tdb.h"

/*!
 * date2j() from PostgreSQL utils/adt/datetime.c.
 *
 * Converts a date (%Y-%m-%d) to a julian day number.
 *
 * Note that y is the actual year number (not year - 1900); m is from 1 to 12,
 * not from 0.
 */
int date2j(int y, int m, int d);

/*!
 * j2date() from PostgreSQL utils/adt/datetime.c.
 *
 * Converts a julian day number to a date (%Y-%m-%d).
 *
 * Note that y is the actual year number (not year - 1900); m is from 1 to 12,
 * not from 0.
 */
void j2date(int jd, int *year, int *month, int *day);

#endif  // UTILS_PG_DATETIME_H
