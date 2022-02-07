/*!
 * @file
 *
 * Declares pg_mkdir_p() adapted from PostgreSQL. See COPYRIGHT and the
 * function doc for a copyright notice.
 */
#ifndef UTILS_PGMKDIRP_H
#define UTILS_PGMKDIRP_H

#include "tdb.h"

int pg_mkdir_p(char *path, int omode);


#endif      // UTILS_PGMKDIRP_H
