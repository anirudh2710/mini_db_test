/*!
 * @file
 *
 * This header file includes all internal struct representation of system
 * catalog records.
 */

#ifndef CATALOG_SYSTABLES_H
#define CATALOG_SYSTABLES_H

#include "tdb.h"

// These headers have header guards now, but we still include all of them here
// for convenience.

#include "catalog/systables/Column.h"
#include "catalog/systables/Function.h"
#include "catalog/systables/Table.h"
#include "catalog/systables/Type.h"
#include "catalog/systables/FunctionArgs.h"
#include "catalog/systables/Operator.h"
#include "catalog/systables/Index.h"
#include "catalog/systables/IndexColumn.h"
#include "catalog/systables/Cast.h"
#include "catalog/systables/Aggregation.h"
#include "catalog/systables/initoids.h"

namespace taco {
/*!
 * This is an arbitrarily chosen large number that all systable entries
 * in the init file should have an OID <= max_sys_oid.
 */
constexpr Oid max_sys_oid = 19999;

}   // namespace taco

#endif      // CATALOG_SYSTABLES_H
