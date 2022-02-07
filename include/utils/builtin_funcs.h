#ifndef UTILS_BUILTIN_FUNCS_H
#define UTILS_BUILTIN_FUNCS_H

#include "tdb.h"

namespace taco {


/*!
 * To define a builtin function that is collected by the Function systable at
 * compile time, use BUILTIN_RETTYPE, BUILTIN_FUNC, and BUILTIN_ARGTYPE macros
 * to define that in any source file in src/. These macros must be appear on
 * three consecutive lines with no leading spaces/tabs in the same order as
 * below.
 *
 * The return and argument types need to be some SQL type known to the system
 * at compile time. For instance, to define a function f with a funcid = 1000
 * that accepts INT4 and INT2 typed arguments and returns a INT4, add the
 * following lines (BUILTIN_xxx macros must appear at the beginning of the
 * lines with no space) to some source file in src/. The funcid must be unique
 * across all systable rows and will be checked at compile time for
 * duplication.
 *
 * BUILTIN_RETTYPE(INT4)
 * BUILTIN_FUNC(f, 1000)
 * BUILTIN_ARGTYPE(INT4, INT2)
*  BUILTIN_OPR(ADD) // only needed if defining a operator
 * {
 *  // implementation of f
 * }
 *
 * You'd also need to add the file that contains the builtin function to the
 * list FILES_WITH_BUILTIN_FUNCS in src/utils/CMakeLists.txt.
 */
#define BUILTIN_RETTYPE(...)
#define BUILTIN_FUNC(func_name, ...) \
    ::taco::Datum func_name (FMGR_FUNCTION_ARGS)
#define BUILTIN_ARGTYPE(...)
#define BUILTIN_OPR(...)

/*!
 * Initializes the lookup table for built-in functions registered in the system
 * catalog.
 */
void InitBuiltinFunctions();

/*!
 * Looks up the callable function info for a built-in function registered in
 * the system catalog.
 *
 * @returns a valid FunctionInfo if found, or an invalid FunctionInfo if not
 * found
 */
FunctionInfo FindBuiltinFunction(Oid oid);

}   // namespace taco

#endif      // UTILS_BUILTIN_FUNCS_H
