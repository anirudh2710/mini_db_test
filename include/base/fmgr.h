/*!
 * @file
 *
 * This file contains the definition of the function manager and function-call
 * interface, which is a simplified version of fmgr.h in PostgreSQL. See
 * COPYRIGHT for a copyright notice of code copied or derived from PostgreSQL.
 *
 * This file does not have a header guard intentionally. It must be included
 * indirectly from base/tdb_base.h.
 *
 */

namespace taco {


/*!
 * This struct is the data actually passed to an fmgr function. In addition to
 * the arguments, we also need to pass the type parameters, unless it is known
 * that the function does not care about the type parameter.
 */
struct FunctionCallInfo
{
    std::vector<NullableDatumRef> args;
    uint64_t                      typparam;
};


typedef Datum (*FunctionPtr)(FunctionCallInfo &fcinfo);

/*!
 * An FMGR managed function should be declared as
 *
 * ::taco::Datum some_function(FMGR_FUNCTION_ARGS);
 *
 * To define a built-in function that the Function catalog collects at
 * compile time, use the BUILTIN_XXX() macros defined in utils/builtin_funcs.h.
 *
 * It should access its arguments using appropriate FMGR_GETARG macros and
 * should return its result using return Datum::From() or return
 * Datum::FromXXX().
 *
 * To call a function, use any of the function call templates (e.g.,
 * FunctionCall()) below, or directly call FunctionInfo with a FunctionCallInfo
 * supplied by the caller (in which case the caller is responsible for checking
 * whether the return value is null).
 *
 * FunctionInfo can be implicitly cast to a bool, to indicate whether it is
 * valid and callable.
 */
typedef std::function<Datum(FunctionCallInfo&)> FunctionInfo;

/* Standard parameter list for fmgr-compatible functions */
#define FMGR_FUNCTION_ARGS    ::taco::FunctionCallInfo &fcinfo_

/*!
 * Get number of arguments passed to the function.
 */
#define FMGR_NARGS() (fcinfo_.args.size())

/*!
 * Get the n^th argument passed to the function.
 */
#define FMGR_ARG(n) (fcinfo_.args[n])

/*!
 * Get the type parameter passed to the function. Only valid when the function
 * is called with FunctionCallWithTypparam().
 */
#define FMGR_TYPPARAM() (fcinfo_.typparam)

/*!
 * If any function needs to return void, use this macro. Don't return a NULL
 * value.
 */
#define FMGR_RETURN_VOID()     return Datum::From(0)

/*!
 * Call a function without passing any type parameter for the return type.
 */
template<class ...Args>
Datum
FunctionCall(const FunctionInfo &func, Args&& ...args) {
    FunctionCallInfo flinfo {
        .args = std::vector<NullableDatumRef>{std::forward<Args>(args)...},
        .typparam = 0
    };

    return func(flinfo);
}

/*
 * Call a function with a type parameter for the return type.
 */
template<class ...Args>
Datum
FunctionCallWithTypparam(const FunctionInfo &func,
                         uint64_t typparam,
                         Args&& ...args) {
    FunctionCallInfo flinfo {
        .args = std::vector<NullableDatumRef>{std::forward<Args>(args)...},
        .typparam = typparam,
    };

    return func(flinfo);
}

}   // namespace taco

