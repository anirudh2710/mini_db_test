#include "expr/FuncCallOperator.h"
#include "utils/builtin_funcs.h"
#include "catalog/CatCache.h"

namespace taco {

FunctionInfo
FuncCallOperator::CheckAndLookupFunctionImpl(
    Oid funcid,
    std::shared_ptr<const SysTable_Type> &rettyp,
    std::vector<const ExprNode*> arg_expr_ptrs) {

    auto func = g_catcache->FindFunction(funcid);
    if (!func) {
        LOG(kFatal, "function (func id = " OID_FORMAT ") not found");
    }
    int16_t nargs = func->funcnargs();
    if ((size_t) nargs != arg_expr_ptrs.size()) {
        LOG(kFatal, "function %s expects %hd arguments, but got %lu",
                    func->funcname(), nargs, arg_expr_ptrs.size());
    }

    for (int16_t i = 0; i < nargs; ++i) {
        auto funcarg = g_catcache->FindFunctionArgs(funcid, i);
        if (arg_expr_ptrs[i]->ReturnType()->typid() != funcarg->funcargtypid()) {
            auto expected_typ = g_catcache->FindType(funcarg->funcargtypid());
            LOG(kFatal, "function %s expects %s type for its %hdth argument, "
                        "got %s instead", func->funcname(),
                        (expected_typ.get() ? expected_typ->typname() :
                         absl::StrCat("(typid = ", funcarg->funcargtypid(),
                                      ")")),
                        arg_expr_ptrs[i]->ReturnType()->typid());
        }
    }

    rettyp = g_catcache->FindType(func->funcrettypid());
    FunctionInfo finfo = FindBuiltinFunction(funcid);
    if (!finfo) {
        LOG(kFatal, "unable to find the built-in function %s",
                    func->funcname());
    }
    return finfo;
}

Datum
FuncCallOperator::Eval(const std::vector<NullableDatumRef>& record) const {
    std::vector<Datum> args;
    if (m_nargs != 0)
        args.reserve(m_nargs);
    for (int16_t i = 0; i < m_nargs; ++i) {
        args.emplace_back(get_input_as<ExprNode>(i)->Eval(record));
    }
    FunctionCallInfo flinfo {
        .args = std::vector<NullableDatumRef>(args.begin(), args.end()),
        .typparam = 0
    };
    return m_func(flinfo);
}

Datum
FuncCallOperator::Eval(const char* record) const {
    std::vector<Datum> args;
    if (m_nargs != 0)
        args.reserve(m_nargs);
    for (int16_t i = 0; i < m_nargs; ++i) {
        args.emplace_back(get_input_as<ExprNode>(i)->Eval(record));
    }
    FunctionCallInfo flinfo {
        .args = std::vector<NullableDatumRef>(args.begin(), args.end()),
        .typparam = 0
    };
    return m_func(flinfo);
}

void
FuncCallOperator::node_properties_to_string(std::string &buf, int indent) const {
    append_indent(buf, indent);
    absl::StrAppend(&buf, "funcid = ", m_funcid, "\n");

    auto func = g_catcache->FindFunction(m_funcid);
    append_indent(buf, indent);
    absl::StrAppend(&buf, "funcname = ",
                    (func ? func->funcname() : "unknown_function"), "\n");
}

}
