#ifndef EXPR_FUNCCALLOPERATOR_H
#define EXPR_FUNCCALLOPERATOR_H

#include "expr/ExprNode.h"

namespace taco {

class FuncCallOperator: public ExprNode {
public:
    template<class ...UniquePtrs>
    static std::unique_ptr<FuncCallOperator>
    Create(Oid funcid, UniquePtrs&& ...arg_exprs) {
        std::shared_ptr<const SysTable_Type> rettyp;
        FunctionInfo finfo =
            CheckAndLookupFunction(funcid, rettyp, arg_exprs...);
        return absl::WrapUnique(
            new FuncCallOperator(std::move(finfo),
                                 funcid,
                                 std::move(rettyp),
                                 std::forward<UniquePtrs>(arg_exprs)...));
    }

    ~FuncCallOperator() override {}

    Datum Eval(const std::vector<NullableDatumRef>& record) const override;

    Datum Eval(const char* record) const override;

    void node_properties_to_string(std::string &buf, int indent) const override;

private:
    template<class ...UniquePtrs>
    FuncCallOperator(FunctionInfo &&func,
                     Oid funcid,
                     std::shared_ptr<const SysTable_Type> rettyp,
                     UniquePtrs&& ...arg_exprs):
    ExprNode(TAG(FuncCallOperator), std::forward<UniquePtrs>(arg_exprs)...),
    m_func(std::move(func)),
    m_funcid(funcid),
    m_nargs((int16_t) sizeof...(UniquePtrs)) {
        m_type = std::move(rettyp);
    }

    template<class ...UniquePtrs>
    static FunctionInfo
    CheckAndLookupFunction(Oid funcid,
                           std::shared_ptr<const SysTable_Type> &rettyp,
                           const UniquePtrs&... arg_exprs) {
        std::vector<const ExprNode*> arg_expr_ptrs {{
            &*arg_exprs...
        }};

        return CheckAndLookupFunctionImpl(funcid, rettyp, arg_expr_ptrs);
    }

    static FunctionInfo CheckAndLookupFunctionImpl(
        Oid funcid,
        std::shared_ptr<const SysTable_Type> &rettyp,
        std::vector<const ExprNode*> arg_expr_ptrs);

    FunctionInfo m_func;
    Oid          m_funcid;
    int16_t      m_nargs;
};

}

#endif
