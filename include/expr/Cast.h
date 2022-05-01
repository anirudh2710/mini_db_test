#ifndef EXPR_CAST_H
#define EXPR_CAST_H

#include "expr/ExprNode.h"

namespace taco {

class Cast: public ExprNode {
public:
    static std::unique_ptr<Cast>
    Create(Oid typ_oid, std::unique_ptr<ExprNode>&& child, bool implicit);

    ~Cast() override;

    Datum Eval(const std::vector<NullableDatumRef>& record) const override;

    Datum Eval(const char* record) const override;

    void node_properties_to_string(std::string& buf, int indent) const override;

private:
    Cast(Oid typ_oid, std::unique_ptr<ExprNode>&& child, bool implicit);

    FunctionInfo m_func;

};

}

#endif
