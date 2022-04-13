#ifndef EXPR_UNARYOPERATOR_H
#define EXPR_UNARYOPERATOR_H

#include "expr/ExprNode.h"
#include "expr/optypes.h"

namespace taco {

class UnaryOperator: public ExprNode {
public:
    static std::unique_ptr<UnaryOperator>
    Create(OpType optype, std::unique_ptr<ExprNode>&& child);

    ~UnaryOperator() override {}

    Datum Eval(const std::vector<NullableDatumRef>& record) const override;

    Datum Eval(const char* record) const override;

    void node_properties_to_string(std::string &buf, int indent) const override;

private:
    UnaryOperator(OpType optype, std::unique_ptr<ExprNode>&& child);

    OpType m_optype;
    FunctionInfo m_func;
};

}

#endif
