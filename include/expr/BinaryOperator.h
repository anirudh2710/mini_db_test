#ifndef EXPR_BINARYOPERATOR_H
#define EXPR_BINARYOPERATOR_H

#include "expr/ExprNode.h"
#include "expr/optypes.h"

namespace taco {

class BinaryOperator: public ExprNode {
public:
    static std::unique_ptr<BinaryOperator>
    Create(OpType optype,
           std::unique_ptr<ExprNode>&& left,
           std::unique_ptr<ExprNode>&& right);

    ~BinaryOperator() override {}

    Datum Eval(const std::vector<NullableDatumRef>& record) const override;

    Datum Eval(const char* record) const override;

    void node_properties_to_string(std::string &buf, int indent) const override;
private:
    BinaryOperator(OpType optype,
                   std::unique_ptr<ExprNode>&& left,
                   std::unique_ptr<ExprNode>&& right);

    OpType m_optype;
    FunctionInfo m_func;
};

}

#endif
