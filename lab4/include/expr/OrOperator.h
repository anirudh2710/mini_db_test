#ifndef EXPR_OROPERATOR_H
#define EXPR_OROPERATOR_H

#include "expr/ExprNode.h"

namespace taco {

class OrOperator: public ExprNode {
public:
    static std::unique_ptr<OrOperator>
    Create(std::unique_ptr<ExprNode>&& left,
           std::unique_ptr<ExprNode>&& right);

    ~OrOperator() override;

    Datum Eval(const std::vector<NullableDatumRef>& record) const override;

    Datum Eval(const char* record) const override;

    void node_properties_to_string(std::string &buf, int indent) const override;
private:
    OrOperator(std::unique_ptr<ExprNode>&& left,
               std::unique_ptr<ExprNode>&& right);
};

}

#endif
