#ifndef EXPR_ANDOPERATOR_H
#define EXPR_ANDOPERATOR_H

#include "expr/ExprNode.h"

namespace taco {

class AndOperator: public ExprNode {
public:
    static std::unique_ptr<AndOperator>
    Create(std::unique_ptr<ExprNode>&& left, std::unique_ptr<ExprNode>&& right);

    ~AndOperator() override {}

    Datum Eval(const std::vector<NullableDatumRef>& record) const override;

    Datum Eval(const char* record) const override;

    void node_properties_to_string(std::string &buf, int indent) const override;

private:
    AndOperator(std::unique_ptr<ExprNode>&& left,
                std::unique_ptr<ExprNode>&& right);
};

}

#endif
