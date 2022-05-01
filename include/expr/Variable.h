#ifndef EXPR_VARIABLE_H
#define EXPR_VARIABLE_H

#include "expr/ExprNode.h"
#include "catalog/Schema.h"

namespace taco {

class Variable: public ExprNode {
public:
    static std::unique_ptr<Variable>
    Create(const Schema* schema, FieldId fid);

    ~Variable() override {}

    Datum Eval(const std::vector<NullableDatumRef>& record) const override;

    Datum Eval(const char* record) const override;

    void node_properties_to_string(std::string &buf, int indent) const override;
private:
    Variable(const Schema* schema, FieldId fid);

    const Schema* m_schema;
    FieldId m_fid;
};

};

#endif
