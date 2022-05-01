#ifndef EXPR_LITERAL_H
#define EXPR_LITERAL_H

#include "expr/ExprNode.h"
#include "catalog/CatCache.h"
#include "catalog/systables/initoids.h"

namespace taco {

class Literal: public ExprNode {
public:
    static std::unique_ptr<Literal>
    Create(Datum&& val, Oid type_oid);

    ~Literal() override {}

    Datum Eval(const std::vector<NullableDatumRef>& record) const override;

    Datum Eval(const char* record) const override;

    void node_properties_to_string(std::string &buf, int indent) const override;

private:
    Literal(Datum &&val, Oid type_oid);

    Datum m_value;
};

}

#endif
