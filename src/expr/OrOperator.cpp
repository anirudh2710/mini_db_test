#include "expr/OrOperator.h"
#include "catalog/CatCache.h"
#include "catalog/systables/initoids.h"

namespace taco {

OrOperator::OrOperator(std::unique_ptr<ExprNode>&& left,
                       std::unique_ptr<ExprNode>&& right)
: ExprNode(TAG(OrOperator), std::move(left), std::move(right)) {
    auto left_type = get_input_as<ExprNode>(0)->ReturnType()->typid();
    auto right_type = get_input_as<ExprNode>(1)->ReturnType()->typid();
    if (left_type != initoids::TYP_BOOL || right_type != initoids::TYP_BOOL) {
        LOG(kFatal, "The type an AndOperator child is not bool.");
    }
    m_type = g_catcache->FindType(initoids::TYP_BOOL);
}

OrOperator::~OrOperator() {}

std::unique_ptr<OrOperator>
OrOperator::Create(std::unique_ptr<ExprNode>&& left,
                   std::unique_ptr<ExprNode>&& right) {
    return absl::WrapUnique(
        new OrOperator(std::move(left), std::move(right)));
}

Datum
OrOperator::Eval(const std::vector<NullableDatumRef>& record) const {
    if (get_input_as<ExprNode>(0)->Eval(record).GetBool())
        return Datum::From(true);
    else if (get_input_as<ExprNode>(1)->Eval(record).GetBool())
        return Datum::From(true);
    else return Datum::From(false);
}

Datum
OrOperator::Eval(const char* record) const {
    if (get_input_as<ExprNode>(0)->Eval(record).GetBool())
        return Datum::From(true);
    else if (get_input_as<ExprNode>(1)->Eval(record).GetBool())
        return Datum::From(true);
    else return Datum::From(false);
}


void
OrOperator::node_properties_to_string(std::string &buf, int indent) const {
    append_indent(buf, indent);
    absl::StrAppendFormat(&buf, "OP_TYPE = ||");
}

}