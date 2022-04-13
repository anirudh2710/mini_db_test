#include "expr/AndOperator.h"
#include "catalog/systables/initoids.h"
#include "catalog/CatCache.h"

namespace taco {

AndOperator::AndOperator(std::unique_ptr<ExprNode>&& left,
                         std::unique_ptr<ExprNode>&& right)
: ExprNode(TAG(AndOperator), std::move(left), std::move(right)) {

    auto left_type = get_input_as<ExprNode>(0)->ReturnType()->typid();
    auto right_type = get_input_as<ExprNode>(1)->ReturnType()->typid();
    if (left_type != initoids::TYP_BOOL || right_type != initoids::TYP_BOOL) {
        LOG(kError, "The type an AndOperator child is not bool.");
    }

    m_type = g_catcache->FindType(initoids::TYP_BOOL);
}

std::unique_ptr<AndOperator>
AndOperator::Create(std::unique_ptr<ExprNode>&& left,
                    std::unique_ptr<ExprNode>&& right) {

    return absl::WrapUnique(
        new AndOperator(std::move(left), std::move(right)));
}

Datum
AndOperator::Eval(const std::vector<NullableDatumRef>& record) const {
    if (!get_input_as<ExprNode>(0)->Eval(record).GetBool())
        return Datum::From(false);
    else if (!get_input_as<ExprNode>(1)->Eval(record).GetBool())
        return Datum::From(false);
    else return Datum::From(true);
}

Datum
AndOperator::Eval(const char* record) const {
    if (!get_input_as<ExprNode>(0)->Eval(record).GetBool())
        return Datum::From(false);
    else if (!get_input_as<ExprNode>(1)->Eval(record).GetBool())
        return Datum::From(false);
    else return Datum::From(true);
}

void
AndOperator::node_properties_to_string(std::string& buf, int indent) const {
    append_indent(buf, indent);
    absl::StrAppendFormat(&buf, "OP_TYPE = &&");
}

}