#include "expr/BinaryOperator.h"
#include "utils/builtin_funcs.h"
#include "catalog/CatCache.h"

namespace taco {

BinaryOperator::BinaryOperator(OpType optype,
                               std::unique_ptr<ExprNode>&& left,
                               std::unique_ptr<ExprNode>&& right)
: ExprNode(TAG(BinaryOperator), std::move(left), std::move(right))
, m_optype(optype) {
    if(!OpTypeIsBinary(optype)) {
        LOG(kFatal, "OpType %d does not name a binary operator.", optype);
    }
    auto left_type = get_input_as<ExprNode>(0)->ReturnType()->typid();
    auto right_type = get_input_as<ExprNode>(1)->ReturnType()->typid();
    auto func_oid =
        g_catcache->FindOperator(optype, left_type, right_type);
    if (func_oid == InvalidOid) {
        LOG(kFatal, "Cannot find function for OpType %d.", optype);
    }
    m_func = FindBuiltinFunction(func_oid);
    m_type =
        g_catcache->FindType(g_catcache->FindFunction(func_oid)->funcrettypid());
}

std::unique_ptr<BinaryOperator>
BinaryOperator::Create(OpType optype,
                       std::unique_ptr<ExprNode>&& left,
                       std::unique_ptr<ExprNode>&& right) {
    return absl::WrapUnique(
        new BinaryOperator(optype, std::move(left), std::move(right)));
}

Datum
BinaryOperator::Eval(const std::vector<NullableDatumRef>& record) const {
    return FunctionCall(m_func,
                        get_input_as<ExprNode>(0)->Eval(record),
                        get_input_as<ExprNode>(1)->Eval(record));
}

Datum
BinaryOperator::Eval(const char* record) const {
    return FunctionCall(m_func,
                        get_input_as<ExprNode>(0)->Eval(record),
                        get_input_as<ExprNode>(1)->Eval(record));
}

void
BinaryOperator::node_properties_to_string(std::string &buf, int indent) const {
    append_indent(buf, indent);
    absl::StrAppendFormat(&buf,
                          "OP_TYPE = %s",
                          GetOpTypeSymbol(m_optype));
}

}