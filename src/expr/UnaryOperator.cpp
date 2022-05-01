#include "expr/UnaryOperator.h"
#include "catalog/CatCache.h"
#include "utils/builtin_funcs.h"

namespace taco {

UnaryOperator::UnaryOperator(OpType optype, std::unique_ptr<ExprNode>&& child)
: ExprNode(TAG(UnaryOperator), std::move(child))
, m_optype(optype) {
    if (!OpTypeIsUnary(optype)) {
        LOG(kError, "OpType %d does not name an unary operator.", optype);
    }
    auto func_oid =
        g_catcache->FindOperator(optype,
                                 get_input_as<ExprNode>(0)->ReturnType()->typid(),
                                 InvalidOid);
    m_func = FindBuiltinFunction(func_oid);
    m_type =
        g_catcache->FindType(g_catcache->FindFunction(func_oid)->funcrettypid());
}

std::unique_ptr<UnaryOperator>
UnaryOperator::Create(OpType optype, std::unique_ptr<ExprNode>&& child) {
    return absl::WrapUnique(new UnaryOperator(optype, std::move(child)));
}

Datum
UnaryOperator::Eval(const std::vector<NullableDatumRef>& record) const {
    return FunctionCall(m_func, get_input_as<ExprNode>(0)->Eval(record));
}

Datum
UnaryOperator::Eval(const char* record) const {
    return FunctionCall(m_func, get_input_as<ExprNode>(0)->Eval(record));
}

void
UnaryOperator::node_properties_to_string(std::string& buf, int indent) const {
    append_indent(buf, indent);
    absl::StrAppendFormat(&buf,
                          "OP_TYPE = %s",
                          GetOpTypeSymbol(m_optype));
}

}