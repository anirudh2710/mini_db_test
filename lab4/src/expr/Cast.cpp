#include "expr/Cast.h"
#include "catalog/CatCache.h"
#include "utils/builtin_funcs.h"

namespace taco {

Cast::Cast(Oid typ_oid, std::unique_ptr<ExprNode>&& child, bool implicit)
: ExprNode(TAG(Cast), std::move(child))  {
    auto child_type = get_input_as<ExprNode>(0)->ReturnType()->typid();
    Oid cast_oid = g_catcache->FindCast(child_type, typ_oid, implicit);
    if (cast_oid == InvalidOid) {
        LOG(kError, "Cannot find cast function from type %d to type %d...", child_type, typ_oid);
    }
    m_func = FindBuiltinFunction(cast_oid);
    m_type = g_catcache->FindType(typ_oid);
}

std::unique_ptr<Cast>
Cast::Create(Oid typ_oid, std::unique_ptr<ExprNode>&& child, bool implicit) {
    return absl::WrapUnique(
        new Cast(typ_oid, std::move(child), implicit));
}

Cast::~Cast() {}

Datum
Cast::Eval(const std::vector<NullableDatumRef>& record) const {
    return FunctionCall(m_func, get_input_as<ExprNode>(0)->Eval(record));
}

Datum
Cast::Eval(const char* record) const {
    return FunctionCall(m_func, get_input_as<ExprNode>(0)->Eval(record));
}


void
Cast::node_properties_to_string(std::string& buf, int indent) const {
    append_indent(buf, indent);
    absl::StrAppendFormat(&buf,
                          "CAST TO TYP = %d",
                          m_type->typid());
}

}