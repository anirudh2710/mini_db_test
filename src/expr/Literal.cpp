#include "expr/Literal.h"

namespace taco {

Literal::Literal(Datum&& val, Oid type_oid)
: ExprNode(TAG(Literal))
, m_value((val.HasExternalRef()) ?
           val.DeepCopy() :
           std::move(val)) {
    auto typ = g_catcache->FindType(type_oid);
    if (!typ) {
        LOG(kError, "OID %u does not name a valid type.", type_oid);
    }
    m_type = typ;
}

std::unique_ptr<Literal>
Literal::Create(Datum&& val, Oid type_oid) {
    return absl::WrapUnique(new Literal(std::move(val), type_oid));
}

Datum
Literal::Eval(const std::vector<NullableDatumRef>& record) const {
    return m_value.DeepCopy();
}

Datum
Literal::Eval(const char* record) const {
    return m_value.DeepCopy();
}

void
Literal::node_properties_to_string(std::string &buf, int indent) const {
    append_indent(buf, indent);
    // TODO
}

}
