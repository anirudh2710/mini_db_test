#include "expr/Variable.h"
#include "catalog/systables/initoids.h"
#include "catalog/CatCache.h"

namespace taco {

Variable::Variable(const Schema* schema, FieldId fid)
: ExprNode(TAG(Variable))
, m_schema(schema)
, m_fid(fid) {
    if (m_fid >= schema->GetNumFields()) {
        LOG(kError, "Field ID %d is not valid", m_fid);
    }

    auto type_id = m_schema->GetFieldTypeId(m_fid);
    //if (type_id == initoids::TYP_CHAR) type_id = initoids::TYP_VARCHAR;
    m_type = g_catcache->FindType(type_id);
}

std::unique_ptr<Variable>
Variable::Create(const Schema* schema, FieldId fid) {
    return absl::WrapUnique(new Variable(schema, fid));
}

Datum
Variable::Eval(const std::vector<NullableDatumRef>& record) const {
    return record[m_fid].DeepCopy(m_type->typbyref());
}

Datum
Variable::Eval(const char* record) const {
    return m_schema->GetField(m_fid, record).DeepCopy();
}

void
Variable::node_properties_to_string(std::string& buf, int indent) const {
    append_indent(buf, indent);
    absl::StrAppendFormat(&buf, "Field ID = %d", m_fid);
}

}
