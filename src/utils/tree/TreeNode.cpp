#include "utils/tree/TreeNode.h"

#include <absl/strings/str_format.h>

#include "utils/string_utils.h"

namespace taco {

void
TreeNode::node_to_string(std::string &buf, int indent) const {
    buf.append(this->node_name()).append("{\n");
    this->node_properties_to_string(buf, indent + 1);

    for (size_t i = 0; i != m_input.size(); ++i) {
        append_indent(buf, indent + 1);
        absl::StrAppendFormat(&buf, "input[%lu] = ", i);
        m_input[i]->node_to_string(buf, indent + 1);
    }

    append_indent(buf, indent);
    buf.append("}\n");
}

void
TreeNode::append_indent(std::string &buf, int indent) {
    str_append_spaces(buf, indent * TO_STRING_INDENT_SIZE);
}

}   // namespace taco
