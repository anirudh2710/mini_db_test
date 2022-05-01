#ifndef UTILS_TREE_NODETAG_H
#define UTILS_TREE_NODETAG_H

#include "tdb.h"

#include "utils/misc.h"

namespace taco {

#define NODE_TAG(clsname, ...) \
    CONCAT(T_, clsname) IF_NONEMPTY(CADR(__VA_ARGS__), = CADR(__VA_ARGS__)),
enum class NodeTag: uint16_t {
    T_TreeNode = 0,
#   include "utils/tree/node_tags.inc"

    T_InvalidTag = 0xFFFF
};
#undef NODE_TAG

extern const char *node_tag_name(NodeTag tag);

extern NodeTag node_tag_base(NodeTag tag);

extern bool node_tag_is_a(NodeTag tag1, NodeTag tag2);

#define TAG(nodeclsname) CONCAT(NodeTag::T_, nodeclsname)

}   // namespace taco

#endif      // UTILS_TREE_TREENODETAG_H
