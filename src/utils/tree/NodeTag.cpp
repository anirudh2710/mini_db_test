#include "utils/tree/NodeTag.h"

#include "utils/misc.h"

namespace taco {

#define NODE_TAG_GROUP_NUMBER_MASK 0xf000
#define NODE_TAG_GROUP_NUMBER_SHIFT 12
#define NODE_TAG_MAX_NUM_GROUPS 16
#define NODE_TAG_GROUP_OFFSET_MASK 0x0fff

#define node_tag_group_number(tag) \
    ((((uint16_t) tag) & NODE_TAG_GROUP_NUMBER_MASK) >> \
        NODE_TAG_GROUP_NUMBER_SHIFT)
#define node_tag_group_offset(tag) \
    (((uint16_t) tag) & NODE_TAG_GROUP_OFFSET_MASK)

#define NODE_TAG(...) \
    IF_EMPTY(CADDR(__VA_ARGS__), + 1) \
    IF_NONEMPTY_COMMA(CADDR(__VA_ARGS__), + 0) \
    IF_NONEMPTY(CADDR(__VA_ARGS__), 1)
static constexpr const uint16_t node_tag_group_count[NODE_TAG_MAX_NUM_GROUPS]
    = {
    1 /* T_TreeNode */
#   include "utils/tree/node_tags.inc"
};
#undef NODE_TAG

static constexpr const uint16_t
    node_tag_group_base_offset[NODE_TAG_MAX_NUM_GROUPS]
    = {
    0,
    (uint16_t)(node_tag_group_count[0] + node_tag_group_base_offset[0]),
    (uint16_t)(node_tag_group_count[1] + node_tag_group_base_offset[1]),
    (uint16_t)(node_tag_group_count[2] + node_tag_group_base_offset[2]),
    (uint16_t)(node_tag_group_count[3] + node_tag_group_base_offset[3]),
    (uint16_t)(node_tag_group_count[4] + node_tag_group_base_offset[4]),
    (uint16_t)(node_tag_group_count[5] + node_tag_group_base_offset[5]),
    (uint16_t)(node_tag_group_count[6] + node_tag_group_base_offset[6]),
    (uint16_t)(node_tag_group_count[7] + node_tag_group_base_offset[7]),
    (uint16_t)(node_tag_group_count[8] + node_tag_group_base_offset[8]),
    (uint16_t)(node_tag_group_count[9] + node_tag_group_base_offset[9]),
    (uint16_t)(node_tag_group_count[10] + node_tag_group_base_offset[10]),
    (uint16_t)(node_tag_group_count[11] + node_tag_group_base_offset[11]),
    (uint16_t)(node_tag_group_count[12] + node_tag_group_base_offset[12]),
    (uint16_t)(node_tag_group_count[13] + node_tag_group_base_offset[13]),
    (uint16_t)(node_tag_group_count[14] + node_tag_group_base_offset[14])
};

static constexpr const uint16_t num_node_tags =
    node_tag_group_count[15] + node_tag_group_base_offset[15];


#define NODE_TAG(clsname, ...) STRINGIFY(clsname),
static const char * const node_tag_name_[num_node_tags] = {
    "TreeNode",
#   include "utils/tree/node_tags.inc"
};
#undef NODE_TAG

#define NODE_TAG(clsname, ...) CONCAT(NodeTag::T_, CAR(__VA_ARGS__)),
static constexpr NodeTag node_tag_base_[num_node_tags] = {
    NodeTag::T_InvalidTag, /* for TreeNode */
#   include "utils/tree/node_tags.inc"
};
#undef NODE_TAG

inline static constexpr uint16_t
node_tag_offset(NodeTag tag) {
    return node_tag_group_base_offset[node_tag_group_number(tag)] +
            node_tag_group_offset(tag);
}

/*!
 * Returns the class name of the tag's class.
 */
const char*
node_tag_name(NodeTag tag) {
    return node_tag_name_[node_tag_offset(tag)];
}

/*!
 * Returns the immediate base class of ``tag''.
 */
NodeTag
node_tag_base(NodeTag tag) {
    return node_tag_base_[node_tag_offset(tag)];
}

/*!
 * Returns whether ``tag1'' is a class derived from ``tag2''.
 */
bool
node_tag_is_a(NodeTag tag1, NodeTag tag2) {
    // class A is a class A
    if (tag1 == tag2) return true;

    // Special case: everything is derived from tree node
    if (tag2 == NodeTag::T_TreeNode) return true;

    // Fast path: two tags must be in the same group if tag1 is a tag2 (unless
    // tag2 is TreeTag::T_TreeNode)
    if (((uint16_t) tag1 ^ (uint16_t) tag2) & NODE_TAG_GROUP_NUMBER_MASK) {
        return false;
    }

    // Otherwise, we need to search through the inheritance relation chain.
    do {
        tag1 = node_tag_base(tag1);
        if (tag2 == tag1)
            return true;
    } while (tag1 != NodeTag::T_TreeNode);
    return false;
}

}   // namespace taco

