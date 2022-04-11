#include "index/volatiletree/VolatileTree.h"

#include <absl/container/flat_hash_map.h>

#include "index/tuple_compare.h"
#include "storage/FileManager.h"
#include "utils/builtin_funcs.h"

namespace taco {

struct VolatileTreeRecordHeader {
    // see below
    uint64_t    m_flags;
};

// The 32-th bit is set when this is a buffer record and the record payload
// follows. Otherwise, it's a search key where a pointer to the search key
// follows. This is a workaround for pre-c++17 std::multimap where the search
// key must be of the same type as its key_type.
static constexpr const uint64_t VTHDR_ISREC_MASK = ((uint64_t) 1) << 32;

// The lower 32 bits are used to store an index into the m_tuplebufs. We don't
// expect to have so many items in the volatile tree as it is supposed to be
// mainly used for unit tests. It's unlikely the tree can ever grow that large,
// before oom.
static constexpr const uint64_t VTHDR_BUFIDX_MASK = (((uint64_t) 1) << 32) - 1;

inline static bool
VTHDRGetIsRecord(VolatileTreeRecordHeader *hdr) {
    return hdr->m_flags & VTHDR_ISREC_MASK;
}

inline static void
VTHDRSetIsRecord(VolatileTreeRecordHeader *hdr, bool is_rec) {
    if (is_rec)
        hdr->m_flags |= VTHDR_ISREC_MASK;
    else
        hdr->m_flags &= ~VTHDR_ISREC_MASK;
}

inline static size_t
VTHDRGetBufIdx(VolatileTreeRecordHeader *hdr) {
    return (size_t) (hdr->m_flags & VTHDR_BUFIDX_MASK);
}

inline static void
VTHDRSetBufIdx(VolatileTreeRecordHeader *hdr, size_t bufidx) {
    if (bufidx >= ~(uint32_t)0) {
        LOG(kFatal, "too many records inserted into volatile tree");
    }
    hdr->m_flags = (hdr->m_flags & ~VTHDR_BUFIDX_MASK) |
                   (bufidx & VTHDR_BUFIDX_MASK);
}

inline const IndexKey *&
VTHDRKeyPointer(VolatileTreeRecordHeader *hdr) {
    return *(const IndexKey**)(((char*)hdr) + sizeof(VolatileTreeRecordHeader));
}

inline char *
VTHDRGetData(VolatileTreeRecordHeader *hdr) {
    return ((char*) hdr) + sizeof(VolatileTreeRecordHeader);
}

static absl::flat_hash_map<Oid, std::shared_ptr<VolatileTreeBase>>
    s_volatile_tree_base;

using tree_type = VolatileTree::tree_type;

void
InitVolatileTree() {
    if (!s_volatile_tree_base.empty()) {
        LOG(kWarning, "volatile tree was not properly cleaned up in previoud "
                      "db close");
        CleanVolatileTree();
    }
}

void
CleanVolatileTree() {
    s_volatile_tree_base.clear();
}

VolatileTreeBase::VolatileTreeBase(std::shared_ptr<VolatileTreeFuncs> funcs):
    m_tree(VolatileTreeLess(funcs)),
    m_funcs(std::move(funcs)),
    m_tuplebufs() {}

void
VolatileTree::Initialize(const IndexDesc *idxdesc) {
    if (idxdesc->GetIndexEntry()->idxfid() != INVALID_FID) {
        LOG(kFatal, "VolatileTree does not need a file");
    }

    Oid idxid = idxdesc->GetIndexEntry()->idxid();
    if (s_volatile_tree_base.count(idxid) == 1) {
        LOG(kFatal, "volatile tree index " OID_FORMAT " has been initialized",
                    idxid);
    }

    FieldOffset idxncols = idxdesc->GetIndexEntry()->idxncols();
    std::vector<FunctionInfo> lt_funcs;
    std::vector<FunctionInfo> eq_funcs;
    lt_funcs.reserve(idxncols);
    eq_funcs.reserve(idxncols);
    for (FieldOffset idxcolid = 0; idxcolid < idxncols; ++idxcolid) {
        Oid idxcolltfuncid =
            idxdesc->GetIndexColumnEntry(idxcolid)->idxcolltfuncid();
        if (idxcolltfuncid  == InvalidOid) {
            LOG(kError, "can't build VolatileTree index over type " OID_FORMAT
                        " without \"<\" operator",
                        idxdesc->GetKeySchema()->GetFieldTypeId(idxcolid));
        }
        ASSERT(idxcolltfuncid != InvalidOid);
        FunctionInfo lt_func = FindBuiltinFunction(idxcolltfuncid);
        ASSERT((bool) lt_func);
        lt_funcs.emplace_back(std::move(lt_func));

        Oid idxcoleqfuncid =
            idxdesc->GetIndexColumnEntry(idxcolid)->idxcoleqfuncid();
        if (idxcoleqfuncid == InvalidOid) {
            LOG(kError, "can't build VolatileTree index over type " OID_FORMAT
                        " without \"=\" operator",
                        idxdesc->GetKeySchema()->GetFieldTypeId(idxcolid));
        }
        FunctionInfo eq_func = FindBuiltinFunction(idxcoleqfuncid);
        ASSERT((bool) eq_func);
        eq_funcs.emplace_back(std::move(eq_func));
    }

    // We have to make a copy of the key schema here.
    auto funcs = std::shared_ptr<VolatileTreeFuncs>(new VolatileTreeFuncs{
        *idxdesc->GetKeySchema(), std::move(lt_funcs), std::move(eq_funcs)
    });
    auto base = std::make_shared<VolatileTreeBase>(std::move(funcs));
    s_volatile_tree_base.emplace(idxid, std::move(base));
}

std::unique_ptr<VolatileTree>
VolatileTree::Create(std::shared_ptr<const IndexDesc> idxdesc) {
    Oid idxid = idxdesc->GetIndexEntry()->idxid();
    auto iter = s_volatile_tree_base.find(idxid);
    if (iter == s_volatile_tree_base.end()) {
        LOG(kFatal, "volatile tree index " OID_FORMAT
                    " has not been initialized", idxid);
    }
    return absl::WrapUnique(new VolatileTree(std::move(idxdesc), iter->second));
}

VolatileTree::VolatileTree(std::shared_ptr<const IndexDesc> idxdesc,
                           std::shared_ptr<VolatileTreeBase> base):
    Index(std::move(idxdesc)),
    m_base(std::move(base)) {
}

VolatileTree::~VolatileTree() {
}

bool
VolatileTree::InsertKey(const IndexKey *key, RecordId rid) {
    std::vector<NullableDatumRef> data = key->ToNullableDatumVector();
    maxaligned_char_buf buf(sizeof(VolatileTreeRecordHeader), 0);
    VTHDRSetIsRecord((VolatileTreeRecordHeader*) buf.data(), true);
    if (GetKeySchema()->WritePayloadToBuffer(data, buf) == -1) {
        LOG(kFatal, "key is too long");
    }

    bool has_null = key->HasAnyNull();

    // check for existing key or (key, rid) pair first
    map_iterator iter = m_base->m_tree.lower_bound(buf.data());
    while (iter != m_base->m_tree.end()) {
        auto hdr = (VolatileTreeRecordHeader*) iter ->first;
        const char *keybuf = VTHDRGetData(hdr);
        if (!TupleEqual(key, keybuf, GetKeySchema(),
                       m_base->m_funcs->m_eq_funcs.data())) {
            break;
        }
        if (GetIndexDesc()->GetIndexEntry()->idxunique() && !has_null) {
            // found duplicate key in a unique index
            return false;
        }

        // check if there's a matching record id in a non-unique index
        if (iter->second == rid) {
            // the (key, rid) pair has been inserted into the tree
            return false;
        }
        ++iter;
    }
    VTHDRSetBufIdx((VolatileTreeRecordHeader*) buf.data(),
                   m_base->m_tuplebufs.size());
    m_base->m_tuplebufs.emplace_back(std::move(buf));
    char *tupbuf = m_base->m_tuplebufs.back().data();
    m_base->m_tree.emplace_hint(iter, tupbuf, rid);
    return true;
}

bool
VolatileTree::DeleteKey(const IndexKey *key, RecordId &rid) {
    auto buf = unique_aligned_alloc(8, sizeof(VolatileTreeRecordHeader) +
                                    sizeof(const IndexKey*));
    VolatileTreeRecordHeader *hdr = (VolatileTreeRecordHeader *) buf.get();
    VTHDRSetIsRecord(hdr, false);
    VTHDRKeyPointer(hdr) = key;

    map_iterator iter = m_base->m_tree.lower_bound((char*) hdr);
    while (iter != m_base->m_tree.end()) {
        auto cur_hdr = (VolatileTreeRecordHeader*) iter->first;
        const char *cur_keybuf = VTHDRGetData(cur_hdr);
        if (!TupleEqual(key, cur_keybuf, GetKeySchema(),
                        m_base->m_funcs->m_eq_funcs.data())) {
            break;
        }
        if (!rid.IsValid() || iter->second == rid) {
            auto rechdr = (VolatileTreeRecordHeader*) iter->first;
            ASSERT(VTHDRGetIsRecord(rechdr));
            size_t idx = VTHDRGetBufIdx(rechdr);
            RecordId deleted_rid = iter->second;
            m_base->m_tree.erase(iter);
            // XXX maybe we could free the mem as well by shrinking its
            // capacity
            m_base->m_tuplebufs[idx].clear();
            rid = deleted_rid;
            return true;
        }
        ++iter;
    }
    rid.SetInvalid();
    return false;
}

std::unique_ptr<Index::Iterator>
VolatileTree::StartScan(const IndexKey *lower,
                        bool lower_isstrict,
                        const IndexKey *upper,
                        bool upper_isstrict) {
    if (lower == nullptr) {
        return absl::WrapUnique(new Iterator(this,
                                             m_base->m_tree.begin(),
                                             upper,
                                             upper_isstrict));
    }

    // Can't simply write this key into a buffer as an actual record key,
    // because it might only have a non-null prefix of the keys.
    //
    // The search key is represented as a IndexKey * that immediately follows
    // the VolatileTreeRecordHeader.
    auto buf = unique_aligned_alloc(8, sizeof(VolatileTreeRecordHeader) +
                                    sizeof(const IndexKey*));
    VolatileTreeRecordHeader *hdr = (VolatileTreeRecordHeader *) buf.get();
    VTHDRSetIsRecord(hdr, false);
    VTHDRKeyPointer(hdr) = lower;
    map_iterator iter;
    if (lower_isstrict) {
        iter = m_base->m_tree.upper_bound((char*) hdr);
    } else {
        iter = m_base->m_tree.lower_bound((char*) hdr);
    }
    return absl::WrapUnique(new Iterator(this, iter, upper, upper_isstrict));
}

bool
VolatileTreeLess::operator()(char *buf1, char *buf2) {
    auto hdr1 = (VolatileTreeRecordHeader *) buf1;
    auto hdr2 = (VolatileTreeRecordHeader *) buf2;
    int res;
    if (!VTHDRGetIsRecord(hdr1)) {
        ASSERT(VTHDRGetIsRecord(hdr2));
        const IndexKey *key1 = VTHDRKeyPointer(hdr1);
        res = TupleCompare(key1, VTHDRGetData(hdr2),
                           &m_funcs->m_key_schema,
                           m_funcs->m_lt_funcs.data(),
                           m_funcs->m_eq_funcs.data());
    } else if (!VTHDRGetIsRecord(hdr2)) {
        const IndexKey *key2 = VTHDRKeyPointer(hdr2);
        res = -TupleCompare(key2, VTHDRGetData(hdr1),
                            &m_funcs->m_key_schema,
                            m_funcs->m_lt_funcs.data(),
                            m_funcs->m_eq_funcs.data());
    } else {
        // both are records, need to dissemble one of them
        std::vector<Datum> buf1_data =
            m_funcs->m_key_schema.DissemblePayload(VTHDRGetData(hdr1));
        auto key1 = IndexKey::Create(buf1_data);
        res = TupleCompare(key1.get(), VTHDRGetData(hdr2),
                           &m_funcs->m_key_schema,
                           m_funcs->m_lt_funcs.data(),
                           m_funcs->m_eq_funcs.data());
    }
    return res < 0;
}

VolatileTree::Iterator::Iterator(VolatileTree *index,
                                 VolatileTree::map_iterator iter,
                                 const IndexKey *upper,
                                 bool upper_isstrict):
    Index::Iterator(index),
    m_iter(iter),
    m_rec(),
    m_upper((upper) ? upper->Copy() : nullptr),
    m_upper_data_buffer(),
    m_is_first(true),
    m_upper_isstrict(upper_isstrict) {

    // make sure we do not have a dangling pointer to some deallocated Datum
    if (m_upper.get())
        m_upper->DeepCopy(index->GetKeySchema(), m_upper_data_buffer);
}

bool
VolatileTree::Iterator::Next() {
    if (m_iter == GetIndexAs<VolatileTree>()->m_base->m_tree.end()) {
        return false;
    }

    VolatileTree *index = GetIndexAs<VolatileTree>();

    if (m_is_first) {
        m_is_first = false;
    } else {
        ++m_iter;
        if (m_iter == index->m_base->m_tree.end()) {
            return false;
        }
    }

    auto hdr = (VolatileTreeRecordHeader *) m_iter->first;
    const char *buf = VTHDRGetData(hdr);
    if (m_upper.get()) {
        int res = TupleCompare(m_upper.get(), buf,
                               index->GetKeySchema(),
                               index->m_base->m_funcs->m_lt_funcs.data(),
                               index->m_base->m_funcs->m_eq_funcs.data());
        if (res <= (m_upper_isstrict ? 0 : -1)) {
            // done
            m_iter = index->m_base->m_tree.end();
            return false;
        }
    }

    m_rec.GetData() = buf;
    m_rec.GetLength() = (FieldOffset)(
        index->m_base->m_tuplebufs[VTHDRGetBufIdx(hdr)].size());
    m_rec.GetRecordId() = m_iter->second;
    return true;
}

bool
VolatileTree::Iterator::IsAtValidItem() {
    if (m_is_first ||
        m_iter == GetIndexAs<VolatileTree>()->m_base->m_tree.end()) {
        return false;
    }
    return true;
}

}   // namespace taco

