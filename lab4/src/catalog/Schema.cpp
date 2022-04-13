// src/catalog/Schema.cpp
#include "catalog/Schema.h"

#include "catalog/CatCache.h"
#include "catalog/BootstrapCatCache.h"
#include "catalog/systables.h"
#include "utils/builtin_funcs.h"

namespace taco {

static void inline
copy_bytes(bool passbyref, int16_t len, const char *src, char *tgt) {
    if (passbyref) {
        memcpy(tgt, src, len);
    } else {
        switch (len) {
        case 1:
            *(uint8_t*) tgt = *(const uint8_t*) src;
            break;
        case 2:
            *(uint16_t*) tgt = *(const uint16_t*) src;
            break;
        case 4:
            *(uint32_t*) tgt = *(const uint32_t*) src;
            break;
        default:
            *(uint64_t*) tgt = *(const uint64_t*) src;
        }
    }
}

Schema::Schema(const std::vector<Oid> &typid,
               const std::vector<uint64_t> &typparam,
               const std::vector<bool> &nullable,
               std::vector<std::string> field_names):
    m_type_info_collected(false),
    m_layout_computed(false),
    m_field(typid.empty() ? 1 : typid.size()),
    m_field_names(std::move(field_names)) {

    ASSERT(!typid.empty() && ((FieldId) typid.size()) <= MaxNumRegularFieldId);
    ASSERT(typid.size() == typparam.size());
    ASSERT(typid.size() == nullable.size());
    ASSERT(m_field_names.empty() ||
           typid.size() == m_field_names.size());

    for (FieldId i = 0; i < (FieldId) typid.size(); ++i) {
        m_field[i].m_typid = typid[i];
        m_field[i].m_typparam = typparam[i];
        m_field[i].m_nullbit_id = nullable[i] ? 0 : -1;
        m_field[i].m_offset = 0;
    }
}

Schema::Schema(const std::vector<FieldInfo>& fields,
               const std::vector<std::string>& field_names):
    m_type_info_collected(false),
    m_layout_computed(false),
    m_field(std::move(fields)),
    m_field_names(std::move(field_names)) {}

Schema*
Schema::Create(const std::vector<Oid> &typid,
               const std::vector<uint64_t> &typparam,
               const std::vector<bool> &nullable) {

    if (typid.empty() ||
        typid.size() != typparam.size() ||
        typid.size() != nullable.size()) {
        return nullptr;
    }
    return new Schema(typid, typparam, nullable, {});
}

Schema*
Schema::Combine(const Schema* left, const Schema* right) {
    std::vector<FieldInfo> fields;
    fields.reserve(left->m_field.size() + right->m_field.size());
    fields.insert(fields.end(), left->m_field.begin(), left->m_field.end());
    fields.insert(fields.end(), right->m_field.begin(), right->m_field.end());

    std::vector<std::string> field_names;
    field_names.reserve(left->m_field_names.size() + right->m_field_names.size());
    field_names.insert(field_names.end(),
                       left->m_field_names.begin(),
                       left->m_field_names.end());
    field_names.insert(field_names.end(),
                       right->m_field_names.begin(),
                       right->m_field_names.end());

    return new Schema(fields, field_names);
}

bool
Schema::Identical(const Schema* left, const Schema* right) {
    if (left->m_field.size() != right->m_field.size()) {
        return false;
    }
    for (size_t i = 0; i < left->m_field.size(); ++i) {
        if (left->m_field[i].m_typid != right->m_field[i].m_typid)
            return false;
        else if (left->m_field[i].m_typparam != right->m_field[i].m_typparam)
            return false;
        else if ((left->m_field[i].m_nullbit_id >= 0
                  && right->m_field[i].m_nullbit_id < 0) ||
                 (left->m_field[i].m_nullbit_id < 0
                  && right->m_field[i].m_nullbit_id >= 0))
            return false;
    }
    return true;
}

bool
Schema::Compatible(const Schema* left, const Schema* right) {
    if (left->m_field.size() != right->m_field.size()) {
        return false;
    }
    for (size_t i = 0; i < left->m_field.size(); ++i) {
        if (left->m_field[i].m_typid != right->m_field[i].m_typid)
            return false;
        else if (left->m_field[i].m_nullbit_id < 0
                 && right->m_field[i].m_nullbit_id >= 0)
            return false;
    }
    return true;
}

Schema*
Schema::Create(const std::vector<Oid> &typid,
               const std::vector<uint64_t> &typparam,
               const std::vector<bool> &nullable,
               std::vector<std::string> field_names) {

    if (typid.empty() ||
        typid.size() != typparam.size() ||
        typid.size() != nullable.size() ||
        (!field_names.empty() && typid.size() != field_names.size())) {
        return nullptr;
    }
    return new Schema(typid, typparam, nullable, std::move(field_names));
}


template<class CCache>
void
Schema::ComputeLayoutImpl(CCache *catcache, bool cache_typinfo_only) {
    FieldOffset off = 0;
    FieldId num_fields = GetNumFields();
    FieldId num_nonnullable_fixedlen_fields = 0;
    FieldId num_nullable_fixedlen_fields = 0;
    FieldId num_varlen_fields = 0;
    FieldId num_nullable_fields = 0;
    FieldId num_nullable_varlen_fields = 0;

    if (!cache_typinfo_only) {
        m_field_reorder_idx.resize(num_fields);
    }

    // 1. Computes the offsets and reorder index of non-nullable fixed-len
    // fields. Also caches the typlen, type alignment and m_offset values for
    // all other fields. We also compute the number of fields in each section
    // here.
    for (FieldId i = 0; i < num_fields; ++i) {
        Oid typid = m_field[i].m_typid;
        // Must use auto to declare the type here:
        // BootstrapCatCache returns const SysTable_Type* while the regular
        // catalog cache returns std::shared_ptr<const SysTable_Type>.
        auto typ = catcache->FindType(typid);
        m_field[i].m_typlen = typ->typlen();
        m_field[i].m_typalign = typ->typalign();

        if (!typ->typisvarlen() && typ->typlenfunc() != InvalidOid) {
            // Need to calculate the type length for those fixed-length types
            // with type parameters. If the type parameter is invalid, this may
            // return -1 which is treated the same as variable-length below.
            FunctionInfo f = FindBuiltinFunction(typ->typlenfunc());

            Datum arg1 = Datum::From(m_field[i].m_typparam);
            Datum res = FunctionCall(f, arg1);
            ASSERT(!res.isnull());
            m_field[i].m_typlen = res.GetInt16();
        }

        if (typ->typisvarlen() || m_field[i].m_typlen == -1) {

            // Variable-length field:
            // The typlen of a varlen field must be -1 in the catalog, but let's
            // ensure that happens even if someone messes up with the catalog.
            // We need this to differentiate between varlen fields and nullable
            // fixed-len fields.
            if (m_field[i].m_typlen != -1)
                m_field[i].m_typlen = -1;

            // variable-length field must be passed by reference
            ASSERT(typ->typbyref());
            m_field[i].m_typbyref = true;

            if (!cache_typinfo_only) {
                // assign the index into the varlen end
                // array. The offset value is -m_offset + 1, so make sure we
                // increment the num_varlen_fields before assigning the index.
                ++num_varlen_fields;
                m_field[i].m_offset = -num_varlen_fields;

                if (m_field[i].m_nullbit_id >= 0) {
                    ++num_nullable_varlen_fields;
                }
            }
        } else {
            m_field[i].m_typbyref = typ->typbyref();

            // Need to calculate the type length for those with type parameters
            if (typ->typlenfunc() != InvalidOid) {
                ASSERT(m_field[i].m_typlen >= 0);
            } else {
                // pass-by-value types must a length of 1, 2, 4 or 8 currently
                ASSERT(m_field[i].m_typlen <= 8 &&
                        ((m_field[i].m_typlen - 1) & m_field[i].m_typlen) == 0);
            }

            if (!cache_typinfo_only) {
                if (m_field[i].m_nullbit_id >= 0) {
                    // nullable fixed-length field
                    ++num_nullable_fixedlen_fields;
                    m_field[i].m_offset = -num_nullable_fixedlen_fields;
                } else {
                    // non-nullable fixed-length field
                    // This is the only case where we can directly compute a fixed
                    // offset.
                    uint8_t align = typ->typalign();
                    // check for overflow after alignment
                    RETURN_IF((off = TYPEALIGN(align, off)) < 0);
                    m_field[i].m_offset = off;
                    RETURN_IF(!AddWithCheck(off, m_field[i].m_typlen));

                    m_field_reorder_idx[num_nonnullable_fixedlen_fields] = i;
                    ++num_nonnullable_fixedlen_fields;
                }
            }
        }
    }

    m_type_info_collected = true;
    if (cache_typinfo_only) {
        return ;
    }

    m_num_nonnullable_fixedlen_fields = num_nonnullable_fixedlen_fields;
    m_num_nullable_fixedlen_fields = num_nullable_fixedlen_fields;
    m_num_varlen_fields = num_varlen_fields;

    if (num_nonnullable_fixedlen_fields == num_fields) {
        // fast path for a schema with all non-nullable fixed-length fields
        RETURN_IF(MAXALIGN(off) < 0);
        off = MAXALIGN(off);
        m_null_bitmap_begin = off;
        m_varlen_end_array_begin = off;
        m_varlen_payload_begin = off;
        m_has_only_nonnullable_fixedlen_fields = true;
        m_layout_computed = true;
        return ;
    }

    m_has_only_nonnullable_fixedlen_fields = false;
    m_null_bitmap_begin = off;

    // 2. Assign the remaining of the reorder indexes, and the null bit id.
    // Also computes the number of nullable fields.
    //
    // The null bits follow the same order as the fields are laid in the
    // schema layout, rather than the FieldId number. For example,
    // suppose Field 2 and 3 are nullable variable-length fields, while Field 0
    // is a nullable fixed-length field. Then the order in the bitmap
    // is 2, 3, 0.
    for (FieldId i = 0; i < num_fields; ++i) {
        if (m_field[i].m_typlen == -1) {
            // variable-len field
            FieldId field_seqno = num_nonnullable_fixedlen_fields +
                (-m_field[i].m_offset - 1);
            m_field_reorder_idx[field_seqno] = i;

            if (m_field[i].m_nullbit_id >= 0) {
                m_field[i].m_nullbit_id = (-m_field[i].m_offset - 1);
                ++num_nullable_fields;
            }
        } else {
            if (m_field[i].m_nullbit_id >= 0) {
                // nullable fixed-len field
                // They follow the non-nullable fixed-len fields in order.
                FieldId field_seqno = num_nonnullable_fixedlen_fields +
                    num_varlen_fields +
                    (-m_field[i].m_offset - 1);
                m_field_reorder_idx[field_seqno] = i;
                m_field[i].m_nullbit_id = -m_field[i].m_offset - 1 +
                                           num_nullable_varlen_fields;
                ++num_nullable_fields;
            }
        }
    }

    // 3. Compute the null bitmap's size.  The number of bytes needed for the
    // null bitmap is ceil(num_nullable_fields / 8). No need to MAXALIGN the
    // offset here. We'll do that below.
    m_null_bitmap_begin = off;
    RETURN_IF(!AddWithCheck(off, (num_nullable_fields + 7) >> 3));

    // 4. Compute the offset to the varlen end array and the beginning
    // of the varlen payload.
    RETURN_IF((off = TYPEALIGN(sizeof(FieldOffset), off)) < 0);
    m_varlen_end_array_begin = off;
    RETURN_IF(!AddWithCheck(off,
                            sizeof(FieldOffset) * num_varlen_fields));
    m_varlen_payload_begin = off;

    m_layout_computed = true;
}

void
Schema::ComputeLayout() {
    ComputeLayoutImpl(g_db->catcache(), false);
}

void
Schema::ComputeLayout(BootstrapCatCache *catcache) {
    ComputeLayoutImpl(catcache, false);
}

void
Schema::CollectTypeInfo() {
    ComputeLayoutImpl(g_db->catcache(), true);
}

std::pair<FieldOffset, FieldOffset>
Schema::GetOffsetAndLength(
    FieldId field_id,
    const char *payload) const {
    // We shouldn't run into any overflows when computing the offsets, as
    // the payload is a constructed record of this schema, where the offsets
    // are already validated against any overflow. So any overflow in
    // this function is an assertion failure rather than a run-time error.

    if (m_field[field_id].m_offset >= 0) {
        // non-nullable fixed-length field
        return std::make_pair(m_field[field_id].m_offset,
                              m_field[field_id].m_typlen);
    }

    if (m_field[field_id].m_typlen == -1) {
        FieldOffset begin;
        FieldOffset end;
        // variable-length field
        const FieldOffset *varlen_end =
            reinterpret_cast<const FieldOffset*>(payload +
                                                 m_varlen_end_array_begin);
        FieldId varlen_idx = -m_field[field_id].m_offset - 1;

        // find the end offset
        end = varlen_end[varlen_idx];

        // find the (unaligned) begin offset
        if (varlen_idx > 0) {
            begin = varlen_end[varlen_idx - 1];
        } else {
            begin = m_varlen_payload_begin;
        }

        begin = TYPEALIGN(m_field[field_id].m_typalign, begin);
        ASSERT(begin >= 0, "unexpected field offset overflow "
                           "at field " FIELDID_FORMAT
                           " from offset " FIELDOFFSET_FORMAT,
                           field_id, m_varlen_payload_begin);
        // We never checked if this field is NULL or not, so there is still
        // a chance that end < begin if this is a NULL field.
        return std::make_pair(begin, (end >= begin) ? (end - begin) : 0);
    }

    // nullable fixed-len fields. These come last and we need to scan
    // the null bitmap in the storage order to figure out the offset.
    FieldId seqno = m_num_nonnullable_fixedlen_fields + m_num_varlen_fields;
    FieldOffset off;
    if (m_num_varlen_fields == 0) {
        off = m_varlen_payload_begin;
    } else {
        const FieldOffset *varlen_end =
            reinterpret_cast<const FieldOffset*>(payload +
                                                 m_varlen_end_array_begin);
        off = varlen_end[m_num_varlen_fields - 1];
    }

    const uint8_t *null_bitmap =
        reinterpret_cast<const uint8_t*>(payload + m_null_bitmap_begin);
    while (m_field_reorder_idx[seqno] != field_id) {
        FieldId i = m_field_reorder_idx[seqno];
        FieldId nullbit_id = m_field[i].m_nullbit_id;
        if (!(null_bitmap[nullbit_id >> 3] & (1 << (nullbit_id & 7)))) {
            // it's not NULL
            FieldOffset newoff = TYPEALIGN(m_field[i].m_typalign, off);
            ASSERT(newoff >= 0, "unexpected field offset overflow "
                                "at field " FIELDID_FORMAT
                                " from offset " FIELDOFFSET_FORMAT,
                                i, off);
            off = newoff + m_field[i].m_typlen;
            ASSERT(off >= 0, "unexpected field offset overflow "
                             "at field " FIELDID_FORMAT
                             " from offset " FIELDOFFSET_FORMAT,
                             i, newoff);
        }
        ++seqno;
    }
    FieldOffset begin;
    begin = TYPEALIGN(m_field[field_id].m_typalign, off);
    ASSERT(begin >= 0, "unexpected field offset overflow "
                       "at field " FIELDID_FORMAT
                       " from offset " FIELDOFFSET_FORMAT,
                       field_id, off);
    return std::make_pair(begin, m_field[field_id].m_typlen);
}

FieldOffset
Schema::WritePayloadToBuffer(const std::vector<Datum> &data,
                             maxaligned_char_buf &buf) const {
    return WritePayloadToBufferImpl(data, buf);
}

FieldOffset
Schema::WritePayloadToBuffer(const std::vector<DatumRef> &data,
                             maxaligned_char_buf &buf) const {
    return WritePayloadToBufferImpl(data, buf);
}

FieldOffset
Schema::WritePayloadToBuffer(const std::vector<NullableDatumRef> &data,
                             maxaligned_char_buf &buf) const {
    return WritePayloadToBufferImpl(data, buf);
}

template<class SomeDatum>
FieldOffset
Schema::WritePayloadToBufferImpl(const std::vector<SomeDatum> &data,
                                 maxaligned_char_buf &buf) const {
    if (buf.size() >= (size_t) std::numeric_limits<FieldOffset>::max()) {
        return -1;
    }
    FieldOffset init_len = (FieldOffset) buf.size();
    init_len = MAXALIGN(init_len);
    RETURN_IF(init_len < 0, -1);
    RETURN_IF(init_len + m_varlen_payload_begin < 0, -1);
    // XXX avoid frequent allocations with small payload sizes, this
    // probably shouldn't be a fixed capacity though.
    buf.reserve(64);
    buf.resize(init_len + m_varlen_payload_begin);

    // This does not need to be computed until we reach the varlen payload,
    // where the buffer may be reallocated.
    FieldOffset off = m_varlen_payload_begin;
    char *payload_begin = buf.data() + init_len;
    for (FieldId field_id : m_field_reorder_idx) {
        FieldOffset field_len;

        const char *field_bytes;

        if (m_field[field_id].m_offset >= 0) {
            // non-nullable fixed-len field
            if (data[field_id].isnull()) {
                LOG(kError, "NULL value passed to non-null field "
                            FIELDID_FORMAT, field_id);
            }
            field_len = m_field[field_id].m_typlen;
            if (m_field[field_id].m_typbyref) {
                // pass by reference
                field_bytes = data[field_id].GetVarlenBytes();
                copy_bytes(true, field_len, data[field_id].GetVarlenBytes(),
                           payload_begin + m_field[field_id].m_offset);
            } else {
                // pass by value
                field_bytes = data[field_id].GetFixedlenBytes();
                copy_bytes(false, field_len, data[field_id].GetFixedlenBytes(),
                           payload_begin + m_field[field_id].m_offset);
            }
        } else {
            // set null bit if necessary
            if (data[field_id].isnull()) {
                char *null_bitmap = payload_begin + m_null_bitmap_begin;
                null_bitmap[m_field[field_id].m_nullbit_id >> 3] |=
                    1 << (m_field[field_id].m_nullbit_id & 7);

                // set the record end offset for a varlen field
                if (m_field[field_id].m_typlen == -1) {
                    FieldOffset *varlen_end_array = (FieldOffset *)(
                        payload_begin + m_varlen_end_array_begin);
                    varlen_end_array[-m_field[field_id].m_offset - 1] = off;
                }
                // nothing to do for a nullable fixed-len field
                continue;
            }

            // It's non-null. First, align the offset.
            FieldOffset newoff = TYPEALIGN(m_field[field_id].m_typalign,
                                           off);
            RETURN_IF(init_len + newoff < 0, -1);

            // non-null datum, continue with copying the content below
            if (m_field[field_id].m_typlen == -1) {
                // variable-len field
                field_len = (FieldOffset) data[field_id].GetVarlenSize();
                field_bytes = data[field_id].GetVarlenBytes();

                // update the varlen end array
                FieldOffset *varlen_end_array = (FieldOffset *) (
                    payload_begin + m_varlen_end_array_begin);
                varlen_end_array[-m_field[field_id].m_offset - 1] =
                    newoff + field_len;
            } else {
                // nullable fixed-len field
                field_len = m_field[field_id].m_typlen;
                if (m_field[field_id].m_typbyref) {
                    // pass by reference
                    field_bytes = data[field_id].GetVarlenBytes();
                } else {
                    // pass by value
                    field_bytes = data[field_id].GetFixedlenBytes();
                }
            }
            RETURN_IF(init_len + newoff + field_len < 0, -1);

            ASSERT(off + init_len == (FieldOffset) buf.size());
            buf.resize(newoff + field_len + init_len);
            // the buffer may be reallocated, so recompute the
            // payload_begin
            payload_begin = buf.data() + init_len;
            copy_bytes(m_field[field_id].m_typbyref,
                       field_len, field_bytes,
                       payload_begin + newoff);
            off = newoff + field_len;
        }
    }

    ASSERT(off + init_len == (FieldOffset) buf.size());
    off = MAXALIGN(off);
    RETURN_IF(init_len + off < 0, -1);
    if (off + init_len > (FieldOffset) buf.size()) {
        buf.resize(off + init_len);
    }
    return off;
}


bool
Schema::FieldIsNull(FieldId field_id, const char *payload) const {
    // non-nullable
    if (m_field[field_id].m_nullbit_id < 0)
        return false;

    // otherwise, check the null bit
    const uint8_t *null_bitmap =
        reinterpret_cast<const uint8_t*>(payload + m_null_bitmap_begin);
    auto i = m_field[field_id].m_nullbit_id;
    return null_bitmap[i >> 3] & (1 << (i & 7));
}

Datum
Schema::GetField(FieldId field_id, const char *payload) const {
    if (FieldIsNull(field_id, payload)) {
        return Datum::FromNull();
    }

    auto p = GetOffsetAndLength(field_id, payload);
    if (m_field[field_id].m_typlen == -1) {
        // varlen field
        return Datum::FromVarlenBytes(payload + p.first, p.second);
    }

    if (m_field[field_id].m_typbyref) {
        // pass-by-reference fixed-len field
        return Datum::FromVarlenBytes(payload + p.first,
                                      m_field[field_id].m_typlen);
    }


    // pass-by-value fixed-len field
    return Datum::FromFixedlenBytes(payload + p.first, p.second);
}

std::vector<Datum>
Schema::DissemblePayload(const char *payload) const {
    std::vector<Datum> ret;
    FieldId n = GetNumFields();
    for (FieldId i = 0; i < n; ++i) {
        ret.emplace_back(GetField(i, payload));
    }
    return ret;
}

FieldId
Schema::GetFieldIdFromFieldName(absl::string_view field_name) const {
    FieldId n = (FieldId) m_field_names.size();
    for (FieldId i = 0; i < n; ++i) {
        if (field_name == m_field_names[i])
            return i;
    }
    return InvalidFieldId;
}

}   // namespace taco
