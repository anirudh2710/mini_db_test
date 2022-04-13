// catalog/Schema.h
#ifndef CATALOG_SCHEMA_H
#define CATALOG_SCHEMA_H

#include "tdb.h"

#include <vector>

namespace taco {

class BootstrapCatCache;

/*!
 * A Schema object stores the information for accessing an ordered set of typed
 * fields either from a disk-based record payload, or from an in-memory
 * Datum/DatumRef array. A field value is always treated as an array of
 * fixed-length or variable-length bytes with certain alignment requirements,
 * and one needs to use the type-dependent functions to interpret the field
 * value (which is not handled by Schema). A successfully computed Schema
 * object supports queries of the field nullness, offset and length in a
 * max-aligned record payload and guarantees the offset returned is suitable
 * for read or write for the specified type.
 *
 * The fields may be optionally named, which is usually the case for a schema
 * object created for a record read from or derived from a file, or for the
 * result of query processing.
 *
 * The layout of the fields is:
 *
 *
 * | non-nullable fixed-len fields |
 * | null bitmap |
 * | varlen field end array + alignment padding |
 * | varlen fields |
 * | nullable fixed-len fields |
 *
 * The entire payload is always maximum aligned to 8-byte boundary at the end.
 */
class Schema {
private:
    struct FieldInfo {
        /*! The type ID of this field */
        Oid                m_typid;

        /*! Cached copy of the typlen. Not set if layout is not computed. */
        int16_t            m_typlen;

        /*! Cached copy of typalign. Not set if layout is not computed. */
        int8_t            m_typalign;

        /*!
         * Whether this field is passed by ref. Not set if layout is not
         * computed.
         */
        bool            m_typbyref;

        /*! The type parameter of this field. */
        uint64_t        m_typparam;

        /*!
         * Stores about the null bit of this field in the payload layout.
         * Possible values:
         *
         *    < 0: not nullable.
         *    Any valid regular FieldId i: this field's null bit is i_th bit.
         *
         * When the null bit ID hasn't been computed for a nullable field,
         * it is set to some non-negative value.
         */
        FieldId            m_nullbit_id;

        /*!
         * Stores information about how to find this field in the payload
         * layout. Possible values:
         *
         *    < 0: this is the (-offset - 1)^th variable-length field from left
         *    to right, or this is the (-offset - 1)^th nullable fixed-length
         *    field from left to right.
         *
         *    >= 0: the actual offset of a non-nullable fixed-length field
         *    relative to the beginning of the payload.
         *
         *    This field is undefined when the layout hasn't been computed.
         */
        FieldOffset        m_offset;
    };

    /*!
     * Constructs a Schema object without initializing its payload. The vectors
     * typid, typparam and nullable must be of the same length and may not be
     * empty. The vector field_names may be either empty or of the same length
     * as typid.
     *
     * @param typid a vector of type Oid of the fields in this sc
     * @param typparam a vector of type parameter of the fields
     * @param nullable a vector of nullability of the fields
     * @param field_names a vector of field_names
     */
    Schema(const std::vector<Oid> &typid,
           const std::vector<uint64_t> &typparam,
           const std::vector<bool> &nullable,
           std::vector<std::string> field_names);

    Schema(const std::vector<FieldInfo>& fields,
           const std::vector<std::string>& field_names);

    /*!
     * The default constructor for constructing a fake schema.
     */
    Schema() = default;

public:
    /*!
     * Creates a new schema with the given types and field names. The returned
     * schema object is not fully initialized. To do so, the caller either
     * needs to call `Schema::ComputeLayout()` (for a schema that will be used
     * for reading/writing record payloads) or `Schema::CollectTypeInfo()` (for
     * a schema that will be used for caching the type info only).
     */
    static Schema *Create(const std::vector<Oid> &typid,
                          const std::vector<uint64_t> &typparam,
                          const std::vector<bool> &nullable);

    /*!
     * Creates a new schema with the given types without field names. If one
     * calls `GetFieldName()` on the returned schema object, it will return an
     * empty string. The returned schema object is not fully initialized. To do
     * so, the caller either needs to call `Schema::ComputeLayout()` (for a
     * schema that will be used for reading/writing record payloads) or
     * `Schema::CollectTypeInfo()` (for a schema that will be used for caching
     * the type info only).
     */
    static Schema *Create(const std::vector<Oid> &typid,
                          const std::vector<uint64_t> &typparam,
                          const std::vector<bool> &nullable,
                          std::vector<std::string> field_names);

    /*!
     * Combines two schema into one.
     */
    static Schema *Combine(const Schema* left, const Schema* right);

    /*!
     * Returns whether the two schemas are identical (i.e., having the same
     * numbers of fields, the same types for the fields with the same field id,
     * and the nullness of the fields).
     */
    static bool Identical(const Schema* left, const Schema* right);

    /*!
     * Returns if the schema on the right is union compatible with the left
     * (i.e., having the same number of fields, the same types for each field with same
     * field id, and making sure right is not nullable if left does not allow
     * nulls.)
     */
    static bool Compatible(const Schema* left, const Schema* right);

private:
    /*!
     * The generic implementation of schema layout computation with some
     * catalog cache class CCache. CCache is required to have a function
     * ``const SysTable_Type *FindType(Oid)''.
     */
    template<class CCache>
    void ComputeLayoutImpl(CCache *catcache, bool cache_typinfo_only);

public:
    /*!
     * Computes the layout of the record payload with this schema, using the
     * global catalog cache g_db->catcache(). This also collects the type info.
     */
    void ComputeLayout();

    /*!
     * Computes the layout of the record payload with this schema, using the
     * provided Bootstrap catelog cache. This also collects the type info.
     *
     * This should only be used at DB startup and testing.
     */
    void ComputeLayout(BootstrapCatCache *catcache);

    /*!
     * Collects the type info only without computing the record payload layout.
     * This may be used when the Schema is in-memory only, i.e., the caller
     * does not need to call the field extraction and serialization functions,
     * (e.g., DissemblePayload, GetField, WritePayloadToBuffer).
     */
    void CollectTypeInfo();

    constexpr bool
    IsTypeInfoCollected() const {
        return m_type_info_collected;
    }

    constexpr bool
    IsLayoutComputed() const {
        return m_layout_computed;
    }

private:

    void EnsureLayoutComputed() const {
        if (!IsLayoutComputed()) {
            LOG(kFatal, "Schema::ComputeLayout() must be called first "
                        "before querying the on-disk layout of a Schema");
        }
    }

    void EnsureTypeInfoCollected() const {
        if (!IsTypeInfoCollected()) {
            LOG(kFatal, "Schema::CollectTypeInfo() or Schema::ComputeLayout() "
                        "must be called first before querying the in-memory "
                        "type info of a schema");
        }
    }

public:

    /*!
     * Returns the type Oid of a field.
     */
    inline constexpr Oid
    GetFieldTypeId(FieldId field_id) const {
        return m_field[field_id].m_typid;
    }

    /*!
     * Returns the type parameter of a field.
     */
    inline constexpr uint64_t
    GetFieldTypeParam(FieldId field_id) const {
        return m_field[field_id].m_typparam;
    }

    /*!
     * Returns the field name of field \p field_id. If no field name was given
     * when creating this schema object, it returns an empty string.
     */
    inline absl::string_view
    GetFieldName(FieldId field_id) const {
        if ((size_t) field_id >= m_field_names.size()) {
            return "";
        }
        return m_field_names[field_id];
    }

    /*!
     * Returns whether this field is nullable.
     */
    inline bool
    FieldIsNullable(FieldId field_id) const {
        return m_field[field_id].m_nullbit_id >= 0;
    }

    /*!
     * Returns whether this field is passed by reference or by value in memory.
     *
     * Note that both variable-length fields and some fixed-length fields (that
     * are not 1,2,4,8 bytes long) are passed by reference.
     */
    inline bool
    FieldPassByRef(FieldId field_id) const {
        return m_field[field_id].m_typbyref;
    }

    /*!
     * Returns the cached size of a field. If this field is variable-length
     * or is fixed-length with unknown type parameter, returns -1.
     */
    FieldOffset
    GetFieldLength(size_t field_id) const {
        EnsureTypeInfoCollected();
        return m_field[field_id].m_typlen;
    }

    /*!
     * Returns the total number of fields.
     */
    inline FieldId
    GetNumFields() const {
        return (FieldId) m_field.size();
    }

    /*!
     * Returns the field ID of the field with the ``field_name''.
     *
     * Or InvalidFieldId if there's no such field.
     */
    FieldId GetFieldIdFromFieldName(absl::string_view field_name) const;

    /*!
     * Returns the offset and the length of a field in this schema as a pair.
     *
     * @param field_id the column ID of the field
     * @param payload the record payload
     */
    std::pair<FieldOffset, FieldOffset> GetOffsetAndLength(
        FieldId field_id,
        const char *payload) const;

    /*!
     * See Schema::WritePayloadToBufferImpl().
     */
    FieldOffset WritePayloadToBuffer(const std::vector<Datum> &data,
                                     maxaligned_char_buf &buf) const;

    /*!
     * See Schema::WritePayloadToBufferImpl().
     */
    FieldOffset WritePayloadToBuffer(const std::vector<DatumRef> &data,
                                     maxaligned_char_buf &buf) const;

    /*!
     * See Schema::WritePayloadToBufferImpl().
     */
    FieldOffset WritePayloadToBuffer(const std::vector<NullableDatumRef> &data,
                                     maxaligned_char_buf &buf) const;


    /*!
     * Returns whether a field is null or not in a record payload.
     */
    bool FieldIsNull(FieldId field_id, const char *payload) const;

    /*!
     * Returns a field in the payload as a Datum. The returned datum references
     * the payload so the payload must be alive when the return value is use.
     * However, any in-place change to payload may or may not be reflected in
     * the returned datum.
     */
    Datum GetField(FieldId field_id, const char *payload) const;

    /*!
     * Dissemble the payload into a vector of Datums in field order as defined
     * in the schema. The data in the returned vector are a reference into the
     * passed payload so the payload must be alive when the return value is
     * used. However, any in-place change to payload may or may not be
     * reflected in the returned data vector.
     */
    std::vector<Datum> DissemblePayload(const char *payload) const;


private:
    /*!
     * Convert the data as bytes in storage layout and append them to the buf
     * without clear it first. This allows one to add an optional header before
     * the payload. buf will be MAXALIGN'd before any data is appended into it.
     *
     * \p buf is an std::vector of char with a different allocator that always
     * uses aligned_alloc for allocating buffer spaces aligned to 8-byte
     * boundaries. Always use the type alias maxaligned_char_buf to declare or
     * define such a buffer (see base/tdb_base.h).
     *
     * It is undefined if the size of data is not the same as `GetNumFields()`.
     *
     * @returns the length of the payload (not including the initial MAXALIGN
     * padding), or -1 if the length of the buf exceeds the maximum limit (max
     * FieldOffset)
     */
    template<class SomeDatum>
    FieldOffset WritePayloadToBufferImpl(const std::vector<SomeDatum> &data,
                                         maxaligned_char_buf &buf) const;

    /*! whether the layout has been computed */
    bool m_type_info_collected;

    bool m_layout_computed;

    bool m_has_only_nonnullable_fixedlen_fields;

    FieldId m_num_nonnullable_fixedlen_fields;

    FieldId m_num_nullable_fixedlen_fields;

    FieldId m_num_varlen_fields;

    /*!
     * The offset to the null bitmap.
     */
    FieldOffset m_null_bitmap_begin;

    /*!
     * The offset to the beginning of the variable-length field end array. This
     * is the length of the entire record if there's no variable-length field.
     */
    FieldOffset m_varlen_end_array_begin;

    /*!
     * The offset to the beginning of the varlen payload. This might not be
     * properly aligned to the first field in the payload. So anyone using this
     * offset needs to ensure the data are aligned per their alignment
     * requirements.
     *
     * However, in case this is a schema with only non-nullable fixed-length
     * fields, or this is a build with fixed-length data page only, this is
     * the record length.
     */
    FieldOffset m_varlen_payload_begin;

    /*!
     * The order of the fields to be placed in the actual payload. E.g.,
     * m_field_order_idx = { 0, 2, 3, 1} means the field 0 is first in the
     * payload, the field 2 is the second, the field 3 is the third, and the
     * field 1 is the fourth.
     */
    std::vector<FieldId> m_field_reorder_idx;

    /*! information about the individual fields */
    std::vector<FieldInfo> m_field;

    /*! optional field names (may be empty or of the same length as m_field) */
    std::vector<std::string> m_field_names;
};

}   // namespace taco

#endif      // CATALOG_SCHEMA_H
