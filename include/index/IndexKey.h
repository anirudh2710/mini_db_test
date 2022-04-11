#ifndef INDEX_INDEXKEY_H
#define INDEX_INDEXKEY_H

#include "tdb.h"

#include "catalog/Schema.h"

namespace taco {

struct IndexKey;

//! The returned smart pointer of IndexKey::Create().
typedef std::unique_ptr<IndexKey, AlignedAllocImpl::FreeMem> UniqueIndexKey;

/*!
 * An IndexKey stores references to a few data (Datum objects) that comprise a
 * key tuple to in an index. It is a plain byte array and may be trivially
 * destructed. When the IndexKey is used, any Datum it references must remain
 * alive.
 *
 * IndexKey provides both a creation function Create() and an inplace
 * construction function Construct(), which are analogous to c++ new and in
 * place new expressions. See the function docs for details.
 *
 * One may also use IndexKey to construct a partial search key, where the
 * number of fields is smaller than the key schema does in an index. Any key
 * after the last key in this IndexKey is treated as NULL.
 *
 * Under the hood, IndexKey stores a 2-byte FieldId as a header, and a null
 * bitmap that immediately follows. Following that is some padding to 8-byte
 * address and an array of 8-byte DatumRef. The main benefit of using IndexKey
 * instead of std::vector<NullableDatumRef> is that it is more compact in space
 * and is likely to be more efficient to access.
 */
struct IndexKey {
public:
    /*!
     * Allocates and constructs a new IndexKey that references the \p data.
     *
     * The key has exactly enough space to hold `data.size()` Datum references.
     */
    template<class SomeDatum>
    static UniqueIndexKey
    Create(const std::vector<SomeDatum> &data) {
        if (data.size() > (size_t) std::numeric_limits<FieldId>::max()) {
            LOG(kFatal, "too many fields in a key: %lu", data.size());
        }
        FieldId nkeys = (FieldId) data.size();
        size_t sz = ComputeStructSize(nkeys);
        void *new_key = aligned_alloc(8, sz);
        ConstructImpl(new_key, data.data(), nkeys, sz);
        return UniqueIndexKey((IndexKey*) new_key);
    }

    template<class SomeDatum>
    static UniqueIndexKey
    Create(SomeDatum *data, FieldId nkeys) {
        size_t sz = ComputeStructSize(nkeys);
        void *new_key = aligned_alloc(8, sz);
        ConstructImpl(new_key, data, nkeys, sz);
        return UniqueIndexKey((IndexKey*) new_key);
    }

    /*!
     * Constructs a new IndexKey that references the \p data on a preallocated
     * \p key. The \p key pointer must be aligned to 8-byte address and it is
     * undefined if the size of \p key is smaller than
     * `IndexKey::GetStructSize(data.size())`. However, it is safe to construct
     * an IndexKey with fewer keys than allocated.
     *
     * This is useful for reusing the same space for different keys during
     * iteration in an index.
     */
    template<class SomeDatum>
    static void
    Construct(void *key, const std::vector<SomeDatum> &data) {
        if (data.size() > (size_t) std::numeric_limits<FieldId>::max()) {
            LOG(kFatal, "too many fields in a key: %lu", data.size());
        }
        FieldId nkeys = (FieldId) data.size();
        size_t sz = ComputeStructSize(nkeys);
        ConstructImpl(key, data.data(), nkeys, sz);
    }

    template<class SomeDatum>
    static void
    Construct(void *key, SomeDatum *data, FieldId nkeys) {
        size_t sz = ComputeStructSize(nkeys);
        ConstructImpl(key, data, nkeys, sz);
    }

    /*!
     * Makes a copy of the index key in a newly allocated buffer.
     *
     * The space is exactly enought for storing `GetNumKeys()` keys.
     */
    UniqueIndexKey
    Copy() const {
        size_t sz = ComputeStructSize(GetNumKeys());
        void *new_key = aligned_alloc(8, sz);
        ConstructImpl(new_key, &GetKey(0), GetNumKeys(), sz);
        return UniqueIndexKey((IndexKey*) new_key);
    }

    /*!
     * Makes a deep copy of all the variable-length Datum the key is
     * refernecing in a newly allocated memory space and append any deep copied
     * Datum into the \p data_buffer. Then it replaces the old DataRef with the
     * new ones that references the new Datum in \p data_buffer in this
     * IndexKey. The caller should ensure \p data_buffer is alive when the
     * returned IndexKey is accessed.
     *
     * It is undefined if `GetNumKeys() > sch->GetNumFields()`.
     */
    void
    DeepCopy(const Schema *sch, std::vector<Datum> &data_buffer) {
        FieldId nkeys = GetNumKeys();
        ASSERT(nkeys <= sch->GetNumFields());
        for (FieldId i = 0; i < nkeys; ++i) {
            if (!IsNull(i) && sch->FieldPassByRef(i)) {
                Datum &old_datum = GetKey(i).GetDatum();
                data_buffer.emplace_back(old_datum.DeepCopy());
                GetKey(i) = data_buffer.back();
            }
        }
    }

    /*!
     * Returns the number of keys in this IndexKey.
     */
    inline FieldId
    GetNumKeys() const {
        return *reinterpret_cast<const FieldId*>(this);
    }

    /*!
     * Returns the key \p keyid. It is undefined if `keyid >= GetNumKeys()`.
     */
    inline DatumRef&
    GetKey(FieldId keyid) const {
        return ((DatumRef*)(((uint8_t*) m_data)
            + MAXALIGN(2 + ((GetNumKeys() + 7) >> 3))))[keyid];
    }

    /*!
     * Returns the key \p keyid is null. Note that it is ok to call IsNull with
     * `keyid >= GetNumKeys()`. Those keys are treated as NULL.
     */
    inline bool
    IsNull(FieldId keyid) const {
        if (keyid >= GetNumKeys()) {
            return true;
       }
        return !!(((uint8_t*) &m_data)[2 + (keyid >> 3)] & (1 << (keyid & 7)));
    }

    /*!
     * Sets or clears the null bit for the key \keyid. It is undefined if
     * `keyid >= GetNumKeys()`.
     */
    inline void
    SetNullBit(FieldId keyid, bool isnull) const {
        if (isnull)
            ((uint8_t*) &m_data)[2 + (keyid >> 3)] |= (1 << (keyid & 7));
        else
            ((uint8_t*) &m_data)[2 + (keyid >> 3)] &=
                ~(uint8_t)(1 << (keyid & 7));
    }

    /*!
     * Returns if any of the keys is null.
     */
    inline bool
    HasAnyNull() const {
        uint8_t *bm = ((uint8_t *) &m_data) + 2;
        size_t n = GetNumKeys() >> 3;
        for (size_t i = 0; i < n; ++i) {
            // this is ok because we zero filled the null bitmap initially.
            if (bm[i] != 0)
                return true;
        }
        return false;
    }

    /*!
     * Returns the minimum size of the byte array to store this IndexKey.
     *
     * Same as `ComputeStructSize(this->GetNumKeys())`.
     */
    inline size_t
    GetStructSize() const {
        return ComputeStructSize(GetNumKeys());
    }

    /*!
     * Computes the minimum size of an byte array to store this IndexKey that
     * holds \p nkeys.
     */
    inline static size_t
    ComputeStructSize(FieldId nkeys) {
        size_t sz = MAXALIGN(2 + ((nkeys + 7) >> 3)) + nkeys * sizeof(DatumRef);
        return sz;
    }

    /*!
     * Returns the key \keyid as a NullableDatumRef.
     */
    inline NullableDatumRef
    GetNullableKey(FieldId keyid) const {
        if (IsNull(keyid)) {
            return NullableDatumRef(Datum::FromNull());
        }
        return NullableDatumRef(GetKey(keyid));
    }

    /*!
     * Returns the IndexKey as an std::vector<NullableDatumRef>.
     */
    std::vector<NullableDatumRef>
    ToNullableDatumVector() const {
        std::vector<NullableDatumRef> vec;
        vec.reserve(GetNumKeys());
        for (FieldId i = 0; i < GetNumKeys(); ++i) {
            vec.emplace_back(GetNullableKey(i));
        }
        return vec;
    }

    IndexKey(const IndexKey&) = delete;
    IndexKey(IndexKey&&) = delete;
    IndexKey &operator=(const IndexKey&) = delete;
    IndexKey &operator=(IndexKey&&) = delete;

private:
    template<class SomeDatum>
    static void
    ConstructImpl(void *key, SomeDatum *data,
                  FieldId nkeys, size_t sz) {
        *(FieldId *)key = nkeys;
        uint8_t *bm = ((uint8_t *) key ) + 2;
        DatumRef *key_data =
            (DatumRef*)(((uint8_t *) key) + sz - nkeys * sizeof(DatumRef));
        memset(bm, 0, ((nkeys + 7) >> 3));
        for (FieldId i = 0; i < nkeys; ++i) {
            if (data[i].isnull()) {
                bm[i >> 3] |= 1 << (i & 7);
            } else {
                key_data[i] = DatumRef(data[i]);
            }
        }
    }

    // forces 8-byte alignment
    uint64_t m_data[1];
};

}   // namespace taco

#endif      // INDEX_INDEXKEY_H

