/*!
 * @file
 *
 * This file contains the definition of the Datum and DatumRef structures for
 * storing and passing C++ objects of certain C++ types that are used as
 * runtime representation of the objects of SQL types in the database. A Datum
 * stores and manages the memory resource of a **read-only** C++ object and a
 * DatumRef is a reference to a read-only object. That means one may not make
 * in-place updates to an object managed or referenced by these classes, rather
 * has to create a new one for an updated value. It is meant to explicitly
 * manage the ownership of objects on heap allocated memory and is designed to
 * support the RAII idiom.
 *
 * The design of DatumRef is inspired by the Datum in PostgreSQL but we take a
 * different approach towards memory management. Here, a heap allocated object
 * managed in Datum is usually tied to an object that usually reclaims the
 * memory when it reaches the end of its life cycle. In PostgreSQL, a heap
 * allocated datum are usually reclaimed when a memory pool reaches the end of
 * its life cycle, so that objects are almost never explicitly deallocated.
 *
 * Part of the code is based on postgres.h in PostgreSQL. See COPYRIGHT
 * for a copyright notice for code copied or derived from PostgreSQL.
 *
 * This file does not have a header guard intentionally. This must be included
 * indirectly from base/tdb_base.h.
 */

namespace taco {

/*!
 * The private definitions of datum classes. This has to be in a named
 * namespace as it is included into every source file through base/tdb_base.h.
 */
namespace datum_impl {

typedef uintptr_t DatumRep;
static_assert(sizeof(DatumRep) == 8, "Pointers are not 64-bit.");

/*! Note from PostgreSQL: Float <-> Datum conversions.  These have to be
 * implemented as inline functions rather than macros, when passing by
 * value, because many machines pass int and float function
 * parameters/results differently; so we need to play weird games with
 * unions.
 */
union DatumFloatConversion {
    int32_t     int32_v;
    float       float_v;
};

/*! Note from PostgreSQL: Float <-> Datum conversions.  These have to be
 * implemented as inline functions rather than macros, when passing by
 * value, because many machines pass int and float function
 * parameters/results differently; so we need to play weird games with
 * unions.
 */
union DatumDoubleConversion {
    int64_t     int64_v;
    double      double_v;
};

template<class DatumImpl>
class NonVarlenGetters {
public:
    /*!
     * Return the pointer to the binary representation of a fixed-length datum
     * that is passed by value.
     */
    constexpr const char*
    GetFixedlenBytes() const {
        return (const char*) &val();
    }

    /*!
     * Returns boolean value of a datum.
     */
    constexpr bool
    GetBool() const {
        return ((bool) ((val()) != 0));
    }

    /*!
     * Returns character value of a datum.
     */
    constexpr char
    GetChar() const {
        return ((char) (val()));
    }

    /*!
     * Returns 8-bit integer value of a datum.
     */
    constexpr int8_t
    GetInt8() const {
        return ((int8_t) (val()));
    }

    /*!
     * Returns 8-bit unsigned integer value of a datum.
     */
    constexpr uint8_t
    GetUInt8() const {
        return ((uint8_t) (val()));
    }

    /*!
     * Returns 16-bit integer value of a datum.
     */
    constexpr int16_t
    GetInt16() const {
        return ((int16_t) (val()));
    }

    /*!
     * Returns 16-bit unsigned integer value of a datum.
     */
    constexpr uint16_t
    GetUInt16() const {
        return ((uint16_t) (val()));
    }

    /*!
     * Returns 32-bit integer value of a datum.
     */
    constexpr int32_t
    GetInt32() const {
        return ((int32_t) (val()));
    }

    /*!
     * Returns 32-bit unsigned integer value of a datum.
     */
    constexpr uint32_t
    GetUInt32() const {
        return ((uint32_t) (val()));
    }

    /*!
     * Returns object identifier value of a datum.
     */
    constexpr Oid
    GetOid() const {
        return ((Oid) (val()));
    }

    /*!
     * Returns 64-bit integer value of a datum.
     */
    constexpr int64_t
    GetInt64() const {
        return (int64_t) val();
    }

    /*!
     * Returns 64-bit unsigned integer value of a datum.
     */
    constexpr uint64_t
    GetUInt64() const {
        return (uint64_t) val();
    }

    /*!
     * Returns single-precision floating point value of a datum.
     */
    constexpr float
    GetFloat() const {
        return datum_impl::DatumFloatConversion{.int32_v=GetInt32()}.float_v;
    }

    /*!
     * Returns double-precision floating point value of a datum.
     */
    constexpr double
    GetDouble() const {
        return datum_impl::DatumDoubleConversion{.int64_v=GetInt64()}.double_v;
    }

    /*!
     * Returns pointer value of a datum.
     */
    constexpr void*
    GetPointer() const {
        return ((void*) (val()));
    }

    /*!
     * Returns the pointer value of a datum as const T*.
     */
    template<class T>
    constexpr T*
    GetPointerAs() const {
        return reinterpret_cast<T* const>(val());
    }

private:
    constexpr const DatumRep&
    val() const {
        return static_cast<const DatumImpl&>(*this).m_val;
    }
};


}   // namespace datum_impl

/*!
 * A Datum stores and possibly manage the memory resource of a **read-only**
 * value of a plain fixed-length C++ type, a null-terminated string (cstring),
 * or an object of a variable-length type. Datum is usually used for passing
 * the runtime C++ representation of a value of a SQL type, but sometimes it
 * can also be used to pass internal values of primitive types (see the caution
 * below).  It can be created using the From() static member functions. Datum
 * may not be copied and may only be moved, as it represents a unique ownership
 * of an object. The copy constructor and assignment operator are explicitly
 * deleted because of that. A Datum does not store the type information. It is
 * up to the caller to find out the correct GetXXX() function to call depending
 * on the types.
 *
 * To pass Datum around in the system, use the implicit conversions to DatumRef
 * or NullableDatumRef, which can be freely copied. However, a DatumRef or
 * NullableDatumRef must **NOT** be used once its referencing Datum is
 * desctructed.
 *
 * Caution: the pointer to an object of a type with a non-trivial destructor
 * must **NOT** be owned by a Datum, because Datum does not know its type and
 * will never call the destructor. The workaround is to manage the value using
 * std::unique_ptr and directly get Datum or DatumRef from the plain pointer.
 *
 * Implementation notes:
 *
 * If a value is a fixed-length type is <= 8 bytes long, it is stored as a copy
 * of the value. In this case, there are no dynamically allocated memory and
 * m_isown is never true. Otherwise, m_isown is true if Datum is created from
 * an std::unique_ptr (in which case the ownership of the pointer in the smart
 * pointer is transfered to the Datum), or false if Datum is created from a
 * plain pointer (in which case the caller retains the ownership of the data).
 * The latter case is useful when we don't want to make a copy of an object
 * located on a buffer page. To avoid break the assumption that Datum is
 * **read-only**, the caller is responsible for the objected pointed by a plain
 * pointer is not modified (e.g., the caller should have a lock on a tuple on a
 * buffer page).
 *
 * Note that the ownership of the memory resource is **irrelevant** to the
 * ownership of the Datum. One should always ensure a Datum is live when it is
 * referenced through a DatumRef or a NullableDatumRef, even if the value is
 * copied into the reference when it fits into the m_val field in the current
 * implementation.
 *
 */
class Datum: public datum_impl::NonVarlenGetters<Datum> {
public:
    ~Datum() {
        if (m_isowned) {
            free(GetPointer());
        }
    }

    /*!
     * Move constructor of Datum. This Datum assumes the ownership of the object
     * in ``d'', and ``d'' will be set to null upon return.
     */
    Datum(Datum &&d) {
        memcpy(this, &d, sizeof(Datum));

        // GCC >=8 complains about clearing an object with no trivial
        // copy-assignment. It should be Ok here, since we are transferring
        // the ownership from d to this.
#if __GNUC__ >= 8
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
        memset(&d, 0, sizeof(Datum));
#if __GNUC__ >= 8
#pragma GCC diagnostic pop
#endif
        d.m_isnull = true;
    }

    /*!
     * Move assignment operator of Datum. This Datum assumes the ownership of
     * the object in ``d'', and ``d'' will be set to null upon return.
     */
    Datum &operator=(Datum &&d) {
        memcpy(this, &d,  sizeof(Datum));

        // same as above in Datum(Datum&&)
#if __GNUC__ >= 8
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
        memset(&d, 0, sizeof(Datum));
#if __GNUC__ >= 8
#pragma GCC diagnostic pop
#endif
        d.m_isnull = true;
        return *this;
    }

    constexpr bool
    isnull() const {
        return m_isnull;
    }

    /*!
     * Returns the variable-length value of a datum as an array of bytes.
     */
    constexpr const char*
    GetVarlenBytes() const {
        return (const char *) m_val;
    }

    /*!
     * Returns the size of the variable-length value of a datum.
     */
    constexpr uint32_t
    GetVarlenSize() const {
        return m_size;
    }

    /*
     * Returns the variable-length value of a datum as an absl::string_view.
     *
     * This is equivalent to absl::string_view(GetVarlenBytes(),
     * GetVarlenSize()).
     */
    constexpr absl::string_view
    GetVarlenAsStringView() const {
        return absl::string_view((const char *) m_val, m_size);
    }

    /*!
     * Returns a constant reference to `*this'.
     */
    constexpr const Datum&
    GetThis() const {
        return *this;
    }

    /*
     * Returns datum representation of a null value.
     */
    static Datum
    FromNull() {
        return Datum();
    }

    /*!
     * Returns datum representation for a boolean.
     */
    static Datum
    From(bool X) {
        return Datum((datum_impl::DatumRep) ((X) ? 1 : 0));
    }

    /*!
     * Returns datum representation for a character.
     */
    static Datum
    From(char X) {
        return Datum((datum_impl::DatumRep) (X));
    }

    /*!
     * Returns datum representation for an 8-bit integer.
     */
    static Datum
    From(int8_t X) {
        return Datum((datum_impl::DatumRep) (X));
    }

    /*!
     * Returns datum representation for an 8-bit unsigned integer.
     */
    static Datum
    From(uint8_t X) {
        return Datum((datum_impl::DatumRep) (X));
    }

    /*!
     * Returns datum representation for a 16-bit integer.
     */
    static Datum
    From(int16_t X) {
        return Datum((datum_impl::DatumRep) (X));
    }

    /*!
     * Returns datum representation for a 16-bit unsigned integer.
     */
    static Datum
    From(uint16_t X) {
        return Datum((datum_impl::DatumRep) (X));
    }

    /*!
     * Returns datum representation for a 32-bit integer.
     */
    static Datum
    From(int32_t X) {
        return Datum((datum_impl::DatumRep) (X));
    }

    /*!
     * Returns datum representation for a 32-bit unsigned integer.
     *
     * Note: there is no From(Oid) because an Oid is represented as a uint32_t
     * in C++.
     */
    static Datum
    From(uint32_t X) {
        return Datum((datum_impl::DatumRep) (X));
    }

    /*!
     * Returns datum representation for a pointer.
     */
    static Datum
    From(void *X) {
        return Datum((datum_impl::DatumRep) (X));
    }

    /*!
     * Returns datum representation for a signed 64-bit integer.
     *
     */
    static Datum
    From(int64_t X) {
        return Datum((datum_impl::DatumRep) X);
    }

    /*!
     * Returns datum representation for an unsigned 64-bit integer.
     *
     */
    static Datum
    From(uint64_t X) {
        return Datum((datum_impl::DatumRep) X);
    }


    /*!
     *  Returns datum representation for a single-precision floating point
     *  number.
     */
    static Datum
    From(float X) {
        return datum_impl::DatumFloatConversion{.float_v=X}.int32_v;
    }

    /*!
     *  Returns datum representation for a double-precision floating point
     *  number.
     */
    static Datum
    From(double X) {
        return datum_impl::DatumDoubleConversion{.double_v=X}.int64_v;
    }

    /*!
     * Returns datum representation of a null-terminated string that is not
     * owned by this datum. A null-terminated string is always treated as
     * a variable-length datum so you have to call GetVarlenAsStringView() to
     * convert it back to a string (though there is no guarantee that the
     * returned string_view has a pointer to a null-terminated string).
     */
    static Datum
    FromCString(const char *str) {
        size_t len = std::strlen(str);
        if (len > std::numeric_limits<uint32_t>::max()) {
            LOG(kFatal, "cstring is too long: %lu", len);
        }
        return FromVarlenBytes(str, len);
    }

    /*
     * Returns datum representation of a null-terminated string that is not
     * owned by this datum if isnull is false, or otherwise, returns a datum of
     * a null value. A null-terminated string is always treated as
     * a variable-length datum so you have to call GetVarlenAsStringView() to
     * convert it back to a string (though there is no guarantee that the
     * returned string_view has a pointer to a null-terminated string).
     */
    static Datum
    FromCString(const char *str, bool isnull) {
        return isnull ? Datum() : FromCString(str);
    }

    /*!
     * Returns datum representation of a null-terminated string that is owned
     * by this datum. Upon return the pointer stored in ``str'' is released and
     * owned by the Datum returned. A null-terminated string is always treated as
     * a variable-length datum so you have to call GetVarlenAsStringView() to
     * convert it back to a string (though there is no guarantee that the
     * returned string_view has a pointer to a null-terminated string).
     *
     * The optional second argument isnull is ignored if ``str'' stores a
     * nullptr. Otherwise, isnull takes precedence in deciding whether the
     * Datum is null or not. Note that a non-null pointer in ``bytes'' will be
     * deallocated if isnull is true.
     */
    static Datum
    FromCString(std::unique_ptr<char[]> str, bool isnull = false) {
        if (isnull || !str.get())
            return Datum();
        size_t len = std::strlen(str.get());
        if (len > std::numeric_limits<uint32_t>::max()) {
            LOG(kFatal, "cstring is too long: %lu", len);
        }
        return Datum((datum_impl::DatumRep) str.release(), true, len);
    }

    /*!
     * Returns datum representation of a variable-length object that is not
     * owned by this datum.
     */
    static Datum
    FromVarlenBytes(const char *bytes, uint32_t size) {
        return Datum((datum_impl::DatumRep) bytes, false, size);
    }

    /*!
     * Returns datum representation of a variable-length object that is not
     * owned by this datum if isnull is false, or otherwise, returns a datum of
     * a null value.
     */
    static Datum
    FromVarlenBytes(const char *bytes, uint32_t size, bool isnull) {
        return isnull ? Datum() : FromVarlenBytes(bytes, size);
    }

    /*!
     * Returns datum representation of a variable-length object that is owned
     * by this datum. Upon return the pointer stored in ``bytes'' is released
     * and owned by the Datum returned.
     *
     * The optional second argument isnull is ignored if ``bytes'' stores a
     * nullptr. Otherwise, isnull takes precedence in deciding whether the
     * Datum is null or not. Note that a non-null pointer in ``bytes'' will be
     * deallocated if isnull is true.
     *
     * The first argument ``bytes'' must point to some byte buffer allocated
     * via malloc()/aligned_alloc() (or unique_aligned_alloc()/unique_malloc())
     * rather than C++ new expression. That means it must **NOT** point to some
     * object that requires non-trivial destruction (see Caution in the class
     * documentation), i.e., one should never try to bypass the restriction by
     * casting a pointer T* where T is some class with non-trivial destructor
     * into char* and create a unique_ptr of that as the first argument!!
     */
    static Datum
    FromVarlenBytes(unique_malloced_ptr bytes, uint32_t size,
                    bool isnull = false) {
        if (!bytes.get() || isnull) {
            return Datum();
        }
        return Datum((datum_impl::DatumRep) bytes.release(), true, size);
    }

    /*!
     * Returns datum representation of a variable-length object stored as an
     * absl::string_view that is not owned by this datum.
     */
    static Datum
    FromVarlenAsStringView(absl::string_view bytes) {
        if (!bytes.data())
            return Datum();
        return FromVarlenBytes(bytes.data(), bytes.size());
    }

    /*!
     * This template version of From accepts a second argument isnull and
     * returns a Datum with a null value if that's true. Otherwise, it just
     * forwards the argument to the corresponding single-argument From()
     * function.
     */
    template<class T>
    static Datum
    From(T&& X, bool isnull) {
        return isnull ? Datum() : From(std::forward<T>(X));
    }

    /*!
     * Returns a fixed-length datum that is passed by value by copying its
     * binary representation.
     */
    static Datum
    FromFixedlenBytes(const char *bytes, uint32_t size) {
        //ASSERT(TYPEALIGN(size, (uintptr_t) bytes) == (uintptr_t) bytes);

        switch (size) {
        case 1:
            return Datum((datum_impl::DatumRep)*(uint8_t*)bytes);
        case 2:
            return Datum((datum_impl::DatumRep)*(uint16_t*)bytes);
        case 4:
            return Datum((datum_impl::DatumRep)*(uint32_t*)bytes);
        case 8:
            return Datum((datum_impl::DatumRep)*(uint64_t*)bytes);
        }

        LOG(kError, "unexpected pass-by-value data length %u", size);
        return Datum();
    }

    /*!
     * Whether this datum is variable-length and owns its byte array.  A Datum
     * with `HasExternalRef() == true` may only be read when the byte array it
     * references is still alive (e.g., it must be pinned if it is referencing
     * a buffer frame). Otherwise, a Datum is safe to be read at any time.
     */
    bool
    HasExternalRef() const {
        return m_isvarlen && m_isowned;
    }

    /*!
     * Returns a deep copy of this datum so that `HasExternalRef() == false`.
     *
     * It may just return itself.
     */
    Datum
    DeepCopy() const {
        if (HasExternalRef()) {
            const char *bytes = GetVarlenBytes();
            size_t size = GetVarlenSize();
            // We don't know what exactly the alignment requirement is, so
            // make it the maximum a type might need.
            unique_malloced_ptr bytes_copy = unique_aligned_alloc(8, size);
            memcpy(bytes_copy.get(), bytes, size);
            return Datum::FromVarlenBytes(std::move(bytes_copy), size);
        }
        return *this;
    }

private:
    /*!
     * Constructs a Datum with a null value.
     */
    Datum():
        m_isowned(false),
        m_isnull(true),
        m_isvarlen(false),
        m_size(0),
        m_val(0) {}

    Datum(datum_impl::DatumRep value):
        m_isowned(false),
        m_isnull(false),
        m_isvarlen(false),
        m_size(0),
        m_val(value) {}

    /*!
     * Constructs a Datum with a non-null and non-variable-length value (except
     * for CString).
     */
    /*Datum(datum_impl::DatumRep value, bool isowned):
        m_isowned(isowned),
        m_isnull(false),
        m_isvarlen(false),
        m_size(0),
        m_val(value) {} */

    Datum(datum_impl::DatumRep value,
          bool isowned,
          uint32_t size):
        m_isowned(isowned),
        m_isnull(false),
        m_isvarlen(true),
        m_size(size),
        m_val(value) {}

    // The default constructor and copy assignment should not be used outside
    // the Datum class.
    Datum(const Datum&) = default;
    Datum &operator=(const Datum&) = default;

    /*!
     * Whether the managed object is owned by this Datum. Currently we assume
     * any owned object is passed by reference and its memory is allocated on
     * the heap via new().
     */
    bool        m_isowned : 1;

    /*!
     * Whether the value is null.
     */
    bool        m_isnull : 1;

    /*!
     * Whether the managed object is a variable-length object that is not a
     * null-terminated string.
     */
    bool        m_isvarlen : 1;

    /*!
     * The length of the variable-length value of the datum. This is only valid
     * when m_isvarlen == true.
     */
    uint32_t    m_size;

    /*!
     * The actual value storage.
     */
    datum_impl::DatumRep    m_val;

    friend class datum_impl::NonVarlenGetters<Datum>;
    friend class DatumRef;
    friend class NullableDatumRef;
};

namespace datum_impl {

template<class DatumImpl>
class DatumRefVarlenGetters: public NonVarlenGetters<DatumImpl> {
public:

    /*!
     * Returns the variable-length value of a datum as an array of bytes.
     */
    constexpr const char*
    GetVarlenBytes() const {
        return NonVarlenGetters<DatumImpl>::template GetPointerAs<Datum>()
            ->GetVarlenBytes();
    }

    /*!
     * Returns the length of the variable-length value of a datum.
     */
    constexpr uint32_t
    GetVarlenSize() const {
        return NonVarlenGetters<DatumImpl>::template GetPointerAs<Datum>()
            ->GetVarlenSize();
    }

    /*
     * Returns the variable-length value of a datum as an absl::string_view.
     *
     * This is equivalent to
     * absl::string_view(GetVarlenBytes(), GetVarlenSize()).
     *
     * This is also used for returning a string value but the string is not
     * guaranteed to be null-terminated -- see FromCString() for additional
     * notes.
     */
    constexpr absl::string_view
    GetVarlenAsStringView() const {
        return NonVarlenGetters<DatumImpl>::template GetPointerAs<Datum>()
            ->GetVarlenAsStringView();
    }

    /*!
     * Returns a reference to the underlying datum if this references one
     * with a variable-length value.
     *
     * Not valid if it stores a fixed-length value, in which case the value
     * instead of the Datum pointer is stored in the DatumRef/NullableDatumRef.
     */
    constexpr Datum&
    GetDatum() const {
        return *NonVarlenGetters<DatumImpl>::template GetPointerAs<Datum>();
    }
};

}   // namespace datum_impl

/*!
 * A DatumRef object is a **read-only** reference to an C++ object of a
 * supported runtime type. There is an implicit conversion from a Datum to
 * DatumRef so that one may use a Datum variable where it needs a DatumRef.
 * The object referenced by a DatumRef is always assumed to be non-null.
 *
 * DatumRef makes a copy of the actual value stored in its referencing Datum
 * unless the value is variable length with the length field (i.e., the pointer
 * to a null-terminated string still get copied into a DatumRef).  In the case
 * that the value is a variable length field, DatumRef stores a pointer to the
 * referencing Datum, which requires an additional indirection when read. Note
 * that the nullness of the referencing Datum is always ignored, even if we
 * store a pointer here.
 */
class DatumRef: public datum_impl::DatumRefVarlenGetters<DatumRef> {
public:
    DatumRef(const Datum &d):
        m_val(d.m_isvarlen? ((datum_impl::DatumRep) &d) : d.m_val) {}
    // DatumRef must not have a non-trivial destructor.

    DatumRef(const DatumRef &) = default;
    DatumRef &operator=(const DatumRef&) = default;

    constexpr bool
    isnull() const {
        return false;
    }

protected:
    DatumRef(datum_impl::DatumRep val):
        m_val(val) {}

    datum_impl::DatumRep    m_val;

    friend class datum_impl::NonVarlenGetters<DatumRef>;
    friend class NullableDatumRef;
};

/* Make sure that DatumRef may be passed in a register whenever possible. */
static_assert(sizeof(DatumRef) == sizeof(datum_impl::DatumRep));

/*
 * NullableDatumRef additionally stores whether an object is null or not.
 * In some cases, we want to pass the nullness of an object as well, e.g., as
 * arguments to a fmgr-managed function. NullableDatumRef allows for that.
 *
 * Different from DatumRef, the only reason that NullableDatumRef does not just
 * wrap a pointer to a Datum is that we want to have implicit conversion from a
 * DatamRef to a NullableDatumRef, and DatumRef may not be a pointer to a
 * Datum in all cases.
 */
class NullableDatumRef:
    public datum_impl::DatumRefVarlenGetters<NullableDatumRef>  {
public:
    NullableDatumRef(const Datum &d):
        m_isnull(d.m_isnull),
        m_val(d.m_isvarlen? ((datum_impl::DatumRep) &d) : d.m_val) {}

    NullableDatumRef(const DatumRef & d):
        m_isnull(false),
        m_val(d.m_val) {}

    NullableDatumRef(const NullableDatumRef&) = default;
    NullableDatumRef& operator=(const NullableDatumRef&);

    operator DatumRef() const {
        if (m_isnull)
            LOG(kFatal, "casting NULL datum to a DatumRef");
        return DatumRef(m_val);
    }

    constexpr bool
    isnull() const {
        return m_isnull;
    }

private:
    bool                    m_isnull;
    datum_impl::DatumRep    m_val;

    friend class datum_impl::NonVarlenGetters<NullableDatumRef>;
};

}   // namespace taco
