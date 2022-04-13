#ifndef EXTSORT_ITEMITERATOR_H
#define EXTSORT_ITEMITERATOR_H

#include "tdb.h"

namespace taco {

/*!
 * A pure abstract iterator over items (i.e., plain byte arrays).
 *
 * An implementation may optionally supports rewinding (e.g., the output
 * iterator of ExtSort). One may check if the underlying support allows
 * rewinding by calling the virtual function SupportsRewind().
 *
 * The rewind interface uses a uint64_t position (rather than RecordId). See
 * below for explanation.
 */
class ItemIterator {
public:
    /*!
     * Destructor.
     */
    virtual ~ItemIterator() {}

    /*!
     * Subclass implementation should override the Next() function for moving
     * to the next item . Upon a successful return,
     * `GetCurrentItem()` should return a pointer to a byte array which
     * contains the item (which may be a heap record, an index
     * record or something else). The interpretation of the record is up to the
     * caller.
     */
    virtual bool Next() = 0;

    /*!
     * Returns the current serialized item that a previous `Next()` call moved
     * this iterator to. \p p_reclen is set to the length of the item upon
     * return.
     */
    virtual const char* GetCurrentItem(FieldOffset& p_reclen) = 0;

    /*!
     * Whether the subclass implementation supports rewinding.
     */
    virtual bool SupportsRewind() const = 0;

    /*!
     * Saves the current posision if the subclass implementation supports
     * rewinding. The position is expressed as an opaque uint64_t rather than
     * RecordId (which may fit into a uint64_t), so that the implementation can
     * actually store other kinds of position pointer/index.
     *
     * It is undefined if called on a ItemIterator such that
     * `this->SupportsRewind() == false`.
     */
    virtual uint64_t SavePosition() const = 0;

    /*!
     * Rewinds the iterator back to a previously saved posision if the subclass
     * implementation supports rewinding. The argument \p pos must be a value
     * previously returned by a call to SavePosition() on this iterator,
     * otherwise it is undefined.
     *
     * It is undefined if called on a ItemIterator such that
     * `this->SupportsRewind() == false`.
     */
    virtual void Rewind(uint64_t pos) = 0;

    /*!
     * Ends the scan.
     */
    virtual void EndScan() = 0;
};

}   // namespace taco

#endif      // EXTSORT_ITEMITERATOR_H
