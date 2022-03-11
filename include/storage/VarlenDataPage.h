#ifndef STORAGE_VARLENDATAPAGE_H
#define STORAGE_VARLENDATAPAGE_H

#include "tdb.h"

#include "storage/Record.h"

namespace taco {

struct SlotData;

static_assert(MinSlotId == 1);

/*!
 * VarlenDataPage implements a buffered heap page that supports inserting,
 * deleting, updating and random accessing variable-length records on the page.
 * Note that this class does not need to parse or validate the record payload.
 * Rather it may treat each record as an array of bytes of variable length.
 *
 * Note:
 *  1. A variable-length data page must have `PageHeaderData` (see
 *  `storage/FileManager.h`) at the beginning and it may not modify anything in
 *   the bytes occupied by the `PageHeaderData`.
 *  2. Records must be stored at offsets that are aligned to the maximum
 *  aligment requirement (using `MAXALIGN()` in `base/tdb_base.h`). You may
 *  assume any buffer passed to VarlenDataPage is properly aligned (at least
 *  maxaligned addresses).
 *  3. Any valid slot ID on this page must be in the range of
 *  [`GetMinSlotId()`, `GetMaxSlotId()`] (the valid range of slot id). You may
 *  use the function `CheckSID()` to validate whether an \p sid falls into this
 *  range. Unless the page is empty, `GetMaxSlotId()` should always be an
 *  occupied slot with the largest slot id on this page (see GetMaxSlotId() for
 *  details).
 *  4. VarlenDataPage should not store any page content locally in the class,
 *  so that two VarlenDataPage constructed on the same page buffer should
 *  be able to see the effect of each other as long as there's no data race.
 *  5. VarlenDataPage should have a trivial destructor and thus should not have
 *  a user-defined destructor.
 */
class VarlenDataPage {
public:
    /*!
     * Initializes a new (virtual) File page buffered in \p pagebuf as an empty
     * variable-length data page. You may assume the \p pagebuf is zero-filled
     * after the `PageHeaderData`.
     *
     * The caller may optionally reserve a fixed-size user data area of size \p
     * usr_data_sz for other use. The user data must be aligned to a MAXALIGN'd
     * offset within the page, but \p usr_data_sz is not necessarily
     * maxaligned. It's up to the implementation to decide where to store the
     * user data area.
     *
     * It is an error if the page cannot fit at least a record of length
     * `MAXALIGN_OF` (the minimum size of a record).
     * It is undefined if \p pagebuf is nullptr and/or \p usr_data_sz < 0.
     */
    static void InitializePage(char *pagebuf, FieldOffset usr_data_sz = 0);

    /*!
     * Constructs a VarlenDataPage object on a buffered page in \p pagebuf
     * that was previously initialized by `VarlenDataPage::InitializePage()`.
     *
     * It is undefined if \p buf is nullptr. It may never fail and may not
     * throw any error.
     */
    VarlenDataPage(char *pagebuf);

    /*!
     * Returns the pointer to the user data area.
     *
     * This should never fail but it is undefined if the page was initialized
     * with `usr_data_sz == 0`.
     */
    char* GetUserData() const;

    /*!
     * Returns the smallest valid slot ID. This is fixed to MinSlotId.
     */
    constexpr SlotId
    GetMinSlotId() const {
        return MinSlotId;
    }

    /*!
     * Returns the largest occupied slot ID `sid` currently on this page if
     * there's any. Otherwise, returns the slot ID smaller than
     * `GetMinSlotId()` (which is `INVALID_SID`).
     *
     *
     * Note: this is not MaxSlotId, which is the maximum slot ID one could use.
     * The returned slot ID `sid` should satisfy 1) `sid < GetMinSlotId() ||
     * IsOccupied(sid)`; and 2) for any `sid' > sid`, `!IsOccupied(sid')`.
     */
    SlotId GetMaxSlotId() const;

    /*!
     * Returns the slot ID of the first occupied slot on this page. If there's
     * no occupied slot, returns `INVALID_SID`.
     *
     * This is a convenient function to locate the first occupied slot, and you
     * don't have to change the implementation here.
     */
    SlotId
    GetMinOccupiedSlotId() const {
        for (SlotId sid = GetMinSlotId(); sid <= GetMaxSlotId(); ++sid) {
            if (IsOccupied(sid)) {
                return sid;
            }
        }
        return INVALID_SID;
    }

    /*!
     * Returns the slot of the last occupied slot on this page. If there's
     * no occupied slot, returns `INVALID_SID`.
     *
     * This is a convenient function to locate the last occupied slot. It may
     * be the same as GetMaxSlotId() if the latter also returns `INVALID_SID`
     * if the page is empty.
     */
    SlotId
    GetMaxOccupiedSlotId() const {
        return GetMaxSlotId();
    }

    /*!
     * Returns whether the slot \p sid is currently occupied.
     *
     * It throws an error if the slot falls out of the valid range.
     */
    bool IsOccupied(SlotId sid) const;

    /*!
     * Returns the number of the occupied slots on this page.
     */
    SlotId GetRecordCount() const;

    /*!
     * Returns the record buffer of the slot \p sid and sets `*p_len` to the
     * length of the record if \p p_len is not nullptr. It is allowed to pass a
     * nullptr as \p p_len, in which case, \p p_len should not be dereferenced.
     *
     * If a previous insertion or update stores a record with a length that is
     * not aligned to multiples of 8 bytes, it is up to the implementation to
     * return the original length or the MAXALIGN'd length, and in the latter
     * case, any bytes in the returned buffer after the original length is
     * undefined.
     *
     * If \p sid falls out of the valid range, it throws an error and `*p_len`
     * should not be modified even if it is not nullptr. It is undefined if \p
     * sid falls into the valid range but is not currently occupied.
     */
    char *GetRecordBuffer(SlotId sid, FieldOffset *p_len) const;

    /*!
     * Returns the record in the slot \p sid as a Record object, with its
     * `GetRecordId().sid` set to \p sid and `GetRecordId().pid` not modified.
     */
    Record
    GetRecord(SlotId sid) const {
        Record rec;
        rec.GetData() = GetRecordBuffer(sid, &rec.GetLength());
        rec.GetRecordId().sid = sid;
        return rec;
    }

    /*!
     * Inserts the record into the page. Returns \p true if successful, and it
     * also updates rec.GetRecordId().sid to the slot ID of the newly inserted
     * record. If the record does not fit in the page even after compaction,
     * returns `true` but set `rec.GetRecordId().sid` to `INVALID_SID`.
     * Otherwise, it returns \p false without modifying the page.
     *
     * Note: it should not modify `rec.GetRecordId().pid` in any cases. The
     * record length may not be aligned to 8 bytes, but the implementation is
     * responsible for store all the records at an 8-byte offset on the page.
     */
    bool InsertRecord(Record &rec);

    /*!
     * Erases a record from the page specified by the slot ID. Returns \p true
     * if a record is found and erased at the specified slot ID. Otherwise, it
     * returns \p false without modifying the page.
     *
     * It throws an error if \p sid is not in the valid range.
     */
    bool EraseRecord(SlotId sid);

    /*!
     * Updates the record at the specified slot in place to a new one \p rec on
     * this page if there's enough space. Returns `true` if the update is
     * successful and `rec.GetRecordId().sid` is set to `sid`. If the record
     * does not fit the current page with the same slot but may fit in an empty
     * page, it returns `true` with the record at `sid` erased and
     * `rec.GetRecordId().sid` is set to `INVALID_SID`. If the record \p rec is
     * too long to fit even on an empty page, it returns `false` and the page
     * must not be modified.
     *
     * Note: if the new record has a length that is smaller than or equal to
     * the length of the old record, it must perform the update in place at the
     * same offset on the page. Otherwise, the implementation is allowed to
     * place the new record at any offset on the page, possibly rearranging the
     * page to make room. However, it must not move the record to a different
     * slot if it returns `true`.
     *
     * It throws an error if \p sid is not in the valid range or is not
     * currently occupied. In the case of any error, the page must not be
     * modified.
     */
    bool UpdateRecord(SlotId sid, Record &rec);

    /*!
     * Inserts the record at a given \p sid in the range of `[GetMinSlotId(),
     * GetMaxSlotId() + 1]`. If the \p sid is currently occupied, all the valid
     * records in the range of `[sid, GetMaxOccupiedSlotId()]` are moved to the
     * slot whose Id is one higher, provided that there's still enough space.
     * Returns `true` if successful, and upon return `rec.GetRecordId().sid` is
     * set to \p sid. If the record does not fit in the page even after
     * compaction, returns `true` but set `rec.GetRecordId().sid` to
     * `INVALID_SID`.  Otherwise, returns `false` without making any
     * modification to the page.
     *
     * It is an error if \p sid is not in the range of `[GetMinSlotId(),
     * GetMaxSlotId() + 1]`.
     *
     * This is useful for maintaining a sorted array of records in B-tree.
     */
    bool InsertRecordAt(SlotId sid, Record &rec);

    /*!
     * Erases a record at the given \p sid in the range of `[GetMinSlotId(),
     * GetMaxSlotId()]` if it exists and moves all the records in the range of
     * `[sid + 1, GetMaxOccupiedSlotId()]` to the slot whose id is one lower
     * (even if \p sid is not currently occupied).
     *
     * It is an error if \p sid is not in the range of `[GetMinSlotId(),
     * GetMaxSlotId()]`.
     *
     * This is useful for maintaining a sorted array of records in B-tree.
     */
    void RemoveSlot(SlotId sid);

    /*!
     * Shifts all the (occupied or unoccupied) slots to the right by n slots to
     * truncate existing slots or to the left by n slots to reserve new slots.
     *
     * If \p truncate is true, the first \p n slots are removed and the
     * remaining slots are moved to occupy consecutive slots from slot ID
     * `GetMinSlotId()`. It is **not** an error if \p n is greater than the
     * number of slots on the page, in which case, all the slots are removed
     * (and the page becomes an empty page).
     *
     * If \p truncate is false, all the existing slots are moved to occupy
     * consecutive slots from slot ID `n + 1`. The first \p n slots should then
     * be reserved as unoccupied slots. ShiftSlots is allowed to compact the
     * page to make room for the new slots when \p truncate is false. It is
     * a fatal error if it cannot make room for new slots (i.e., the caller is
     * responsible for ensuring there's enough room).
     */
    void ShiftSlots(SlotId n, bool truncate);

    /*!
     * Computes the size of the free space on the page if there are \p num_recs
     * records inserted to an empty VarlenDataPage with a user data area of
     * size \p usr_data_sz and the total length of the records is \p
     * total_reclen. For the purpose of computing the free space, the space
     * between the last (leftmost) slot's offset and the largest maximum
     * aligned offset that is no lager than the last slot's offset should be
     * considered as free space.
     *
     * Note that the argument \p total_reclen, the total length of the records,
     * is the sum of the length of the individual records after 8-byte
     * alignment adjustments. Thuts, it must be multiples of 8 bytes.
     *
     * Returns the size of the free space if the imaginary insertions would fit
     * on the page. Otherwise, returns -1.
     */
    static FieldOffset ComputeFreeSpace(FieldOffset usr_data_sz,
                                        SlotId num_recs,
                                        FieldOffset total_reclen);
private:
    /*!
     * Checks if \p sid is in the range of [`GetMinSlotId()`, `GetMaxSlotId()`].
     * If not, it throws an error.
     *
     * You may use this function to check the \p sid parameters in other
     * member functions.
     */
    inline void
    CheckSID(SlotId sid) const {
        if (sid < GetMinSlotId() || sid > GetMaxSlotId()) {
            LOG(kError, "sid is out of range, got " SLOTID_FORMAT " but "
                        "expecting in [" SLOTID_FORMAT ", " SLOTID_FORMAT "]",
                        sid, GetMinSlotId(), GetMaxSlotId());
        }
    }


    // TODO implement it
};

}   // namespace taco

#endif  // STORAGE_VARLENDATAPAGE_H
