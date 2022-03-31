#include "storage/VarlenDataPage.h"

#include "storage/FileManager.h"

namespace taco {

/*!
 * The page header of a variable-length record data page.
 *
 * Its layout is as follows:
 * |        header      | occupied space | free space | slot array |
 * | fixedhdr | usrdata | (maybe holes   ^            ^            ^
 *            ^         ^  in middle)    |            |            |
 *            |         |                |            |         PAGE_SIZE
 *    VarlenDataPageHeaderSize           |            |
 *                      |                |            |
 *                      |           m_fs_begin        |
 *                ph->m_ph_sz (maxaligned)            |
 *                                      PAGE_SIZE - sizeof(SlotData) * m_nslots
 *
 *
 * The slot array grows backwards (from high address to low). The slot for slot
 * `sid' can be indexed by `slot_array_end[-(int)sid]`, where `slot_array_end =
 * (SlotData*)(page_pointer + PAGE_SIZE)`.
 *
 */
struct VarlenDataPageHeader {
    //! The file manager managed page header. Do not modify.
    PageHeaderData  m_ph;

    //! The size of the header including the user data area.
    FieldOffset     m_ph_sz;

    //! The offset to the beginning of the free space.
    FieldOffset     m_fs_begin;

    /*!
     * Whether there might be some free space among the occupied space after
     * the last compaction. It can be an approximation but setting it to true
     * too aggresively may result in too many space compaction calls.
     */
    bool            m_has_hole : 1;

    // reserved
    bool            : 1;

    //! Number of items on this page.
    SlotId          m_cnt : 14;

    // reserved
    SlotId          :2;

    //! Number of slots in the slot array.
    SlotId          m_nslots : 14;
};

// max-aligned
static constexpr size_t VarlenDataPageHeaderSize =
    MAXALIGN(sizeof(VarlenDataPageHeader));

// we can manage to squeeze everything into 8 bytes after the page header.
static_assert(VarlenDataPageHeaderSize == MAXALIGN(8 + sizeof(PageHeaderData)));

/*!
 * Describes a slot. An invalid slot has an m_off == 0.
 */
struct SlotData {
    //! The offset to the record.
    FieldOffset     m_off;

    //! The length of the record.
    FieldOffset     m_len;

    constexpr bool
    IsValid() const {
        return m_off != 0;
    }
};

void
VarlenDataPage::InitializePage(char *pagebuf, FieldOffset usr_data_sz) {
    // TODO implement it
}


VarlenDataPage::VarlenDataPage(char *pagebuf) {
    // TODO implement it
}

char*
VarlenDataPage::GetUserData() const {
    // TODO implement it
    return nullptr;
}

SlotId
VarlenDataPage::GetMaxSlotId() const {
    // TODO implement it
    return INVALID_SID;
}

SlotId
VarlenDataPage::GetRecordCount() const {
    // TODO implement it
    return 0;
}

char *
VarlenDataPage::GetRecordBuffer(SlotId sid, FieldOffset *p_len) const {
    CheckSID(sid);

    // TODO implement it
    return nullptr;
}

bool
VarlenDataPage::IsOccupied(SlotId sid) const {
    CheckSID(sid);

    // TODO implement it
    return false;
}

bool
VarlenDataPage::InsertRecord(Record &rec) {
    // TODO implement it
    return false;
}

bool
VarlenDataPage::EraseRecord(SlotId sid) {
    // TODO implement it
    return false;
}

bool
VarlenDataPage::UpdateRecord(SlotId sid, Record &rec) {
    // TODO implement it
    return false;
}

bool
VarlenDataPage::InsertRecordAt(SlotId sid, Record &rec) {
    if (sid < GetMinSlotId() || sid > GetMaxSlotId() + 1) {
        LOG(kError, "sid is out of range, got " SLOTID_FORMAT " but "
                    "expecting in [" SLOTID_FORMAT ", " SLOTID_FORMAT "]",
                    sid, GetMinSlotId(), GetMaxSlotId() + 1);
    }

    // No need to implement it at this point in Project heap file
    return false;
}

void
VarlenDataPage::RemoveSlot(SlotId sid) {
    CheckSID(sid);

    // No need to implement it at this point in Project heap file
}

void
VarlenDataPage::ShiftSlots(SlotId n, bool truncate) {
    // No need to implement it at this point in Project heap file
    // Hint: don't forget to update m_has_hole in the page header
    //
    // This is part of the bonus project for b-tree page rebalancing. No need
    // to implement this function if you do not want to implement b-tree page
    // rebalancing.
}

FieldOffset
VarlenDataPage::ComputeFreeSpace(FieldOffset usr_data_sz,
                                 SlotId num_recs,
                                 FieldOffset total_reclen) {
    // No need to implement it at this point in Project heap file
    return -1;
}

}   // namespace taco
