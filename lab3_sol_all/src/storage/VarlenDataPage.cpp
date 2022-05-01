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
    if (usr_data_sz > (FieldOffset) PAGE_SIZE ||
        MAXALIGN(VarlenDataPageHeaderSize + usr_data_sz) + MAXALIGN_OF +
        MAXALIGN(sizeof(SlotData)) > (FieldOffset) PAGE_SIZE) {
        LOG(kError, "The given user data size " FIELDOFFSET_FORMAT " is too "
                    "large for the page to fit at least one record.",
                    usr_data_sz);
    }

    VarlenDataPageHeader *hdr = (VarlenDataPageHeader *) pagebuf;
    hdr->m_ph_sz = MAXALIGN(VarlenDataPageHeaderSize + usr_data_sz);
    hdr->m_fs_begin = hdr->m_ph_sz;
    hdr->m_has_hole = false;
    hdr->m_cnt = 0;
    hdr->m_nslots = 0;
}


VarlenDataPage::VarlenDataPage(char *pagebuf):
    m_pagebuf(pagebuf) {
}

char*
VarlenDataPage::GetUserData() const {
    return m_pagebuf + VarlenDataPageHeaderSize;
}

SlotId
VarlenDataPage::GetMaxSlotId() const {
    return ((VarlenDataPageHeader *) m_pagebuf)->m_nslots + MinSlotId - 1;
}

SlotId
VarlenDataPage::GetRecordCount() const {
    return ((VarlenDataPageHeader *) m_pagebuf)->m_cnt;
}

char *
VarlenDataPage::GetRecordBuffer(SlotId sid, FieldOffset *p_len) const {
    CheckSID(sid);

    SlotData *slot = GetSlotArray() - sid;
    if (slot->m_off == 0) {
        // not occupied
        return nullptr;
    }

    if (p_len) {
        *p_len = slot->m_len;
    }
    return m_pagebuf + slot->m_off;
}

bool
VarlenDataPage::IsOccupied(SlotId sid) const {
    CheckSID(sid);

    SlotData *slot = GetSlotArray() - sid;
    return slot->m_off != 0;
}

bool
VarlenDataPage::InsertRecord(Record &rec) {
    VarlenDataPageHeader *hdr = (VarlenDataPageHeader *) m_pagebuf;

    SlotId max_sid = GetMaxSlotId();
    SlotData *slot = GetSlotArray();
    SlotId sid;
    if (hdr->m_cnt == hdr->m_nslots) {
        // no empty slots left
        sid = max_sid;
    } else {
        // find the empty slot in the slot array
        for (sid = MinSlotId; sid < max_sid; ++sid) {
            if (slot[-sid].m_off == 0) {
                break;
            }
        }
        ASSERT(sid < max_sid);
    }

    // allocate a new slot if all slots from MinSlotId to MaxSlotId are occupied
    // But don't increment hdr->m_nslots yet, in case we find that there's
    // not free space that fits this record.
    bool allocate_new_slot = (sid == max_sid);
    if (allocate_new_slot)
        ++sid;

    FieldOffset slot_array_end =
        PAGE_SIZE - sizeof(SlotData) * hdr->m_nslots;
    if (allocate_new_slot)
        slot_array_end -= sizeof(SlotData);

    // note: available_space could be smaller than 0 here because we could have
    // just allocated a new slot that overlaps with the occupied space.  That
    // is ok though, since we won't actually write anything until
    // available_space is greater than reclen.
    FieldOffset available_space = MAXALIGN_DOWN(slot_array_end) - hdr->m_fs_begin;
    FieldOffset reclen = MAXALIGN(rec.GetLength());
    if (reclen > available_space) {
        if (hdr->m_has_hole) {
            // try to compact the space by removing the holes
            CompactSpace();
            available_space = MAXALIGN_DOWN(slot_array_end) - hdr->m_fs_begin;
            if (reclen > available_space) {
                // not enough space on this page
                rec.GetRecordId().sid = INVALID_SID;
                return true;
            }
        } else {
            // not enough space on this page
            rec.GetRecordId().sid = INVALID_SID;
            return false;
        }
    }

    // There's free space that fits this record. Do the insertion.
    if (allocate_new_slot) {
        ++hdr->m_nslots;
        ASSERT(sid == hdr->m_nslots - 1 + MinSlotId);
    }
    PutRecordIntoSlot(sid, rec);
    return true;
}

void
VarlenDataPage::PutRecordIntoSlot(SlotId sid, Record &rec) {
    VarlenDataPageHeader *hdr = (VarlenDataPageHeader *) m_pagebuf;
    SlotData *slot = GetSlotArray();
    ASSERT(MAXALIGN_DOWN((FieldOffset) PAGE_SIZE
                         - (FieldOffset) sizeof(SlotData) * hdr->m_nslots)
            - hdr->m_fs_begin >= rec.GetLength());
    slot[-sid].m_off = hdr->m_fs_begin;
    slot[-sid].m_len = rec.GetLength();
    std::memcpy(m_pagebuf + hdr->m_fs_begin, rec.GetData(), rec.GetLength());
    hdr->m_fs_begin = hdr->m_fs_begin + MAXALIGN(rec.GetLength());
    ++hdr->m_cnt;
    rec.GetRecordId().sid = sid;
}

bool
VarlenDataPage::EraseRecord(SlotId sid) {
    // IsOccupied() will check SID
    if (!IsOccupied(sid)) return false;

    VarlenDataPageHeader *hdr = (VarlenDataPageHeader *) m_pagebuf;
    SlotData *slot = GetSlotArray() - sid;
    if (MAXALIGN(slot->m_off + slot->m_len) != hdr->m_fs_begin) {
        hdr->m_has_hole = true;
    } else {
        // just free the space at the end of the free space
        hdr->m_fs_begin = slot->m_off;
    }

    slot->m_off = 0;
    slot->m_len = 0;
    --hdr->m_cnt;

    if (hdr->m_nslots == sid) {
        while (hdr->m_nslots > 0 && !slot->IsValid()) {
            --hdr->m_nslots;
            ++slot;
        }
    }
    return true;
}

bool
VarlenDataPage::UpdateRecord(SlotId sid, Record &rec) {
    if (!IsOccupied(sid)) {
        LOG(kError, "slot " SLOTID_FORMAT " is not occupied", sid);
    }

    FieldOffset reclen = MAXALIGN(rec.GetLength());
    VarlenDataPageHeader *hdr = (VarlenDataPageHeader *) m_pagebuf;
    if (hdr->m_ph_sz + reclen +
        MAXALIGN(sizeof(SlotData)) > (FieldOffset) PAGE_SIZE) {
        return false;
    }

    SlotData *slot = GetSlotArray() - sid;
    if (reclen <= MAXALIGN(slot->m_len)) {
        // can do in-place update
        memcpy(m_pagebuf + slot->m_off, rec.GetData(), rec.GetLength());
        if (reclen <= MAXALIGN_DOWN(slot->m_len - 1)) {
            if (slot->m_off + MAXALIGN(slot->m_len) != hdr->m_fs_begin) {
                hdr->m_has_hole = true;
            } else {
                hdr->m_fs_begin = slot->m_off + reclen;
            }
        }
        slot->m_len = rec.GetLength();
        rec.GetRecordId().sid = sid;
        return true;
    }

    // try to find space on this page to fit the update
    FieldOffset slot_array_end =
        MAXALIGN_DOWN(PAGE_SIZE - sizeof(SlotData) * hdr->m_nslots);
    if (slot->m_off + MAXALIGN(slot->m_len) == hdr->m_fs_begin) {
        // if this is the last record in the payload (not last slot)
        // try to extend the space
        FieldOffset available_space = slot_array_end - slot->m_off;
        if (reclen <= available_space) {
            // we can fit the longer record at the original offset
            memcpy(m_pagebuf + slot->m_off, rec.GetData(), rec.GetLength());
            slot->m_len = rec.GetLength();
            hdr->m_fs_begin = slot->m_off + reclen;
            rec.GetRecordId().sid = sid;
            return true;
        }
    } else {
        // try if we can fit the new record completely in the free space
        FieldOffset available_space = slot_array_end - hdr->m_fs_begin;
        if (reclen <= available_space) {
            // yes, we have enough free space for the record
            slot->m_off = hdr->m_fs_begin;
            slot->m_len = rec.GetLength();
            memcpy(m_pagebuf + hdr->m_fs_begin, rec.GetData(), rec.GetLength());
            hdr->m_fs_begin = hdr->m_fs_begin + reclen;
            hdr->m_has_hole = true;
            rec.GetRecordId().sid = sid;
            return true;
        }
    }

    // we can't fit the new record without modifying the page, so try to compact
    // the space if possible.
    // Don't remove the slot even if it is the last one here.  If
    // CompactSpace() does not make enough room for the new record, we'll
    // remove the slot after this.
    slot->m_off = 0;
    slot->m_len = 0;
    if (hdr->m_has_hole) {
        CompactSpace();

        // compute available space again to see if we have successfully
        // freed some more space
        FieldOffset available_space = slot_array_end - hdr->m_fs_begin;
        if (reclen <= available_space) {
            // now there's enough space
            slot->m_off = hdr->m_fs_begin;
            slot->m_len = rec.GetLength();
            memcpy(m_pagebuf + hdr->m_fs_begin, rec.GetData(), rec.GetLength());
            hdr->m_fs_begin = hdr->m_fs_begin + reclen;
            rec.GetRecordId().sid = sid;
            return true;
        }
    }

    // We don't have space but the record could fit in another page. Just erase
    // the record in this case.
    if (sid == hdr->m_nslots) {
        while (hdr->m_nslots > 0 && !slot->IsValid()) {
            --hdr->m_nslots;
            ++slot;
        }
    }
    --hdr->m_cnt;
    rec.GetRecordId().sid = INVALID_SID;
    return true;
}

void
VarlenDataPage::CompactSpace() {
    VarlenDataPageHeader *hdr = (VarlenDataPageHeader *) m_pagebuf;
    ASSERT(hdr->m_has_hole);

    SlotData *slot = GetSlotArray();
    SlotId max_sid = GetMaxSlotId();

    // sort all valid slot ID in increasing offset order
    std::vector<SlotId> sorted_sid;
    sorted_sid.reserve(hdr->m_cnt);
    for (SlotId sid = MinSlotId; sid <= max_sid; ++sid) {
        if (slot[-sid].m_off != 0) {
            sorted_sid.push_back(sid);
        }
    }
    std::sort(sorted_sid.begin(), sorted_sid.end(),
        [=](SlotId sid1, SlotId sid2) -> bool {
            return slot[-sid1].m_off <
                   slot[-sid2].m_off;
        });

    FieldOffset new_fs_begin = hdr->m_ph_sz;
    for (size_t i = 0; i < sorted_sid.size(); ++i) {
        SlotId sid = sorted_sid[i];
        FieldOffset &off = slot[-sid].m_off;
        FieldOffset len = slot[-sid].m_len;
        ASSERT(off >= new_fs_begin);
        if (off > new_fs_begin) {
            ASSERT(off > new_fs_begin);
            std::memmove(m_pagebuf + new_fs_begin, m_pagebuf + off, len);
        }
        off = new_fs_begin;
        new_fs_begin = MAXALIGN(new_fs_begin + len);
    }

    ASSERT(new_fs_begin <= hdr->m_fs_begin);
    hdr->m_fs_begin = new_fs_begin;
    hdr->m_has_hole = false;
}

bool
VarlenDataPage::InsertRecordAt(SlotId sid, Record &rec) {
    if (sid < GetMinSlotId() || sid > GetMaxSlotId() + 1) {
        LOG(kError, "sid is out of range, got " SLOTID_FORMAT " but "
                    "expecting in [" SLOTID_FORMAT ", " SLOTID_FORMAT "]",
                    sid, GetMinSlotId(), GetMaxSlotId() + 1);
    }

    bool allocate_new_slot = false;
    if (sid == GetMaxSlotId() + 1 || IsOccupied(sid)) {
        allocate_new_slot = true;
    }

    VarlenDataPageHeader *hdr = (VarlenDataPageHeader *) m_pagebuf;
    FieldOffset slot_array_end = PAGE_SIZE - sizeof(SlotData) * hdr->m_nslots;
    if (allocate_new_slot)
        slot_array_end -= sizeof(SlotData);

    // note: same as InsertRecord(), available_space could be smaller than 0
    // here (see the explanation there).
    FieldOffset available_space =
        MAXALIGN_DOWN(slot_array_end) - hdr->m_fs_begin;
    FieldOffset reclen = MAXALIGN(rec.GetLength());
    if (reclen > available_space) {
        if (hdr->m_has_hole) {
            CompactSpace();
            available_space = MAXALIGN_DOWN(slot_array_end) - hdr->m_fs_begin;
            if (reclen > available_space) {
                rec.GetRecordId().sid = INVALID_SID;
                return true;
            }
        } else {
            // not enough space to copy the record
            rec.GetRecordId().sid = INVALID_SID;
            return false;
        }
    }

    if (allocate_new_slot) {
        ++hdr->m_nslots;
        if (sid != hdr->m_nslots) {
            // move the existing slots
            SlotData *slot = GetSlotArray();
            std::memmove(slot - hdr->m_nslots,
                         slot - hdr->m_nslots + 1,
                         (hdr->m_nslots - sid) * sizeof(SlotData));
        }
    }
    PutRecordIntoSlot(sid, rec);
    return true;

}

void
VarlenDataPage::RemoveSlot(SlotId sid) {
    CheckSID(sid);

    VarlenDataPageHeader *hdr = (VarlenDataPageHeader *) m_pagebuf;

    // decrement the record count and update the m_has_hole bit
    if (IsOccupied(sid)){
        --hdr->m_cnt;
        SlotData *slot = GetSlotArray() - sid;
        if (MAXALIGN(slot->m_off + slot->m_len) != hdr->m_fs_begin) {
            hdr->m_has_hole = true;
        } else {
            // just free the space at the end
            hdr->m_fs_begin = slot->m_off;
        }
    }

    // shift the slots
    if (sid < hdr->m_nslots) {
        SlotData *slot = GetSlotArray();
        std::memmove(slot - hdr->m_nslots + 1,
                     slot - hdr->m_nslots,
                     (hdr->m_nslots - sid) * sizeof(SlotData));
    }
    --hdr->m_nslots;
}

void
VarlenDataPage::ShiftSlots(SlotId n, bool truncate) {
    VarlenDataPageHeader *hdr = (VarlenDataPageHeader *) m_pagebuf;

    if (truncate) {
        if (n >= hdr->m_nslots) {
            // truncating all the slots
            hdr->m_nslots = 0;
            hdr->m_cnt = 0;
            hdr->m_has_hole = false;
            hdr->m_fs_begin = hdr->m_ph_sz;
            return ;
        } else {
            // update the record count
            SlotData *slot = GetSlotArray();
            for (SlotId i = 1; i <= n; ++i) {
                if (slot[-i].IsValid()) {
                    --hdr->m_cnt;
                }
            }

            // It's probably not worth the effort to check if we'd have any
            // hole after truncating the first n slots since we're likely to
            // compact the space very soon if there indeed is. Setting it to
            // true is always safe at the small cost of a scan on the page.
            hdr->m_has_hole = true;
        }
    } else {
        // make sure we still have enough space for the new slots
        if (n > PAGE_SIZE / sizeof(SlotData)) {
            LOG(kFatal, "not enough space for " SLOTID_FORMAT " new slots", n);
        }
        FieldOffset new_slot_array_end =
            MAXALIGN_DOWN(PAGE_SIZE - (n + hdr->m_nslots) * sizeof(SlotData));
        if (new_slot_array_end < hdr->m_fs_begin) {
            if (hdr->m_has_hole) {
                CompactSpace();
            }
        }
        if (new_slot_array_end < hdr->m_fs_begin) {
            LOG(kFatal, "not enough space for " SLOTID_FORMAT " new slots", n);
        }
    }

    SlotData *cur_slot_array = GetSlotArray() - hdr->m_nslots;
    SlotId new_nslots = truncate ? (hdr->m_nslots - n) : (hdr->m_nslots + n);
    SlotData *tgt_slot_array = GetSlotArray() - new_nslots;
    ASSERT(((FieldOffset)(((char *) tgt_slot_array) - m_pagebuf)) >=
            hdr->m_fs_begin);
    std::memmove(tgt_slot_array, cur_slot_array,
                 std::min(hdr->m_nslots, new_nslots) * sizeof(SlotData));

    hdr->m_nslots = new_nslots;
    if (!truncate) {
        // mark the first n slots as unoccupied
        memset(GetSlotArray() - n, 0, sizeof(SlotData) * n);
    }
}

FieldOffset
VarlenDataPage::ComputeFreeSpace(FieldOffset usr_data_sz,
                                 SlotId num_recs,
                                 FieldOffset total_reclen) {
    // a few checks to prevent arithmetic overflow
    if (usr_data_sz > (FieldOffset) PAGE_SIZE) {
        return -1;
    }
    if (total_reclen > (FieldOffset) PAGE_SIZE) {
        return -1;
    }
    if (num_recs > ((FieldOffset) PAGE_SIZE) / sizeof(SlotData)) {
        return -1;
    }

    FieldOffset total_sz = MAXALIGN(VarlenDataPageHeaderSize + usr_data_sz)
        + total_reclen + sizeof(SlotData) * num_recs;
    if (total_sz > (FieldOffset) PAGE_SIZE)
        return -1;
    return (FieldOffset) PAGE_SIZE - total_sz;
}

}   // namespace taco
