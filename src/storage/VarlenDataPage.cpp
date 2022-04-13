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
    // TODO implement it
    m_pagebuf(pagebuf) {
}


char*
VarlenDataPage::GetUserData() const {
    // TODO implement it
    return m_pagebuf + VarlenDataPageHeaderSize;
}

SlotId
VarlenDataPage::GetMaxSlotId() const {
    // TODO implement it
    return ((VarlenDataPageHeader *) m_pagebuf)->m_nslots + MinSlotId - 1;
}

SlotId
VarlenDataPage::GetRecordCount() const {
    // TODO implement it
    return ((VarlenDataPageHeader *) m_pagebuf)->m_cnt;
}

char *
VarlenDataPage::GetRecordBuffer(SlotId sid, FieldOffset *p_len) const {
    CheckSID(sid);

    // TODO implement it
    SlotData *slot_data;


    slot_data= GetSlotArray() - sid;
    if (p_len) {
        *p_len = slot_data->m_len;
    }
    if (slot_data->m_off == 0) {
        return nullptr;
    }

    return m_pagebuf + slot_data->m_off;
}

bool
VarlenDataPage::IsOccupied(SlotId sid) const {
    CheckSID(sid);

    SlotData *slot_data;
    slot_data = GetSlotArray() - sid;


    return slot_data->m_off != 0;
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
VarlenDataPage::InsertRecord(Record &rec) {
    // TODO implement it
    SlotId sid;
    SlotData *slot_data = GetSlotArray();

    SlotId max_sid = GetMaxSlotId();
    
     VarlenDataPageHeader *head = (VarlenDataPageHeader *) m_pagebuf;

    
    
    if (head->m_cnt == head->m_nslots) {
        sid = max_sid;
    } else {
        for (sid = MinSlotId; sid < max_sid; ++sid) {
            if (slot_data[-sid].m_off == 0) {
                break;
            }
        }
        ASSERT(sid < max_sid);
    }

    bool allocateslot = (sid == max_sid);
    if (allocateslot)
        ++sid;

    FieldOffset slot_array_end =
        PAGE_SIZE - sizeof(SlotData) * head->m_nslots;
    if (allocateslot)
        slot_array_end -= sizeof(SlotData);

    FieldOffset available_space = MAXALIGN_DOWN(slot_array_end) - head->m_fs_begin;
    FieldOffset reclen = MAXALIGN(rec.GetLength());
    if (reclen > available_space) {
        if (head->m_has_hole) {

            CompactSpace();
            available_space = MAXALIGN_DOWN(slot_array_end) - head->m_fs_begin;
            if (reclen > available_space) {
  
                rec.GetRecordId().sid = INVALID_SID;
                return true;
            }
        } else {

            rec.GetRecordId().sid = INVALID_SID;
            return false;
        }
    }

    if (allocateslot) {
        ++head->m_nslots;
        ASSERT(sid == head->m_nslots - 1 + MinSlotId);
    }
    PutRecordIntoSlot(sid, rec);
    return true;
}

void
VarlenDataPage::PutRecordIntoSlot(SlotId sid, Record &rec) {
    SlotData *slot_data;
    VarlenDataPageHeader *head = (VarlenDataPageHeader *) m_pagebuf;

    slot_data = GetSlotArray();

    ASSERT(MAXALIGN_DOWN((FieldOffset) PAGE_SIZE
                         - (FieldOffset) sizeof(SlotData) * head->m_nslots)
            - head->m_fs_begin >= rec.GetLength());
    slot_data[-sid].m_off = head->m_fs_begin;
    slot_data[-sid].m_len = rec.GetLength();
    std::memcpy(m_pagebuf + head->m_fs_begin, rec.GetData(), rec.GetLength());
    head->m_fs_begin = head->m_fs_begin + MAXALIGN(rec.GetLength());
    ++head->m_cnt;
    rec.GetRecordId().sid = sid;
}

bool
VarlenDataPage::EraseRecord(SlotId sid) {
    // TODO implement it
    if (!IsOccupied(sid)) 
    {
        return false;
    }

    VarlenDataPageHeader *head = (VarlenDataPageHeader *) m_pagebuf;
    SlotData *slot_data = GetSlotArray() - sid;

    if (MAXALIGN(slot_data->m_off + slot_data->m_len) != head->m_fs_begin) 
    {
        head->m_has_hole = true;
    } 

    else 
    {
        head->m_fs_begin = slot_data->m_off;
    }

    slot_data->m_off = 0;
    slot_data->m_len = 0;
    --head->m_cnt;

    if (head->m_nslots == sid) 
    {
        while (head->m_nslots > 0 && !slot_data->IsValid())
        {
            --head->m_nslots;
            ++slot_data;
        }
    }
    return true;
}

bool
VarlenDataPage::UpdateRecord(SlotId sid, Record &rec) {
    // TODO implement it


    FieldOffset rec_length = MAXALIGN(rec.GetLength());

    VarlenDataPageHeader *head = (VarlenDataPageHeader *) m_pagebuf;

    if (head->m_ph_sz + rec_length +
        MAXALIGN(sizeof(SlotData)) > (FieldOffset) PAGE_SIZE) {
        return false;
    }

    SlotData *slot_data = GetSlotArray() - sid;

    if (rec_length <= MAXALIGN(slot_data->m_len)) {
        memcpy(m_pagebuf + slot_data->m_off, rec.GetData(), rec.GetLength());

        if (rec_length <= MAXALIGN_DOWN(slot_data->m_len - 1)) {
            
            if (slot_data->m_off + MAXALIGN(slot_data->m_len) != head->m_fs_begin) 
            
            {
                head->m_has_hole = true;
            } 
            
            else 
            {
                head->m_fs_begin = slot_data->m_off + rec_length;
            }
        }
        rec.GetRecordId().sid = sid;


        slot_data->m_len = rec.GetLength();
        
        return true;
    }


    FieldOffset slot_array_end =
        MAXALIGN_DOWN(PAGE_SIZE - sizeof(SlotData) * head->m_nslots);


    if (slot_data->m_off + MAXALIGN(slot_data->m_len) == head->m_fs_begin) 
    {
        FieldOffset aspace = slot_array_end - slot_data->m_off;

        if (rec_length <= aspace) 
        {
            memcpy(m_pagebuf + slot_data->m_off, rec.GetData(), rec.GetLength());

            slot_data->m_len = rec.GetLength();

            head->m_fs_begin = slot_data->m_off + rec_length;

            rec.GetRecordId().sid = sid;
            return true;
        }
    } else {
        FieldOffset aspace = slot_array_end - head->m_fs_begin;

        if (rec_length <= aspace) 
        {
            slot_data->m_off = head->m_fs_begin;
            slot_data->m_len = rec.GetLength();
            memcpy(m_pagebuf + head->m_fs_begin, rec.GetData(), rec.GetLength());
            head->m_fs_begin = head->m_fs_begin + rec_length;
            head->m_has_hole = true;
            rec.GetRecordId().sid = sid;
            return true;
        }
    }
    slot_data->m_off = 0;
    slot_data->m_len = 0;
    if (head->m_has_hole) {
        CompactSpace();
        FieldOffset aspace = slot_array_end - head->m_fs_begin;
        if (rec_length <= aspace) {
            slot_data->m_off = head->m_fs_begin;
            slot_data->m_len = rec.GetLength();
            memcpy(m_pagebuf + head->m_fs_begin, rec.GetData(), rec.GetLength());
            head->m_fs_begin = head->m_fs_begin + rec_length;
            rec.GetRecordId().sid = sid;
            return true;
        }
    }

    if (sid == head->m_nslots) {
        while (head->m_nslots > 0 && !slot_data->IsValid()) {
            --head->m_nslots;
            ++slot_data;
        }
    }
    --head->m_cnt;
    rec.GetRecordId().sid = INVALID_SID;
    return true;
}

bool
VarlenDataPage::InsertRecordAt(SlotId sid, Record &rec) {
    if (sid < GetMinSlotId() || sid > GetMaxSlotId() + 1) {
        LOG(kError, "sid is out of range, got " SLOTID_FORMAT " but "
                    "expecting in [" SLOTID_FORMAT ", " SLOTID_FORMAT "]",
                    sid, GetMinSlotId(), GetMaxSlotId() + 1);
    }
    VarlenDataPageHeader *head = (VarlenDataPageHeader *) m_pagebuf;
    FieldOffset slot_array_end = PAGE_SIZE - sizeof(SlotData) * (head->m_nslots+1);
    FieldOffset aspace = MAXALIGN_DOWN(slot_array_end) - head->m_fs_begin;
    FieldOffset rec_length = MAXALIGN(rec.GetLength());
    
    
    if (rec_length > aspace) {

        if (head->m_has_hole) {
            CompactSpace();
            aspace = MAXALIGN_DOWN(slot_array_end) - head->m_fs_begin;

            if (rec_length > aspace) {
                rec.GetRecordId().sid = INVALID_SID;
                return true;
            }
        } else {
            rec.GetRecordId().sid = INVALID_SID;
            return false;
        }
    }

    int count =GetMaxSlotId()+1;
    if(sid <= GetMaxSlotId() && IsOccupied(sid)){

        SlotData *slot_data = GetSlotArray();

        while(count >sid) {
            Record rec = GetRecord(count-1);
            rec.GetRecordId().sid = count;
            slot_data[-count].m_off = slot_data[-(count-1)].m_off;
            slot_data[-count].m_len = slot_data[-(count-1)].m_len;
            count--;
        }
    }
    ++head->m_nslots;
    PutRecordIntoSlot(sid, rec);
    return true;
}

void
VarlenDataPage::RemoveSlot(SlotId sid) {
    CheckSID(sid);
    int count =sid;
    VarlenDataPageHeader *head = (VarlenDataPageHeader *) m_pagebuf;
    SlotData *slot = GetSlotArray();
    
    while(count<GetMaxOccupiedSlotId()){
        Record rec = GetRecord(count);
        std::memmove((void*)(m_pagebuf + slot[-count].m_off), (void*)(m_pagebuf+slot[-(count+1)].m_off), slot[-(count+1)].m_len);
        RecordId r_id = rec.GetRecordId();
        r_id.sid = count;

        slot[-(count)].m_off = slot[-(count+1)].m_off;
        slot[-(count)].m_len = slot[-(count+1)].m_len;
        count++;
    }

    head->m_fs_begin = *m_pagebuf + slot[-(count-1)].m_off+slot[-(count-1)].m_len;

    slot[-count].m_off = 0;
    slot[-count].m_off = 0;
    --head->m_cnt;
    --head->m_nslots;
}

void
VarlenDataPage::ShiftSlots(SlotId n, bool truncate) {
}

FieldOffset
VarlenDataPage::ComputeFreeSpace(FieldOffset usr_data_sz,
                                 SlotId num_recs,
                                 FieldOffset total_reclen) {
    FieldOffset val =  PAGE_SIZE - (VarlenDataPageHeaderSize + usr_data_sz + total_reclen + sizeof(SlotData)*num_recs);
    return (val <= 0) ? -1 : val;
}

}   // namespace taco
