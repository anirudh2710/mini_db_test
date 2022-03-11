#include "storage/VarlenDataPage.h"
#include <queue>
#include "storage/FileManager.h"
#include "utils/zerobuf.h"

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
    static SlotId firstFreeSlotId = 0;
    VarlenDataPageHeader *header;
    VarlenDataPage* VarlenDataPage::m_obj = NULL;
    void
    VarlenDataPage::InitializePage(char *pagebuf, FieldOffset usr_data_sz) {
        FieldOffset headerSize = MAXALIGN(VarlenDataPageHeaderSize+usr_data_sz);
        if( headerSize >= PAGE_SIZE){
            LOG(kError, "No space for records since user data size is huge");
            return;
        }
        header = ((struct VarlenDataPageHeader*)pagebuf);
        header->m_ph_sz = headerSize;
        header->m_has_hole = false;
        header->m_cnt = 0;
        header->m_nslots = 0;
        header->m_fs_begin =headerSize;
        firstFreeSlotId = 0;
        //m_obj = new VarlenDataPage(pagebuf, usr_data_sz);
    }


    VarlenDataPage::VarlenDataPage(char *pagebuf, FieldOffset usr_data_sz) {
        m_pagebuf = pagebuf;
        m_usr_data_sz = usr_data_sz;
    }

    char*
    VarlenDataPage::GetUserData() const {
        if(m_pagebuf) return (char*)(m_pagebuf+VarlenDataPageHeaderSize);
        return nullptr;
    }

    SlotId
    VarlenDataPage::GetMaxSlotId() const {
        for(SlotId sid = header->m_nslots;sid>0;sid--){
            SlotData* data = (SlotData*)(m_pagebuf+PAGE_SIZE-sizeof(SlotData)*sid);
            if(data->IsValid()){
                return sid;
            }
        }
        return INVALID_SID;
    }

    SlotId
    VarlenDataPage::GetRecordCount() const {
        return header->m_cnt;
    }

    char *
    VarlenDataPage::GetRecordBuffer(SlotId sid, FieldOffset *p_len) const {
        CheckSID(sid);
        SlotData *slotData = (SlotData*)(m_pagebuf+PAGE_SIZE-sizeof(SlotData)*sid);
        char *temp = (char*)malloc(slotData->m_len);
        //memcpy((char*)temp, (char*)(m_pagebuf+slotData->m_off), slotData->m_len);
        if(p_len!=nullptr)
            *p_len = slotData->m_len;
        return (char*)(m_pagebuf+slotData->m_off);
    }

    bool
    VarlenDataPage::IsOccupied(SlotId sid) const {
        CheckSID(sid);
        SlotData *slotData = (SlotData*)(m_pagebuf+PAGE_SIZE-sizeof(SlotData)*sid);
        return slotData->m_off!=0;
    }

    void
    VarlenDataPage::compactPage(){
        VarlenDataPageHeader* pagebuff = (VarlenDataPageHeader*)m_pagebuf;
        struct SlotPtrCmpOffst {
            bool operator() (SlotData* const &a, SlotData* const &b) {
                return a->m_off > b->m_off;
            }
        };
        std::priority_queue<SlotData*, std::vector<SlotData*>, SlotPtrCmpOffst> slotPtrs;
        for(SlotId slotID = 1; slotID <= pagebuff->m_nslots; slotID++) {
            SlotData* sd = (SlotData*)(m_pagebuf+PAGE_SIZE-sizeof(SlotData)*slotID);
            SlotData& slot = *sd;
            if (slot.IsValid())
                slotPtrs.push(&slot);
        }

        FieldOffset offset = pagebuff->m_ph_sz;

        // move records
        while (!slotPtrs.empty()) {
            SlotData* slot = slotPtrs.top();
            slotPtrs.pop();

            // nothing to move, if the record length is 0 bytes
            if (slot->m_len == 0)
                continue;

            // move the data
            memmove((void*)pagebuff+offset, (void*)pagebuff+slot->m_off, slot->m_len);
            slot->m_off = offset;
            offset = MAXALIGN(offset+slot->m_len);
        }
        pagebuff->m_fs_begin = offset;

    }

    bool
    VarlenDataPage::InsertRecord(Record &rec, SlotId sid) {
        VarlenDataPageHeader* pagebuff = (VarlenDataPageHeader*)m_pagebuf;
        FieldOffset lengthOfRecord = rec.GetLength();
        FieldOffset nextFreeSpaceForSlot = PAGE_SIZE - sizeof(SlotData) * (pagebuff->m_nslots);
        FieldOffset nextFreeSpaceForRecord = pagebuff->m_fs_begin;

        FieldOffset fsBeginAfterAddingRecord = MAXALIGN(lengthOfRecord+nextFreeSpaceForRecord);
        if(fsBeginAfterAddingRecord>nextFreeSpaceForSlot)
            compactPage();

        nextFreeSpaceForRecord = pagebuff->m_fs_begin;
        fsBeginAfterAddingRecord = MAXALIGN(lengthOfRecord+nextFreeSpaceForRecord);

        if((fsBeginAfterAddingRecord <= nextFreeSpaceForSlot)||(firstFreeSlotId!=0 && firstFreeSlotId <= pagebuff->m_nslots)){
            memcpy(m_pagebuf+nextFreeSpaceForRecord, rec.GetData(), lengthOfRecord);
            rec.GetRecordId().sid = sid;
            SlotData *slotData = (SlotData*)(m_pagebuf+PAGE_SIZE-sizeof(SlotData)*sid);
            slotData->m_len = lengthOfRecord;
            slotData->m_off = nextFreeSpaceForRecord;
            pagebuff->m_fs_begin = MAXALIGN(lengthOfRecord+nextFreeSpaceForRecord);
            return true;
        }
        rec.GetRecordId().sid = INVALID_SID;
        return false;
    }

    bool
    VarlenDataPage::InsertRecord(Record &rec) {
        VarlenDataPageHeader* pagebuff = (VarlenDataPageHeader*)m_pagebuf;
        FieldOffset lengthOfRecord = rec.GetLength();
        FieldOffset nextFreeSpaceForSlot = PAGE_SIZE - sizeof(SlotData) * (pagebuff->m_nslots+1);
        FieldOffset nextFreeSpaceForRecord = pagebuff->m_fs_begin;

        FieldOffset fsBeginAfterAddingRecord = MAXALIGN(lengthOfRecord+nextFreeSpaceForRecord);
        FieldOffset nextFreeSpaceForSlotAfterAddingSlot = nextFreeSpaceForSlot - (sizeof(SlotData));
        if(fsBeginAfterAddingRecord>nextFreeSpaceForSlotAfterAddingSlot)
            compactPage();

        nextFreeSpaceForRecord = pagebuff->m_fs_begin;
        fsBeginAfterAddingRecord = MAXALIGN(lengthOfRecord+nextFreeSpaceForRecord);
        if((fsBeginAfterAddingRecord <= nextFreeSpaceForSlot)||(firstFreeSlotId!=0 && firstFreeSlotId <= pagebuff->m_nslots)){
            SlotId sid = INVALID_SID;
            if(firstFreeSlotId!=0 && firstFreeSlotId <= pagebuff->m_nslots){
                sid = firstFreeSlotId;

                SlotId i = sid+1;
                for(;i<=pagebuff->m_nslots;i++){
                    SlotData* data = (SlotData*)(m_pagebuf+PAGE_SIZE-sizeof(SlotData)*i);
                    if(!data->IsValid()){
                        break;
                    }
                }
                firstFreeSlotId = i;
            }else{
                pagebuff->m_nslots++;
                sid = pagebuff->m_nslots;
                firstFreeSlotId = pagebuff->m_nslots+1;
                SlotData *sd = new SlotData();
                memcpy(m_pagebuf+nextFreeSpaceForSlot, sd, sizeof(SlotData));
            }
            memcpy(m_pagebuf+nextFreeSpaceForRecord, rec.GetData(), lengthOfRecord);
            rec.GetRecordId().sid = sid;
            SlotData* sd = (SlotData*)(m_pagebuf+PAGE_SIZE-sizeof(SlotData)*sid);
            sd->m_len = lengthOfRecord;
            sd->m_off = nextFreeSpaceForRecord;
            pagebuff->m_cnt++;
            pagebuff->m_fs_begin = MAXALIGN(lengthOfRecord+nextFreeSpaceForRecord);
            return true;
        }
        rec.GetRecordId().sid = INVALID_SID;
        return false;
    }

    bool
    VarlenDataPage::EraseRecord(SlotId sid) {
        CheckSID(sid);
        VarlenDataPageHeader* pagebuff = (VarlenDataPageHeader*)m_pagebuf;
        if(sid>pagebuff->m_nslots) return false;
        SlotData *slotData = (SlotData*)(m_pagebuf+PAGE_SIZE-sizeof(SlotData)*sid);
        if(slotData->m_off==0) return false;
        //memset(pagebuff+slotData->m_off, 0, slotData->m_len);
        slotData->m_off=0;
        slotData->m_len=0;
        pagebuff->m_cnt--;
        if(sid<firstFreeSlotId)
            firstFreeSlotId = sid;
        return true;
    }

    bool
    VarlenDataPage::UpdateRecord(SlotId sid, Record &rec) {
        CheckSID(sid);
        if(rec.GetLength()>=PAGE_SIZE){
            return false;  
        } 
        VarlenDataPageHeader* pagebuff = (VarlenDataPageHeader*)m_pagebuf;
        if(sid>pagebuff->m_nslots){
            LOG(kError, "Invalid sid while updating record");
            return false;
        } 
        SlotData *slotData = (SlotData*)(m_pagebuf+PAGE_SIZE-sizeof(SlotData)*sid);
        if(!slotData->IsValid()){
            LOG(kError, "SlotId unoccupied");
            return false;
        }
        if(rec.GetLength()<=slotData->m_len){
            memcpy(m_pagebuf+slotData->m_off, rec.GetData(), rec.GetLength());
            //memcpy(m_pagebuf+slotData->m_off+rec.GetLength(), g_zerobuf, slotData->m_len - rec.GetLength());
            slotData->m_len = rec.GetLength();
            rec.GetRecordId().sid  = sid;
            return true;
        }
        FieldOffset backupOffset = slotData->m_off;
        slotData->m_off = 0;
        FieldOffset backupLength = slotData->m_len;
        slotData->m_len = 0;
        if(InsertRecord(rec, sid)){
            return true;
        }
        slotData->m_off = backupOffset;
        slotData->m_len = backupLength;
        EraseRecord(sid);
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