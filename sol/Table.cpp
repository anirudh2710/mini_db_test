#include "storage/Table.h"

#include "storage/BufferManager.h"
#include "storage/FileManager.h"
#include "storage/VarlenDataPage.h"

namespace taco {

void
Table::Initialize(const TableDesc *tabdesc) {
    std::shared_ptr<File> file =
        g_fileman->Open(tabdesc->GetTableEntry()->tabfid());

    PageNumber first_pid = file->GetFirstPageNumber();
    ASSERT(first_pid != INVALID_PID);

    char *buf;
    ScopedBufferId bufid = g_bufman->PinPage(first_pid, &buf);
    // We don't have to latch the page here as this should only be called
    // for creating a new table and there're no concurrent access.

    // Make sure we are passed a new file.
    PageHeaderData *ph = (PageHeaderData *) buf;
    if (ph->GetNextPageNumber() != INVALID_PID) {
        LOG(kError, "cannot initialize an existing table file");
    }

    ASSERT(tabdesc->GetTableEntry()->tabisvarlen());
    VarlenDataPage::InitializePage(buf);
    g_bufman->MarkDirty(bufid);
}

std::unique_ptr<Table>
Table::Create(std::shared_ptr<const TableDesc> tabdesc) {
    std::unique_ptr<File> f =
        g_fileman->Open(tabdesc->GetTableEntry()->tabfid());
    return absl::WrapUnique(new Table(std::move(tabdesc), std::move(f)));
}

Table::~Table() {
    // m_file automatically closes upon destruction
}

void
Table::InsertRecord(Record& rec) {
    // Do a quick check before we attemp to insert a record on an actual data
    // page, although the data page usually have smaller size limits.
    if (rec.GetLength() > (FieldOffset) PAGE_SIZE) {
        LOG(kError, "record too long to insert on a page: "
            FIELDOFFSET_FORMAT, rec.GetLength());
    }

    ScopedBufferId bufid;
    for (;;) {
        if (m_insertion_pid == INVALID_PID) {
            // start from the last page
            m_insertion_pid = m_file->GetLastPageNumber();
        }

        char *buf;
        if (!bufid.IsValid()) {
            // we only need to pin and latch the page here if this is an
            // existing page
            bufid = g_bufman->PinPage(m_insertion_pid,
                                      &buf,
                                      m_file->GetFileId());
            if (!bufid.IsValid()) {
                LOG(kFatal,
                    "wrong page read from a different file other than "
                    "the file " FILEID_FORMAT,
                    m_file->GetFileId());
            }
            //g_bufman->LatchPage(bufid, LatchMode::EX);
        } else {
            // new page allocated in the previous loop which is already pinned
            // and latched
            buf = g_bufman->GetBuffer(bufid);
        }

        {
            VarlenDataPage dp(buf);
            if (dp.InsertRecord(rec)) {
                // page modified
                g_bufman->MarkDirty(bufid);
                if (rec.GetRecordId().sid != INVALID_SID) {
                    // done with insertion
                    rec.GetRecordId().pid = g_bufman->GetPageNumber(bufid);
                    return ;
                }
                // Otherwise, the record won't fit here.
                // Fall through to try a new page.
            } else {
                if (dp.GetRecordCount() == 0) {
                    // If even an empty page can't hold a record, that
                    // indicates the tuple is too long.
                    LOG(kError, "record too long to insert on a page: "
                        FIELDOFFSET_FORMAT, rec.GetLength());
                }
            }
        }

        // This page is too full to fit this record. Need to try a new page.
        // We don't need a latch on the page to read the file manager meta.
        //g_bufman->UnlatchPage(bufid);
        PageHeaderData *ph = (PageHeaderData*) buf;
        PageNumber next_pid = ph->GetNextPageNumber();
        if (next_pid == INVALID_PID) {
            // No next page, allocate a new one.
            // There's a chance that a concurrent writer also allocates a page
            // at the same time, but it only leaves a (potentially large space)
            // on a page in the worst case.
            bufid = m_file->AllocatePage(LatchMode::EX);
            m_insertion_pid = g_bufman->GetPageNumber(bufid);

            // initialize the page
            buf = g_bufman->GetBuffer(bufid);
            {
                VarlenDataPage::InitializePage(buf);
            }
            g_bufman->MarkDirty(bufid);
        } else {
            // try the next page
            m_insertion_pid = next_pid;
            g_bufman->UnpinPage(bufid);
        }
    }

    LOG(kFatal, "unreachable");
}

void
Table::EraseRecord(RecordId rid) {
    if (!rid.IsValid()) {
        LOG(kError, "invalid record ID");
    }

    char *buf;
    ScopedBufferId bufid = g_bufman->PinPage(rid.pid,
                                             &buf,
                                             m_file->GetFileId());
    if (bufid == INVALID_BUFID) {
        // not a valid page number for this file
        LOG(kError, "record ID %s does not belong to file " FILEID_FORMAT,
                rid.ToString(), m_file->GetFileId());
    }
    //g_bufman->LatchPage(bufid, LatchMode::EX);

    bool erase_result;
    bool is_empty;
    {
        VarlenDataPage dp(buf);
        erase_result = dp.EraseRecord(rid.sid);
        is_empty = dp.GetRecordCount() == 0;
    }

    if (!erase_result) {
        LOG(kError, "record %s not found on page", rid.ToString());
    }

    g_bufman->MarkDirty(bufid);
    if (is_empty) {
        m_file->FreePage(bufid);
    }
}

void
Table::UpdateRecord(RecordId rid, Record &rec) {
    if (!rid.IsValid()) {
        LOG(kError, "invalid record ID");
    }

    char *buf;
    ScopedBufferId bufid = g_bufman->PinPage(rid.pid, &buf,
                                             m_file->GetFileId());
    if (bufid == INVALID_BUFID) {
        LOG(kError, "record ID %s does not belong to file " FILEID_FORMAT,
                    rid.ToString(), m_file->GetFileId());
    }

    bool update_result;
    {
        VarlenDataPage dp(buf);
        update_result = dp.UpdateRecord(rid.sid, rec);
    }

    if (!update_result) {
        LOG(kError, "the new record's length " FIELDOFFSET_FORMAT
                    "is too long to fit on a data page",
                    rec.GetLength());
    }

    g_bufman->MarkDirty(bufid);
    g_bufman->UnpinPage(bufid);
    if (rec.GetRecordId().sid == INVALID_SID) {
        // doesn't fit the current page, insert it to another page
        InsertRecord(rec);
        return ;
    }

    // success, set the pid and we're done
    rec.GetRecordId().pid = rid.pid;
}

Table::Iterator
Table::StartScan() {
    PageNumber first_page = m_file->GetFirstPageNumber();
    return Iterator(this, RecordId{first_page, MinSlotId});
}

Table::Iterator
Table::StartScanFrom(RecordId rid) {
    return Iterator(this, rid);
}

Table::Iterator::Iterator(Table* tbl, RecordId rid):
    m_table(tbl),
    m_cur_record(),
    m_pinned_bufid(),
    m_lastpg_pid(tbl->m_file->GetLastPageNumber()) {

    m_cur_record.GetRecordId() = rid;
    ASSERT(!m_cur_record.IsValid());
}

bool
Table::Iterator::Next() {
    RecordId &rec_id = m_cur_record.GetRecordId();
    char *buf;

    // we're currently at a record, move to at least its next slot
    if (m_cur_record.IsValid()) {
        rec_id.IsValid();
        ASSERT(m_pinned_bufid != INVALID_BUFID);
        ++rec_id.sid;
        buf = g_bufman->GetBuffer(m_pinned_bufid);
        // TODO check if we still hava a latch on the page?
    } else {
        ASSERT(m_pinned_bufid == INVALID_BUFID);
        if (rec_id.pid == INVALID_PID)
            return false;

        m_pinned_bufid = g_bufman->PinPage(rec_id.pid, &buf,
                                           m_table->m_file->GetFileId());
        if (m_pinned_bufid == INVALID_BUFID) {
            LOG(kFatal,
                "wrong page read from a different file other than "
                "the file " FILEID_FORMAT,
                m_table->m_file->GetFileId());
        }
        //g_bufman->LatchPage(m_pinned_bufid, LatchMode::SH);
    }

    // loop until we either reach the end of the file or find some slot
    // that contains a valid record
    ASSERT(rec_id.pid != INVALID_PID);
    ASSERT(rec_id.sid != INVALID_SID);
    ASSERT(m_pinned_bufid != INVALID_BUFID);
    for (;;) {
        // search on the current page
        {
            VarlenDataPage dp(buf);
            SlotId max_sid = dp.GetMaxSlotId();
            ASSERT(rec_id.sid >= dp.GetMinSlotId());
            for (; rec_id.sid <= max_sid; ++rec_id.sid) {
                if (dp.IsOccupied(rec_id.sid)) {
                    m_cur_record.GetData() = dp.GetRecordBuffer(
                        rec_id.sid, &m_cur_record.GetLength());
                    return true;
                }
            }
        }

        // We've depleted this page, move to the next page.
        if (rec_id.pid == m_lastpg_pid) {
            // If we've end up with the last page, just return false even if
            // there are new pages after this.
            rec_id.pid = INVALID_PID;
            rec_id.sid = INVALID_SID;
            return false;
        }
        PageHeaderData *ph = (PageHeaderData *) buf;
        PageNumber next_pid = ph->GetNextPageNumber();
        // The latch is automatically dropped when we drop the pin.
        g_bufman->UnpinPage(m_pinned_bufid);

        rec_id.pid = next_pid;
        rec_id.sid = MinSlotId;
        if (rec_id.pid == INVALID_PID) {
            return false;
        }

        m_pinned_bufid = g_bufman->PinPage(rec_id.pid, &buf);
        if (m_pinned_bufid == INVALID_BUFID) {
            LOG(kFatal,
                "wrong page read from a different file other than "
                "the file " FILEID_FORMAT,
                m_table->m_file->GetFileId());
        }
        //g_bufman->LatchPage(m_pinned_bufid, LatchMode::SH);
    }

    LOG(kFatal, "unreachable");
    return false;
}

void
Table::Iterator::EndScan() {
    m_cur_record.Clear();
    m_pinned_bufid.Reset();
    m_table = nullptr;
    // TODO add your own clean ups of the iterator if necessary
}

}   // namespace taco
