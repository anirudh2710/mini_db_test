#include "execution/TableScanState.h"

namespace taco {

/*!
 * Encodes a record ID as a saved position in TableScanState.
 *
 * Feel free to devise your own encoding scheme. This function is only provided
 * as a hint of how to do so in Taco-DB.
 *
 * You might also want to decide how to denote the boundary cases (i.e., start
 * of scan and end of scan) in the saved positions. Hint: neither INVALID_PID
 * nor RESERVED_PID may appear in any valid record id.
 */
static Datum
RecordIdToTSSSavedPosition(RecordId recid) {
    // PageNumber and SlotId are both unsigned
    return Datum::From(
        (((uint64_t) recid.pid) << (sizeof(recid.sid) << 3)) |
        ((uint64_t) recid.sid));
}

static RecordId
TSSSavedPositionToRecordId(DatumRef d) {
    uint64_t v = d.GetUInt64();
    return RecordId(
        (PageNumber)((v >> (sizeof(SlotId) << 3)) & (~(PageNumber) 0)),
        (SlotId)(v & (~(SlotId) 0)));
}

TableScanState::TableScanState(const TableScan *plan,
                               std::unique_ptr<Table> table)
: PlanExecState(TAG(TableScanState))
, m_plan(plan)
, m_extracted(false), m_at_end(false)
, m_table(std::move(table)), m_tabiter()
, m_fields() {
    auto n_fields = m_table->GetTableDesc()->GetSchema()->GetNumFields();
    m_fields.reserve(n_fields);
    for (int i = 0; i < n_fields; ++i) {
        m_fields.emplace_back(Datum::FromNull());
    }
}

TableScanState::~TableScanState() {
    if (m_initialized) {
        LOG(kWarning, "not closed upon destruction");
    }
}

void
TableScanState::node_properties_to_string(std::string &buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

void
TableScanState::init() {
    ASSERT(!m_initialized);
    m_tabiter = m_table->StartScan();
    m_initialized = true;
    m_extracted = false;
    m_at_end = false;
}

bool
TableScanState::next_tuple() {
    ASSERT(m_initialized);

    // We must check if we are at the end because m_tabiter may not be
    // initialized if we rewinded back to the end of a scan.

    if (m_at_end)
        return false;
    m_extracted = false;
    if (m_tabiter.Next()) {
        return true;
    }

    // Mark the end of a scan so that we won't call m_tabiter again.
    // This is not essential but we can save a call to the iterator if we're
    // called again.
    m_at_end = true;
    return false;
}

std::vector<NullableDatumRef>
TableScanState::get_record() {
    ASSERT(m_initialized);
    ASSERT(m_tabiter.IsAtValidRecord());

    if (!m_extracted) {
        const Schema* sch = m_table->GetTableDesc()->GetSchema();
        const Record &rec = m_tabiter.GetCurrentRecord();
        for (size_t fid = 0; fid < m_fields.size(); ++fid) {
            m_fields[fid] = sch->GetField(fid, rec.GetData());

        }
        m_extracted = true;
    }

    std::vector<NullableDatumRef> record;
    record.reserve(m_fields.size());
    for (size_t fid = 0; fid < m_fields.size(); ++fid) {
        record.emplace_back(NullableDatumRef(m_fields[fid]));
    }

    return record;
}

void
TableScanState::close() {
    ASSERT(m_initialized);
    m_tabiter.EndScan();
    m_initialized = false;
}

void
TableScanState::rewind() {
    ASSERT(m_initialized);
    m_tabiter.EndScan();
    m_tabiter = m_table->StartScan();
    m_extracted = false;
    m_at_end = false;
}

Datum
TableScanState::save_position() const {
    if (m_at_end) {
        // we've finished scan, we use RESERVED_PID to denote that
        return RecordIdToTSSSavedPosition(RecordId(RESERVED_PID, INVALID_SID));
    }


    if (m_tabiter.IsAtValidRecord()) {
        RecordId recid = m_tabiter.GetCurrentRecordId();
        return RecordIdToTSSSavedPosition(recid);
    }

    // we use invalid pid and invalid sid to denote that we haven't
    // started the scan yet
    return RecordIdToTSSSavedPosition(RecordId(INVALID_PID, INVALID_SID));
}

bool
TableScanState::rewind(DatumRef saved_position) {
    m_tabiter.EndScan();
    m_extracted = false;

    RecordId recid = TSSSavedPositionToRecordId(saved_position);
    if (recid.pid == INVALID_PID) {
        m_tabiter = m_table->StartScan();
        m_at_end = false;
    }
    if (recid.pid == RESERVED_PID) {
        // If we were at the end of scan, we do not need to initialize the
        // iterator. It's ok not to initialize it because EndScan may be
        // called for multiple times.
        m_at_end = true;
    }
    // Otherwise, we start from where we saved the position. Note that,
    // we must advance the iterator so that it is on the record where we
    // saved the position.
    m_tabiter = m_table->StartScanFrom(recid);
    bool has_next = m_tabiter.Next();
    if (!has_next) {
        LOG(kFatal, "record %s not found", recid.ToString());
    }
    m_at_end = false;
    return true;
}

}    // namespace taco
