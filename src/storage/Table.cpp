#include "storage/Table.h"

#include "storage/BufferManager.h"
#include "storage/FileManager.h"
#include "storage/VarlenDataPage.h"

namespace taco {

void
Table::Initialize(const TableDesc *tabdesc) {
    // TODO implement it
}

std::unique_ptr<Table>
Table::Create(std::shared_ptr<const TableDesc> tabdesc) {
    // Hint: construct all private members that may fail here and only pass the
    // successfully constructed objects to the constructor
    //
    // TODO implement it
    return absl::WrapUnique(new Table(std::move(tabdesc)));
}

Table::Table(std::shared_ptr<const TableDesc> tabdesc):
    m_tabdesc(std::move(tabdesc)) {
}

Table::~Table() {
    // TODO implement it
}

void
Table::InsertRecord(Record& rec) {
    // TODO implement it
}

void
Table::EraseRecord(RecordId rid) {
    // TODO implement it
}

void
Table::UpdateRecord(RecordId rid, Record &rec) {
    // TODO implement it
}

Table::Iterator
Table::StartScan() {
    // TODO implement it
    return Iterator();
}

Table::Iterator
Table::StartScanFrom(RecordId rid) {
    // TODO implement it
    return Iterator();
}

Table::Iterator::Iterator(Table* tbl, RecordId rid):
    m_table(tbl),
    m_cur_record(),
    m_pinned_bufid() {
    // TODO implement it
}

bool
Table::Iterator::Next() {
    // TODO implement it
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
