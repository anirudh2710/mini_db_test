#include "extsort/ExternalSort.h"
#include <queue>

namespace taco {

ExternalSort::ExternalSort(const SortCompareFunction& comp, size_t merge_ways)
: m_current_pass(0), m_merge_ways(merge_ways), m_cmp(comp)
, m_inputbuf((char*)aligned_alloc(512, m_merge_ways * PAGE_SIZE))
, m_outbuf((char*)aligned_alloc(512, PAGE_SIZE))
, m_outpg(m_outbuf), m_output_pos(0) {
    m_file[0] = g_fileman->Open(NEW_TMP_FID);
    m_file[1] = g_fileman->Open(NEW_TMP_FID);
}

ExternalSort::~ExternalSort() {
    m_file[0]->Close();
    m_file[1]->Close();
    free(m_inputbuf);
    free(m_outbuf);
}

std::unique_ptr<ExternalSort>
ExternalSort::Create(const SortCompareFunction& comp, size_t merge_ways) {
    return absl::WrapUnique(new ExternalSort(comp, merge_ways));
}

void
ExternalSort::WriteOutRecord(Record& rec) {
    // Write record to the current output page. If there is no space left,
    // write back the current page, and make a new page.

    rec.GetRecordId().sid = INVALID_SID;
    m_outpg.InsertRecordAt(m_outpg.GetMaxSlotId() + 1, rec);
    if (INVALID_SID == rec.GetRecordId().sid) {
        if (m_file[m_current_pass]->GetLastPageNumber() < m_output_pos) {
            m_file[m_current_pass]->AllocatePage();
        }
        m_file[m_current_pass]->WritePage(m_output_pos, m_outbuf);
        ++m_output_pos;
        memset(m_outbuf, 0, PAGE_SIZE);
        m_outpg.InitializePage(m_outbuf);
        m_outpg.InsertRecordAt(m_outpg.GetMaxSlotId() + 1, rec);
        ASSERT(rec.GetRecordId().sid != INVALID_SID);
    }
}

void
ExternalSort::WriteOutInitialPass(std::vector<Record>& pass) {
    memset(m_outbuf, 0, PAGE_SIZE);
    m_outpg.InitializePage(m_outbuf);
    for (auto& item: pass) WriteOutRecord(item);
    if (m_file[m_current_pass]->GetLastPageNumber() < m_output_pos) {
        m_file[m_current_pass]->AllocatePage();
    }
    m_file[m_current_pass]->WritePage(m_output_pos, m_outbuf);
    ++m_output_pos;
}

void
ExternalSort::SortInitialRuns(ItemIterator* input_iter) {
    auto cmp = [this](const Record& a, const Record& b) -> bool  {
        return m_cmp(a.GetData(), b.GetData()) < 0;
    };

    std::vector<Record> pass;
    m_run_boundaries.emplace_back(0);
    m_output_pos = 0;

    size_t buffer_offset = 0;
    const size_t input_buffer_limit = m_merge_ways * PAGE_SIZE;
    while (input_iter->Next()) {
        FieldOffset rec_len;
        const char* cur_rec = input_iter->GetCurrentItem(rec_len);
        if (buffer_offset + rec_len > input_buffer_limit) {
            std::sort(pass.begin(), pass.end(), cmp);
            WriteOutInitialPass(pass);
            pass.clear();
            m_run_boundaries.emplace_back(m_output_pos);
            buffer_offset = 0;
        }
        char* recbuf = m_inputbuf + buffer_offset;
        memcpy(recbuf, cur_rec, rec_len);
        pass.emplace_back(Record(recbuf, rec_len));
        buffer_offset += rec_len;
    }

    if (pass.size() > 0) {
        std::sort(pass.begin(), pass.end(), cmp);
        WriteOutInitialPass(pass);
        m_run_boundaries.emplace_back(m_output_pos);
    }

    input_iter->EndScan();
}

PageNumber
ExternalSort::MergeInternalRuns(size_t low_run, size_t high_run) {
    // TODO: implement it.
    return 0;
}

void
ExternalSort::GenerateNewPass(std::vector<PageNumber>& new_run_boundaries) {
    new_run_boundaries.clear();
    m_output_pos = 0;

    new_run_boundaries.emplace_back(0);
    size_t low_run = 0;
    while (low_run < m_run_boundaries.size() - 1) {
        size_t high_run = std::min(low_run + m_merge_ways - 1, m_run_boundaries.size() - 2);
        new_run_boundaries.emplace_back(MergeInternalRuns(low_run, high_run));
        low_run = high_run + 1;
    }
}

std::unique_ptr<ItemIterator>
ExternalSort::Sort(ItemIterator* input_iterator) {
    // TODO: implement it.
    return nullptr;
}

ExternalSort::OutputIterator::OutputIterator(const File* tmpfile,
                                             PageNumber final_num_pages)
{
    // TODO: implement it.
}

ExternalSort::OutputIterator::~OutputIterator() {
    // TODO: implement it.
}

bool
ExternalSort::OutputIterator::Next() {
    // TODO: implement it.
    return false;
}

const char*
ExternalSort::OutputIterator::GetCurrentItem(FieldOffset& p_reclen) {
    // TODO: implement it.
    return nullptr;
}

void
ExternalSort::OutputIterator::Rewind(uint64_t pos) {
    // TODO: implement it.
}

void
ExternalSort::OutputIterator::EndScan() {
    // TODO: implement it
}

}
