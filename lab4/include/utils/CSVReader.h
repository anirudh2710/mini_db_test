#ifndef UTILS_CSVREADER_H
#define UTILS_CSVREADER_H

#include "tdb.h"

#include "catalog/Schema.h"
#include "storage/FSFile.h"
#include "storage/Record.h"

namespace taco {

class CSVReader {
public:
    /*!
     * has_header currently only has the effect of skipping the first line.
     */
    CSVReader(bool has_header = false, char delim = ',');

    ~CSVReader();

    void FeedBuffer(char *buf, size_t n);

    bool
    Next(absl::string_view &field) {
        if (m_header_skipped)
            return NextImpl(field);
        while (NextImpl(field)) {
            if (AtEOL()) {
                m_header_skipped = true;
                return NextImpl(field);
            }
        }
        return false;
    }

    bool
    AtEOL() const {
        return m_eol;
    }

    bool
    AtEOF() const {
        return m_ipos == m_bufsz;
    }

    size_t
    GetCurLineNumber() const {
        return m_lno + 1;
    }

    size_t
    GetCurCharNumber() const {
        return m_ipos - m_llpos + 1;
    }

private:
    bool NextImpl(absl::string_view &field);

    bool          m_header_skipped;

    const char    m_delim;

    bool          m_eol;

    bool          m_in_broken_field;

    bool          m_broken_field_in_quote;

    bool          m_broken_field_ends_in_quote;

    char          *m_buf;

    size_t        m_bufsz;

    size_t        m_ipos;

    size_t        m_lno;

    ptrdiff_t     m_llpos;

    std::string   m_outbuf;
};

class FileCSVReader: public CSVReader {
public:
    FileCSVReader(bool has_header = false,
                  char delim = ',',
                  size_t bufsize = (size_t) 65536);

    void FeedFile(absl::string_view filename, bool is_last_file = true);

    bool Next(absl::string_view &field);

    /*!
     * May only be called once.
     */
    void SetSchema(const Schema *sch);

    /*!
     * The returned vector points to an internal buffer. This function only
     * returns a non-null pointer when a complete line is read.
     */
    const std::vector<Datum> *NextDeserializedRecord();

    /*!
     * The returned record points to an internal buffer.
     */
    Record NextSerializedRecord();

private:
    bool                    m_is_last_file;

    std::unique_ptr<FSFile> m_fsfile;

    size_t                  m_fsize;

    size_t                  m_nbytes_read;

    unique_malloced_ptr     m_buf;

    const size_t            m_bufsz;

    const Schema            *m_schema;

    std::vector<FunctionInfo> m_infuncs;

    std::vector<Datum>      m_data;

    maxaligned_char_buf     m_recbuf;
};

}

#endif      // UTILS_CSVREADER_H
