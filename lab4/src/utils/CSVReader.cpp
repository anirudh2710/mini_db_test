#include "utils/CSVReader.h"

#include "catalog/CatCache.h"
#include "utils/builtin_funcs.h"

namespace taco {

CSVReader::CSVReader(bool has_header, char delim):
    m_header_skipped(!has_header),
    m_delim(delim),
    m_eol(true),
    m_in_broken_field(false),
    m_broken_field_in_quote(false),
    m_broken_field_ends_in_quote(false),
    m_buf(nullptr),
    m_bufsz(0),
    m_ipos(0),
    m_lno(0),
    m_llpos(0),
    m_outbuf() {
}

CSVReader::~CSVReader() {}

void
CSVReader::FeedBuffer(char *buf, size_t n) {
    m_llpos = m_llpos - (ptrdiff_t) m_bufsz;
    m_buf = buf;
    m_bufsz = n;
    m_ipos = 0;
}

bool
CSVReader::NextImpl(absl::string_view &field) {
    if (AtEOF()) {
        return false;
    }

    m_eol = false;
    bool in_quote;
    if (m_in_broken_field) {
        in_quote = m_broken_field_in_quote;
        m_in_broken_field = false;
        m_broken_field_in_quote = false;
        if (in_quote && m_broken_field_ends_in_quote) {
            // whether this is the end of last field?
            if (m_buf[m_ipos] == '"') {
                // "" inside quote represents a " character
                m_outbuf.push_back('"');
                ++m_ipos;
            } else {
                // end of the previous quote, the next character must be
                // either the end of the line or a delimiter
                if (m_buf[m_ipos] == m_delim) {
                    ++m_ipos;
                    field = m_outbuf;
                    return true;
                } else if (m_buf[m_ipos] == '\n') {
                    ++m_ipos;
                    ++m_lno;
                    m_llpos = m_ipos;
                    m_eol = true;
                    field = m_outbuf;
                    return true;
                } else {
                    LOG(kError, "%lu:%ld: stray character found after a quoted "
                                "field", (unsigned long)(m_lno + 1),
                                (long)((ptrdiff_t) m_ipos - m_llpos + 1));
                }
            }
        }
    } else {
        if (!m_outbuf.empty()) {
            m_outbuf.clear();
        }
        in_quote = m_buf[m_ipos] == '"';
        if (in_quote) {
            ++m_ipos;
        }
    }

    size_t ipos0 = m_ipos;
    if (in_quote) {
        size_t opos = m_ipos;
        while (m_ipos < m_bufsz) {
            if (m_buf[m_ipos] == '"') {
                ++m_ipos;
                if (m_ipos == m_bufsz) {
                    m_in_broken_field = true;
                    m_broken_field_in_quote = true;
                    m_broken_field_ends_in_quote = true;
                    m_outbuf.append(m_buf + ipos0 , opos - ipos0);
                    return false;
                } else if (m_buf[m_ipos] == m_delim || m_buf[m_ipos] == '\n') {
                    if (m_buf[m_ipos] == '\n') {
                        m_eol = true;
                        ++m_lno;
                        m_llpos = m_ipos + 1;
                    }
                    if (m_outbuf.empty()) {
                        field = absl::string_view(m_buf + ipos0,
                                                  opos - ipos0);
                    } else {
                        m_outbuf.append(m_buf + ipos0 , opos - ipos0);
                        field = m_outbuf;
                    }
                    ++m_ipos;
                    return true;
                } else if (m_buf[m_ipos] == '"') {
                    m_buf[opos] = '"';
                    ++opos;
                    ++m_ipos;
                } else {
                    LOG(kError, "%lu:%ld: stray character found after a quoted "
                                "field", (unsigned long)(m_lno + 1),
                                (long)((ptrdiff_t) m_ipos - m_llpos + 1));
                }
            } else {
                if (opos != m_ipos) {
                    m_buf[opos] = m_buf[m_ipos];
                }
                if (m_buf[m_ipos] == '\n') {
                    ++m_lno;
                    m_llpos = m_ipos + 1;
                }
                ++opos;
                ++m_ipos;
            }
        }

        m_in_broken_field = true;
        m_broken_field_in_quote = true;
        m_broken_field_ends_in_quote = false;
        m_outbuf.append(m_buf + ipos0, opos - ipos0);
        return false;
    } else {
        while (m_ipos < m_bufsz) {
            if (m_buf[m_ipos] != m_delim && m_buf[m_ipos] != '\n') {
                ++m_ipos;
                continue;
            }
            if (m_outbuf.empty()) {
                field = absl::string_view(m_buf + ipos0, m_ipos - ipos0);
            } else {
                m_outbuf.append(m_buf + ipos0, m_ipos - ipos0);
                field = m_outbuf;
            }
            if (m_buf[m_ipos] == '\n') {
                m_eol = true;
                ++m_lno;
                m_llpos = m_ipos + 1;
            }
            ++m_ipos;
            return true;
        }

        m_in_broken_field = true;
        m_broken_field_in_quote = false;
        m_outbuf.append(m_buf + ipos0, m_ipos - ipos0);
        m_ipos = m_bufsz;
        return false;
    }

    ASSERT(false); // unreachable
    return false;
}

FileCSVReader::FileCSVReader(bool has_header, char delim, size_t bufsize):
    CSVReader(has_header, delim),
    m_is_last_file(false),
    m_fsfile(nullptr),
    m_fsize(0),
    m_buf(aligned_alloc(8, bufsize)),
    m_bufsz(bufsize),
    m_schema(nullptr),
    m_data(),
    m_recbuf() {
}

void
FileCSVReader::FeedFile(absl::string_view filename, bool is_last_file) {
    m_fsfile = absl::WrapUnique(
        FSFile::Open(cast_as_string(filename), false, false, false));
    if (!m_fsfile) {
        if (errno == 0) {
            LOG(kError, "unable to open file %s: unknown reason", filename);
        } else {
            LOG(kError, "unable to open file %s: %s",
                filename, strerror(errno));
        }
    }
    m_fsize = m_fsfile->Size();
    m_nbytes_read = 0;
    m_is_last_file = is_last_file;
}

bool
FileCSVReader::Next(absl::string_view &field) {
    for (;;) {
        if (AtEOF()) {
            if (!m_fsfile)
                return false;
            size_t count = std::min(m_bufsz, m_fsize - m_nbytes_read);
            if (count == 0) {
                m_fsfile->Close();
                m_fsfile = nullptr;
                if (m_is_last_file && !AtEOL()) {
                    // We need to append the newline to the file if it is not
                    // the last character. The newline at the end of the file
                    // is needed for CSVReader to consider the last field and
                    // the last line as complete.
                    ((char*) m_buf.get())[0] = '\n';
                    FeedBuffer((char*) m_buf.get(), 1);
                    continue;
                }

                return false;
            }
            m_fsfile->Read(m_buf.get(), count, m_nbytes_read);
            m_nbytes_read += count;
            FeedBuffer((char*) m_buf.get(), count);
        }

        if (CSVReader::Next(field)) {
            return true;
        }
    }

    ASSERT(false); // unreachable
    return false;
}

void
FileCSVReader::SetSchema(const Schema *sch) {
    if (m_schema) {
        LOG(kFatal, "can't be called more than once");
    }
    m_schema = sch;
    m_data.clear();
    m_data.reserve(sch->GetNumFields());
    m_infuncs.clear();
    m_infuncs.reserve(sch->GetNumFields());
    for (FieldId i = 0; i < sch->GetNumFields(); ++i) {
        Oid typid = sch->GetFieldTypeId(i);
        auto typ = g_catcache->FindType(typid);
        if (!typ) {
            LOG(kFatal, "type id " OID_FORMAT " does not exist", typid);
        }
        Oid typinfunc = typ->typinfunc();
        if (typinfunc == InvalidOid) {
            LOG(kFatal, "type %s can't be converted from string input",
                        typ->typname());
        }
        m_infuncs.emplace_back(FindBuiltinFunction(typinfunc));
    }
}

const std::vector<Datum> *
FileCSVReader::NextDeserializedRecord() {
    if (AtEOL()) {
        // last line was complete and returned; start a new line
        m_data.clear();
    }
    do {
        absl::string_view field_str;
        if (!Next(field_str)) {
            if (m_is_last_file) {
                ASSERT(AtEOL());
                break;
            } else {
                // not done yet; the caller should supply the next file
                return nullptr;
            }
        }

        // parse the field
        if (m_data.size() >= (size_t) m_schema->GetNumFields()) {
            if (!field_str.empty()) {
                LOG(kError, "line %lu has more fields than " FIELDOFFSET_FORMAT,
                            AtEOL() ? (GetCurLineNumber() - 1) : GetCurLineNumber(),
                            m_schema->GetNumFields());
            } else {
                // Allow skipping trailing empty fields after the last valid
                // field.  (e.g., TPC-H adds trailing '|' to each line and thus
                // its data files have one extra empty field than the
                // corresponding table schemas do).
                continue;
            }
        }

        FieldId i = m_data.size();
        Datum field_str_d = Datum::FromVarlenAsStringView(field_str);
        m_data.emplace_back(FunctionCallWithTypparam(m_infuncs[i],
            m_schema->GetFieldTypeParam(i), field_str_d));

    } while (!AtEOL());

    if (!m_data.empty() && m_data.size() != (size_t) m_schema->GetNumFields()) {
        LOG(kError, "line %lu has %lu fields but the schema expects "
                    FIELDOFFSET_FORMAT, GetCurLineNumber() - 1,
                    (unsigned long) m_data.size(), m_schema->GetNumFields());
    }
    return &m_data;
}

Record
FileCSVReader::NextSerializedRecord() {
    m_recbuf.clear();
    if (!NextDeserializedRecord()) {
        return Record();
    }
    if (m_data.empty()) {
        return Record();
    }
    if (-1 == m_schema->WritePayloadToBuffer(m_data, m_recbuf)) {
        LOG(kError, "line %lu: unable to write record to buffer",
                    GetCurLineNumber() - 1);
    }
    return Record(m_recbuf);
}

}   // namespace taco

