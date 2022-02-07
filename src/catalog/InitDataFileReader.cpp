#include "catalog/InitDataFileReader.h"

#include <absl/strings/numbers.h>

#include "utils/builtin_funcs.h"

namespace taco {

InitDataFileReader::InitDataFileReader(
    const std::string &init_datafile,
    BootstrapCatCache *catcache):
    m_init_datafile(init_datafile),
    m_input(init_datafile),
    m_catcache(catcache),
    // any good state will allow us to continue
    m_last_state(InitLineType::Table) {

}

InitLineType
InitDataFileReader::next(char **rec, FieldOffset *rec_size) {
    std::string line;

    /* Once we're in eof or error state, we're there forever. */
    if (m_last_state == InitLineType::Eof ||
        m_last_state == InitLineType::Error) {
        return m_last_state;
    }

    std::getline(m_input, line);
    ++m_lineno;

    // Nothing left, is it eof or something unexpected?
    if (line.empty()) {
        if (m_input.eof()) {
            return m_last_state = InitLineType::Eof;
        }
        return m_last_state = InitLineType::Error;
    }

    std::string::size_type start_pos = 0;
    absl::string_view cur_token = FindNextToken(line, start_pos);
    if (!cur_token.data()) {
        return m_last_state = InitLineType::Error;
    }

    if (cur_token == "data") {
        // data line
        std::vector<Datum> data;
        FieldId i = 0;
        while (start_pos != line.length()) {
            if (i == m_schema->GetNumFields()) {
                return m_last_state = InitLineType::Error;
            }
            cur_token = FindNextToken(line, start_pos);
            if (!cur_token.data()) {
                return m_last_state = InitLineType::Error;
            }

            Datum input_text = Datum::FromVarlenBytes(
                cur_token.data(), cur_token.size());
            uint64_t typparam = m_schema->GetFieldTypeParam(i);
            Oid typid = m_schema->GetFieldTypeId(i);
            FunctionInfo finfo = m_typ_infuncs[typid];
            ASSERT((bool) finfo);
            data.emplace_back(FunctionCallWithTypparam(finfo, typparam,
                                                       input_text));
            if (data.back().isnull()) {
                return m_last_state = InitLineType::Error;
            }
            ++i;
        }

        if (i != m_schema->GetNumFields()) {
            return m_last_state = InitLineType::Error;
        }

        m_buf.clear();
        if (-1 == m_schema->WritePayloadToBuffer(data, m_buf)) {
            return m_last_state = InitLineType::Error;
        }

        *rec = m_buf.data();
        *rec_size = m_buf.size();
        m_last_state = InitLineType::Data;
    } else if (cur_token == "table") {
        // table line
        cur_token = FindNextToken(line, start_pos);
        if (!cur_token.data() ||
            !absl::SimpleAtoi(cur_token, &m_tabid)) {
            return m_last_state = InitLineType::Error;
        }

        std::vector<Oid> typid;
        std::vector<uint64_t> typparam;
        while (start_pos != line.length()) {
            Oid typid_;
            uint64_t typparam_;
            cur_token = FindNextToken(line, start_pos);
            if (!cur_token.data()) {
                return m_last_state = InitLineType::Error;
            }
            if (!absl::SimpleAtoi(cur_token, &typid_)) {
                return m_last_state = InitLineType::Error;
            }
            typid.push_back(typid_);

            cur_token = FindNextToken(line, start_pos);
            if (!cur_token.data()) {
                return m_last_state = InitLineType::Error;
            }
            if (!absl::SimpleAtoi(cur_token, &typparam_)) {
                return m_last_state = InitLineType::Error;
            }
            typparam.push_back(typparam_);
        }

        // create the schema object
        std::vector<bool> nullable(typid.size(), false);
        m_schema = absl::WrapUnique(Schema::Create(typid, typparam, nullable));
        m_schema->ComputeLayout(m_catcache);
        ASSERT(m_schema->IsLayoutComputed());

        // find all the type input funcs
        for (Oid t: typid) {
            const SysTable_Type *typ = m_catcache->FindType(t);
            auto iter = m_typ_infuncs.find(typ->typid());
            if (iter != m_typ_infuncs.end())
                continue;
            FunctionInfo finfo = FindBuiltinFunction(typ->typinfunc());
            m_typ_infuncs[typ->typid()] = finfo;
        }

        m_last_state = InitLineType::Table;
    } else {
        // unknown init data line
        return m_last_state = InitLineType::Error;
    }

    return m_last_state;
}

namespace {
    constexpr size_t parse_limit = 4096;
    char parse_buf[parse_limit];
}

absl::string_view
InitDataFileReader::FindNextToken(
    const std::string &str,
    std::string::size_type &start_pos) const {
    if (start_pos >= str.length()) {
        return absl::string_view();
    }
    const char *cstr = str.c_str();

    std::string::size_type s = start_pos;
    std::string::size_type i = start_pos;
    if (str[i] == '"') {
        // in quoted str literal
        ++i;
        std::string::size_type bufi = 0;
        ++s;
        while (i < str.length()) {
            if (str[i] == '"') {
                if (i + 1 == str.length()) {
                    start_pos = i + 1;
                } else if (str[i + 1] == ' ') {
                    start_pos = i + 2;
                } else {
                    return absl::string_view();
                }
                if (cstr == parse_buf) {
                    return absl::string_view(cstr, bufi);
                } else {
                    return absl::string_view(cstr + s, i - s);
                }
            }

            if (str[i] == '\\') {
                if (i + 1 == str.length()) {
                    return absl::string_view();
                }
                if (str[i + 1] != '\\' && str[i + 1] != '"') {
                    return absl::string_view();
                }

                if (cstr != parse_buf) {
                    if (i - s >= parse_limit) {
                        return absl::string_view();
                    }
                    memcpy(parse_buf, cstr + s, i - s);
                    bufi = i - s;
                    cstr = parse_buf;
                }

                if (bufi >= parse_limit) {
                    return absl::string_view();
                }
                parse_buf[bufi++] = str[i + 1];
                i += 2;
                continue;
            }

            if (cstr == parse_buf) {
                if (bufi >= parse_limit) {
                    return absl::string_view();
                }
                parse_buf[bufi++] = str[i++];
            } else {
                ++i;
            }
        }
    } else {
        // a plain literal without quote
        ++i;
        while (i < str.length() && str[i] != ' ') ++i;
        if (i == str.length()) {
            start_pos = i;
            return absl::string_view(cstr + s, i - s);
        } else {
            start_pos = i + 1;
            return absl::string_view(cstr + s, i - s);
        }
    }

    ASSERT(false); // unreachable
    return absl::string_view();
}



}   // namespace taco
