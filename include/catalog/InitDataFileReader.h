#ifndef CATALOG_INITDATAFILEREADER_H
#define CATALOG_INITDATAFILEREADER_H

#include "tdb.h"
#include "catalog/BootstrapCatCache.h"

namespace taco {

enum class InitLineType {
    Eof,
    Error,
    Table,
    Data,
};

class InitDataFileReader {
public:
    InitDataFileReader(
        const std::string &init_datafile,
        BootstrapCatCache *catcache);

    /*!
     * Upon a successful return, rec points to some internal buffer that is
     * valid until the next next() call if this is a data line.
     */
    InitLineType next(char **rec, FieldOffset *rec_size);

    constexpr size_t
    LastLineNo() const {
        return m_lineno;
    }

    constexpr Oid
    LastTabid() const {
        return m_tabid;
    }

    constexpr Schema*
    LastSchema() const {
        return m_schema.get();
    }

private:
    absl::string_view FindNextToken(
        const std::string &str,
        std::string::size_type &start_pos) const;

    std::string m_init_datafile;
    std::ifstream m_input;
    BootstrapCatCache *m_catcache;

    size_t m_lineno;
    InitLineType m_last_state;
    Oid m_tabid;
    maxaligned_char_buf m_buf;

    std::unique_ptr<Schema> m_schema;
    absl::flat_hash_map<Oid, FunctionInfo> m_typ_infuncs;
};

}

#endif  // CATALOG_INITDATAFILERADER_H
