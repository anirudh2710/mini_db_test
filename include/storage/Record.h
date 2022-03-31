// storage/Record.h
#pragma once

#include "tdb.h"

#include <absl/strings/str_format.h>
#include <absl/strings/string_view.h>
#include <iostream>

namespace taco {

class Schema;

/*!
 * The record ID of a record on a page is a pair of `(PageNumber, SlotId)'.
 */
struct RecordId {
    RecordId(): pid(INVALID_PID), sid(INVALID_SID) {}
    RecordId(PageNumber pid) : pid(pid), sid(INVALID_SID) {}
    RecordId(PageNumber pid, SlotId sid): pid(pid), sid(sid) {}

    PageNumber  pid;
    SlotId      sid;

    //! 2-byte padding for alignment, but it's currently unused.
    uint16_t    reserved;

    void
    SetInvalid() {
        pid = INVALID_PID;
        sid = INVALID_SID;
    }

    constexpr bool
    IsValid() const {
        return pid != INVALID_PID && sid != INVALID_SID;
    }

    std::string
    ToString() const {
        return absl::StrFormat("(" PAGENUMBER_FORMAT ", " SLOTID_FORMAT ")",
                                pid, sid);
    }
};

// The size of RecordId should be currently exactly 8 bytes.
static_assert(sizeof(RecordId) == 8);

constexpr inline bool
operator==(const RecordId &rid1, const RecordId &rid2) {
    return rid1.pid == rid2.pid && rid1.sid == rid2.sid;
}

constexpr inline bool
operator!=(const RecordId &rid1, const RecordId &rid2) {
    return rid1.pid != rid2.pid || rid1.sid != rid2.sid;
}

constexpr inline bool
operator<(const RecordId &rid1, const RecordId &rid2) {
    return rid1.pid < rid2.pid || (rid1.pid == rid2.pid && rid1.sid < rid2.sid);
}

constexpr inline bool
operator<=(const RecordId &rid1, const RecordId &rid2) {
    return rid1.pid < rid2.pid || (rid1.pid == rid2.pid &&
                                   rid1.sid <= rid2.sid);
}

constexpr inline bool
operator>(const RecordId &rid1, const RecordId &rid2) {
    return rid1.pid > rid2.pid || (rid1.pid == rid2.pid && rid1.sid > rid2.sid);
}

constexpr inline bool
operator>=(const RecordId &rid1, const RecordId &rid2) {
    return rid1.pid > rid2.pid || (rid1.pid == rid2.pid &&
                                   rid1.sid >= rid2.sid);
}

inline std::ostream&
operator<<(std::ostream &out, const RecordId &rid) {
    out << rid.ToString();
    return out;
}

class Record {
public:
    Record():
        m_buffer(nullptr), m_buflen(0) {}
    Record(const char* buf, FieldOffset buflen):
        m_buffer(buf), m_buflen(buflen) {}
    Record(const maxaligned_char_buf &buf):
        m_buffer(buf.data()), m_buflen(buf.size()) {}

    Record(const Record& other) = default;
    Record& operator=(const Record& other) = default;

    const char*&
    GetData() {
        return m_buffer;
    }

    const char*
    GetData() const {
        return m_buffer;
    }

    FieldOffset&
    GetLength() {
        return m_buflen;
    }

    FieldOffset
    GetLength() const {
        return m_buflen;
    }

    void
    Clear() {
        m_buffer = nullptr;
        m_rid.SetInvalid();
    }

    bool
    IsValid() const {
        return m_buffer != NULL;
    }

    RecordId&
    GetRecordId() {
        return m_rid;
    }

    const RecordId&
    GetRecordId() const {
        return m_rid;
    }

private:
    const char*     m_buffer;
    FieldOffset     m_buflen;
    RecordId        m_rid;
};

}   // namespace taco

namespace std {
    template<> struct hash<taco::RecordId> {
        size_t
        operator()(const taco::RecordId& x) const {
            return std::hash<uint64_t>()(((uint64_t)x.pid << 32) + x.sid);
        }
    };
}
