#include "index/btree/BTreeInternal.h"

namespace taco {

bool
BTree::InsertKey(const IndexKey *key, RecordId recid) {
    // find the leaf page to insert into
    std::vector<PathItem> search_path;
    bool idxunique = GetIndexDesc()->GetIndexEntry()->idxunique();

    // If this is a unique index, use the +inf record id instead. If there is a
    // duplicate key (but not duplicate record id) that happens to be the
    // separator key of two sibling internal pages, we will descend into right
    // instead of left and will end up with a page that contains the duplicat
    // key. If there's not, we will end up with a page that also covers the
    // pair to be inserted.
    ScopedBufferId leaf_bufid = FindLeafPage(key,
                                idxunique ? INFINITY_RECORDID : recid,
                                &search_path);

    // find the slot to insert into and check whether there's a duplicate if
    // this index is a unique index
    SlotId sid = FindInsertionSlotIdOnLeaf(key, recid, leaf_bufid);
    if (sid == INVALID_SID) {
        // duplicate key
        return false;
    }

    // construct the index record
    maxaligned_char_buf leaf_recbuf;
    CreateLeafRecord(key, recid, leaf_recbuf);
    if (leaf_recbuf.size() > BTREE_MAX_RECORD_SIZE) {
        LOG(kError, "key is too long for B-tree index: expecting <= "
                    FIELDOFFSET_FORMAT, ", but got " FIELDOFFSET_FORMAT,
                    BTREE_MAX_RECORD_SIZE, leaf_recbuf.size());
    }

    // do the actual insertion
    InsertRecordOnPage(leaf_bufid.Release(),
                       sid,
                       std::move(leaf_recbuf),
                       std::move(search_path));
    return true;
}

SlotId
BTree::FindInsertionSlotIdOnLeaf(const IndexKey *key,
                                 const RecordId &recid,
                                 BufferId bufid) {
    bool idxunique = GetIndexDesc()->GetIndexEntry()->idxunique();
    char *buf = g_bufman->GetBuffer(bufid);

    // Find the smallest slot id such that the key in that slot is greater than
    // or equal to the inserted \p key. This is where we want to insert our
    // record before. For a unique index, we should use the +inf record id
    // instead, so that we can find a leaf record that has a equal key but
    // unequal recid.
    SlotId sid = BinarySearchOnPage(buf, key,
                                    idxunique ? INFINITY_RECORDID : recid);

    VarlenDataPage pg(buf);
    if (sid != INVALID_SID) {
        // otherwise, check if the last slot <= this key is a duplicate. If
        // not we can insert the key into this slot.
        if (idxunique && !key->HasAnyNull()) {
            // This is a unique index, check for duplicate key value only.
            char *recbuf = pg.GetRecordBuffer(sid, nullptr);
            auto rechdr = (BTreeLeafRecordHeaderData*) recbuf;
            // use the heap record id in the leaf record so that we are
            // just comparing the keys
            int res = BTreeTupleCompare(key, rechdr->m_recid,
                                        recbuf, /*isleaf=*/true);

            ASSERT(res >= 0);
            if (res == 0) {
                // duplicate found
                return INVALID_SID;
            }
        } else {
            // For a non-unique index, we still need to guarantee that there're
            // no duplicate (key, recid) pairs.
            char *recbuf = pg.GetRecordBuffer(sid, nullptr);
            int res = BTreeTupleCompare(key, recid, recbuf, true);
            if (res == 0) {
                // duplicate found
                return INVALID_SID;
            }
        }
    }

    return sid + 1;
}

void
BTree::InsertRecordOnPage(BufferId bufid_,
                          SlotId insertion_sid,
                          maxaligned_char_buf &&recbuf,
                          std::vector<PathItem> &&search_path) {
    ScopedBufferId bufid(bufid_);
    char *buf = g_bufman->GetBuffer(bufid);
    VarlenDataPage pg(buf);
    auto hdr = (BTreePageHeaderData*) pg.GetUserData();

    // try insertion
    Record rec(recbuf);
    bool modified = pg.InsertRecordAt(insertion_sid, rec);
    if (modified) {
        g_bufman->MarkDirty(bufid);
    }
    if (rec.GetRecordId().sid != INVALID_SID) {
        // done
        hdr->m_totrlen += rec.GetLength();
        return ;
    }

    // the page is full, need a split. Save the root flag before calling
    // SplitPage() which clears that flag.
    bool is_root = hdr->IsRootPage();
    recbuf = SplitPage(bufid, insertion_sid, std::move(recbuf));

    if (is_root) {
        // it was a root split
        ASSERT(search_path.empty());
        CreateNewRoot(bufid.Release(), std::move(recbuf));
        return ;
    }

    g_bufman->UnpinPage(bufid);
    ASSERT(!search_path.empty());
    auto parent_pid = search_path.back().pid;
    auto parent_sid = search_path.back().sid;
    search_path.pop_back();
    bufid = g_bufman->PinPage(parent_pid, &buf);
    InsertRecordOnPage(bufid.Release(),
                       parent_sid + 1,
                       std::move(recbuf),
                       std::move(search_path));
}

void
BTree::CreateNewRoot(BufferId bufid_,
                     maxaligned_char_buf &&recbuf) {
    ScopedBufferId bufid(bufid_);
    // unset the flag on the old root
    char *buf = g_bufman->GetBuffer(bufid);
    VarlenDataPage pg(buf);

    g_bufman->MarkDirty(bufid);
    PageNumber oldroot_pid = g_bufman->GetPageNumber(bufid);
    g_bufman->UnpinPage(bufid);

    ScopedBufferId root_bufid = CreateNewBTreePage(true,
                                                   false,
                                                   INVALID_PID,
                                                   INVALID_PID);
    char *root_buf = g_bufman->GetBuffer(root_bufid);
    PageNumber root_pid = g_bufman->GetPageNumber(root_bufid);
    VarlenDataPage root_pg(root_buf);
    auto root_hdr = (BTreePageHeaderData*) root_pg.GetUserData();

    BTreeInternalRecordHeaderData first_root_rechdr;
    first_root_rechdr.m_child_pid = oldroot_pid;
    Record first_root_rec((char*) &first_root_rechdr,
                          sizeof(first_root_rechdr));
    root_pg.InsertRecordAt(root_pg.GetMinSlotId(), first_root_rec);
    if (first_root_rec.GetRecordId().sid == INVALID_SID) {
        LOG(kFatal, "failed to insert record into new B-tree root");
    }
    root_hdr->m_totrlen += first_root_rec.GetLength();

    Record second_root_rec(recbuf);
    root_pg.InsertRecordAt(root_pg.GetMinSlotId() + 1, second_root_rec);
    if (second_root_rec.GetRecordId().sid == INVALID_SID) {
        LOG(kFatal, "failed to insert record into new B-tree root");
    }
    root_hdr->m_totrlen += second_root_rec.GetLength();

    g_bufman->MarkDirty(root_bufid);
    g_bufman->UnpinPage(root_bufid);

    // update meta page root pointer
    ScopedBufferId meta_bufid = GetBTreeMetaPage();
    char *meta_buf = g_bufman->GetBuffer(meta_bufid);
    auto meta = (BTreeMetaPageData*) meta_buf;
    meta->m_root_pid = root_pid;
    g_bufman->MarkDirty(meta_bufid);
}

maxaligned_char_buf
BTree::SplitPage(BufferId bufid,
                 SlotId insertion_sid,
                 maxaligned_char_buf &&recbuf) {
    char *buf = g_bufman->GetBuffer(bufid);
    PageNumber pid = g_bufman->GetPageNumber(bufid);
    VarlenDataPage pg(buf);
    auto hdr = (BTreePageHeaderData*) pg.GetUserData();

    ASSERT(MAXALIGN(recbuf.size()) == recbuf.size());

    // find split point
    FieldOffset totrlen = hdr->m_totrlen + recbuf.size();
    // imagine insertion_sid has been inserted and all slots after it
    // has been shifted to the right
    const SlotId max_sid = pg.GetMaxSlotId() + 1;

    // Our goal is to find such a point such that, all sid <= split_sid are
    // on the left, and all sid > split_sid are on the right. They both fit
    // into an empty page, and we want to minimize the difference between the
    // free spaces.
    FieldOffset totrlen_left = 0;
    FieldOffset best_page_usage_diff = PAGE_SIZE;
    SlotId best_split_sid = INVALID_SID;
    for (SlotId split_sid = pg.GetMinSlotId(); split_sid < max_sid;
         ++split_sid) {
        FieldOffset reclen;
        if (split_sid < insertion_sid) {
            reclen = pg.GetRecord(split_sid).GetLength();
        } else if (split_sid == insertion_sid) {
            reclen = recbuf.size();
        } else {
            reclen = pg.GetRecord(split_sid - 1).GetLength();
        }

        totrlen_left += reclen;
        FieldOffset page_usage_left = BTreeComputePageUsage(
            split_sid - pg.GetMinSlotId() + 1, totrlen_left);
        if (page_usage_left > (FieldOffset) PAGE_SIZE) {
            continue;
        }

        // to compute the right page's usage, subtract the first record's
        // payload size
        FieldOffset page_usage_right;
        if (hdr->IsLeafPage()) {
            FieldOffset totrlen_right = totrlen - totrlen_left;
            page_usage_right = BTreeComputePageUsage(
                max_sid - split_sid, totrlen_right);
        } else {
            FieldOffset first_reclen_on_right;
            if (split_sid + 1 < insertion_sid) {
                first_reclen_on_right = pg.GetRecord(split_sid + 1).GetLength();
            } else if (split_sid + 1 == insertion_sid) {
                first_reclen_on_right = recbuf.size();
            } else {
                first_reclen_on_right = pg.GetRecord(split_sid).GetLength();
            }
            FieldOffset totrlen_right = totrlen - totrlen_left
                - first_reclen_on_right +
                BTreeInternalRecordHeaderSize;
            page_usage_right = BTreeComputePageUsage(
                max_sid - split_sid, totrlen_right);
        }
        if (page_usage_right > (FieldOffset) PAGE_SIZE) {
            continue;
        }

        FieldOffset diff = std::abs(page_usage_left - page_usage_right);
        if (diff <= best_page_usage_diff) {
            best_page_usage_diff = diff;
            best_split_sid = split_sid;
            continue;
        } else {
            if (hdr->IsLeafPage()) {
                // The difference should be a V-shaped function for leaf page,
                // so if adding the item at split_sid would make the difference
                // higher, the last one is the best solution.
                break;
            } else {
                // For an internal page, the difference may not be monotonic.
                // So we can't break out of the loop early.
                continue;
            }
        }
    }

    if (best_split_sid <= pg.GetMinSlotId() || best_split_sid == max_sid) {
        LOG(kFatal, "failed to find a split point for B-tree");
    }

    // Create a new page
    ScopedBufferId rbufid = CreateNewBTreePage(false,
                                               hdr->IsLeafPage(),
                                               pid,
                                               hdr->m_next_pid);
    PageNumber rpid = g_bufman->GetPageNumber(rbufid);
    char *rbuf = g_bufman->GetBuffer(rbufid);
    VarlenDataPage rpg(rbuf);
    auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();

    // copy records after best_split_sid to the right
    maxaligned_char_buf new_internal_recbuf;
    FieldOffset rlen_of_first_key_moved_to_right = 0;
    for (SlotId sid = best_split_sid + 1; sid <= max_sid; ++sid) {
        Record rec;
        if (sid < insertion_sid) {
            rec = pg.GetRecord(sid);
        } else if (sid == insertion_sid) {
            rec = Record(recbuf);
        } else {
            rec = pg.GetRecord(sid - 1);
        }
        if (sid == best_split_sid + 1) {
            // We never need to store the key for the first record on an
            // internal page, so truncate away the payload part.
            // But we do have to make a copy of the first key, as an internal
            // record pointing to the right page.
            CreateInternalRecord(rec, rpid, hdr->IsLeafPage(),
                                 new_internal_recbuf);
            if (!hdr->IsLeafPage()) {
                rlen_of_first_key_moved_to_right = rec.GetLength() -
                    BTreeInternalRecordHeaderSize;
                rec.GetLength() = BTreeInternalRecordHeaderSize;
            }
        }
        (void) rpg.InsertRecordAt(rpg.GetMaxSlotId() + 1, rec);
        if (rec.GetRecordId().sid == INVALID_SID) {
            LOG(kFatal, "unable move index records to the right page of a "
                        "split page");
        }
        rhdr->m_totrlen += rec.GetLength();
    }

    // erase the records moved to the right from the left page
    // Doing this backwards will avoid moving the payloads on the page.
    for (SlotId sid = max_sid; sid > best_split_sid; --sid) {
        if (sid < insertion_sid) {
            pg.RemoveSlot(sid);
        } else if (sid > insertion_sid) {
            pg.RemoveSlot(sid - 1);
        }
    }

    // insert the new record if it falls on the left page (if it's on the
    // right, it's already inserted in a previous loop).
    if (insertion_sid <= best_split_sid) {
        Record rec(recbuf);
        pg.InsertRecordAt(insertion_sid, rec);
        if (rec.GetRecordId().sid == INVALID_SID) {
            LOG(kFatal, "failed to insert the new record after page split");
        }
    }

    hdr->m_totrlen = totrlen - rhdr->m_totrlen
        - rlen_of_first_key_moved_to_right;

    // update the left page pointer of the old right page if any
    if (hdr->m_next_pid != INVALID_PID) {
        char *old_rbuf;
        ScopedBufferId old_rbufid = g_bufman->PinPage(hdr->m_next_pid,
                                                      &old_rbuf);
        VarlenDataPage old_rpg(old_rbuf);
        auto old_rhdr = (BTreePageHeaderData *) old_rpg.GetUserData();
        old_rhdr->m_prev_pid = rpid;
        g_bufman->MarkDirty(old_rbufid);
    }

    // now we can update the left page's next pointer and its root flag if any
    hdr->m_next_pid = rpid;
    hdr->m_flags &= ~BTREE_PAGE_ISROOT;

    // and mark both pages dirty
    g_bufman->MarkDirty(bufid);
    g_bufman->MarkDirty(rbufid);
    return new_internal_recbuf;
}

}   // namespace taco
