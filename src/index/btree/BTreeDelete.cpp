#include "index/btree/BTreeInternal.h"

#include <absl/flags/flag.h>

ABSL_FLAG(bool, btree_enable_rebalancing_pages,
          // enable rebalancing pages by default
          true,
          "enable rebalancing btree pages");

namespace taco {

bool
BTree::DeleteKey(const IndexKey *key, RecordId &recid) {
    std::vector<PathItem> search_path;

    BufferId leaf_bufid_ = FindLeafPage(key, recid, &search_path);

    SlotId sid = FindDeletionSlotIdOnLeaf(key, recid, leaf_bufid_, search_path);
    if (sid == INVALID_SID) {
        // no matching key
        recid.SetInvalid();
        g_bufman->UnpinPage(leaf_bufid_);
        return false;
    }

    ScopedBufferId leaf_bufid(leaf_bufid_);

    // Found a matching key. Before deleting it, set recid to its old heap
    // tuple id.
    char *leaf_buf = g_bufman->GetBuffer(leaf_bufid);
    VarlenDataPage leaf_pg(leaf_buf);
    auto recbuf =
        (BTreeLeafRecordHeaderData*) leaf_pg.GetRecordBuffer(sid, nullptr);
    recid = recbuf->m_recid;

    // do the actual deletion
    DeleteSlotOnPage(leaf_bufid, sid);

    // check if the page usage drops below the min threshold and try
    // to rebalance the tree in that case
    HandleMinPageUsage(leaf_bufid.Release(), std::move(search_path));
    return true;
}

SlotId
BTree::FindDeletionSlotIdOnLeaf(const IndexKey *key,
                                const RecordId &recid,
                                BufferId &bufid_,
                                std::vector<PathItem> &search_path) {
    ScopedBufferId bufid(bufid_);
    char *buf = g_bufman->GetBuffer(bufid);

    // Find the largest slot that has a key <= the deletion key.
    SlotId sid = BinarySearchOnPage(buf, key, recid);

    // If recid is invalid, this will be smaller than any slot with a matching
    // key. In that case, we may end up with a page without any matching key --
    // we should check the next page in that case.
    VarlenDataPage pg(buf);
    if (!recid.IsValid()) {
        ++sid;
        if (sid > pg.GetMaxSlotId()) {
            // check the next page is there's one in case there's matching
            // key there
            auto hdr = (BTreePageHeaderData*) pg.GetUserData();
            PageNumber rsibling_pid = hdr->m_next_pid;
            if (rsibling_pid == INVALID_PID) {
                // we don't have a right sibling to the current page
                bufid.Release();
                return INVALID_SID;
            }

            // otherwise, try to find the key on the right sibling
            g_bufman->UnpinPage(bufid);
            bufid = g_bufman->PinPage(rsibling_pid, &buf);
            bufid_ = bufid;
            pg = buf;

            // the matching key if any, must be the first record on the right
            // sibling
            sid = pg.GetMinSlotId();
            ASSERT(sid <= pg.GetMaxSlotId());
            search_path.back().sid += 1;
        }
    } else {
        if (sid == INVALID_SID) {
            // When recid is valid and the binary search returns an invalid
            // sid, this means, we definitely do not have a matching (key,
            // recid) pair on this page, because all pairs > the given (key,
            // recid) pair.
            bufid.Release();
            return sid;
        }
    }

    // when recid is invalid, we need delete any matching key
    // So when doing the comparison, make sure we are using the recid from
    // the tuple.
    Record candidate_rec = pg.GetRecord(sid);
    int res;
    if (recid.IsValid()) {
        res = BTreeTupleCompare(key, recid, candidate_rec.GetData(), true);
    } else {
        // the caller did not specify a specific item, delete anyone that
        // fully matches the key
        auto rechdr = (BTreeLeafRecordHeaderData*) candidate_rec.GetData();
        res = BTreeTupleCompare(key, rechdr->m_recid,
                                candidate_rec.GetData(), true);
    }
    if (res == 0) {
        // found a match
        bufid.Release();
        return sid;
    }

    // no match
    bufid.Release();
    return INVALID_SID;
}

void
BTree::DeleteSlotOnPage(BufferId bufid,
                        SlotId sid) {
    char *buf = g_bufman->GetBuffer(bufid);
    VarlenDataPage pg(buf);
    FieldOffset reclen = pg.GetRecord(sid).GetLength();
    pg.RemoveSlot(sid);
    auto hdr = (BTreePageHeaderData*) pg.GetUserData();
    hdr->m_totrlen -= reclen;
    if (hdr->IsRootPage() && pg.GetMaxSlotId() < pg.GetMinSlotId()) {
        // now the tree is completely empty, the root becomes a leaf page
        hdr->m_flags |= BTREE_PAGE_ISLEAF;
    }
    g_bufman->MarkDirty(bufid);
}


void
BTree::HandleMinPageUsage(BufferId bufid_,
                          std::vector<PathItem> &&search_path) {
    ScopedBufferId bufid(bufid_);
    char *buf = g_bufman->GetBuffer(bufid);
    VarlenDataPage pg(buf);
    auto hdr = (BTreePageHeaderData*) pg.GetUserData();

    if (hdr->IsRootPage()) {
        // For a root page, we should delete it when it only has one single
        // child.
        if (!hdr->IsLeafPage() && pg.GetMaxSlotId() == pg.GetMinSlotId()) {
            do {
                auto rechdr =
                    (BTreeInternalRecordHeaderData*) pg.GetRecordBuffer(
                        pg.GetMinSlotId(), nullptr);
                PageNumber new_root_pid = rechdr->m_child_pid;
                m_file->FreePage(bufid);
                bufid = g_bufman->PinPage(new_root_pid, &buf);
                pg = VarlenDataPage(buf);
                hdr = (BTreePageHeaderData*) pg.GetUserData();
            } while (!hdr->IsLeafPage() &&
                     pg.GetMaxSlotId() == pg.GetMinSlotId());

            PageNumber new_root_pid = g_bufman->GetPageNumber(bufid);
            hdr->m_flags |= BTREE_PAGE_ISROOT;
            g_bufman->MarkDirty(bufid);
            g_bufman->UnpinPage(bufid);

            ScopedBufferId meta_bufid = GetBTreeMetaPage();
            auto meta = (BTreeMetaPageData*) g_bufman->GetBuffer(meta_bufid);
            meta->m_root_pid = new_root_pid;
            g_bufman->MarkDirty(meta_bufid);
            g_bufman->UnpinPage(bufid);
        } else {
            g_bufman->UnpinPage(bufid);
        }
        return ;
    }

    FieldOffset page_usage = BTreeComputePageUsage(
        pg.GetMaxSlotId() - pg.GetMinSlotId() + 1, hdr->m_totrlen);
    if (page_usage >= BTREE_MIN_PAGE_USAGE) {
        // the page usage is above the min threshold
        return ;
    }

    // check if we can merge with the left or right page, which
    // must 1) has the same parent as the current page; and 2) the keys after
    // merge must fit on a page
    ASSERT(!search_path.empty());
    PageNumber parent_pid = search_path.back().pid;
    SlotId sid = search_path.back().sid;
    search_path.pop_back();

    char *pbuf;
    ScopedBufferId pbufid = g_bufman->PinPage(parent_pid, &pbuf);
    VarlenDataPage ppg(pbuf);

    // the parent page on the lowest internal page level may be one page to
    // the left of the actual parent page because we might have moved to
    // the right when searching for the tuple to delete
    if (sid > ppg.GetMaxSlotId()) {
        auto hdr = (BTreePageHeaderData*) ppg.GetUserData();
        parent_pid = hdr->m_next_pid;
        g_bufman->UnpinPage(pbufid);
        pbufid = g_bufman->PinPage(parent_pid, &pbuf);
        ppg = VarlenDataPage(pbuf);
        sid = ppg.GetMinSlotId();
        if (!search_path.empty()) {
            // also update to the sid on the parent's parent page
            search_path.back().sid += 1;
        }
    }

    if (pg.GetMaxSlotId() < pg.GetMinSlotId()) {
        // the page is empty, just delete it from the parent page
        DeleteSlotOnPage(pbufid, sid);

        // free the deleted child page
        m_file->FreePage(bufid);

        // check if the parent page also needs to be merged/rebalanced
        HandleMinPageUsage(pbufid.Release(), std::move(search_path));
        return ;
    }

    // otherwise, try to merge/rebalance the current page with the sibling pages
    bool done = false;
    if (!done && sid < ppg.GetMaxSlotId()) {
        PageNumber rpid = hdr->m_next_pid;
        ASSERT(rpid != INVALID_PID);
        char *rbuf;
        BufferId rbufid = g_bufman->PinPage(rpid, &rbuf);
        BufferId lbufid = bufid.Release();
        if (TryMergeOrRebalancePages(lbufid, rbufid, pbufid, sid)) {
            done = true;
        } else {
            bufid = lbufid;
            g_bufman->UnpinPage(rbufid);
        }
    }

    if (!done && sid > ppg.GetMinSlotId()) {
        PageNumber lpid = hdr->m_prev_pid;
        ASSERT(lpid != INVALID_PID);
        char *lbuf;
        BufferId lbufid = g_bufman->PinPage(lpid, &lbuf);
        BufferId rbufid = bufid.Release();
        if (TryMergeOrRebalancePages(lbufid, rbufid, pbufid, sid - 1)) {
            done = true;
        } else {
            bufid = rbufid;
            g_bufman->UnpinPage(lbufid);
        }
    }

    // The current page's pin may be released at this point. But if not, unpin
    // it here since we won't need it any more.
    if (bufid.IsValid()) {
        g_bufman->UnpinPage(bufid);
    }
    if (done) {
        HandleMinPageUsage(pbufid.Release(), std::move(search_path));
    }
}

bool
BTree::TryMergeOrRebalancePages(BufferId lbufid,
                                BufferId rbufid,
                                BufferId pbufid,
                                SlotId lsid) {
    // Try to merge the pages first. If everything fits into a single page
    // after moving the key in the internal record pointing to the right page
    // and all the index records on the right page, into the left page, we're
    // finished here.
    if (MergePages(lbufid, rbufid, pbufid, lsid)) {
        return true;
    }

    if (absl::GetFlag(FLAGS_btree_enable_rebalancing_pages)) {
        // Otherwise, try to rebalance the pages if we can't merge them and the
        // test enables that. This may also fail because the separator key
        // might be longer than the parent page's current key.
        return RebalancePages(lbufid, rbufid, pbufid, lsid);
    }
    return false;
}

bool
BTree::MergePages(BufferId lbufid_,
                  BufferId rbufid_,
                  BufferId pbufid,
                  SlotId lsid) {
    ScopedBufferId lbufid(lbufid_);
    ScopedBufferId rbufid(rbufid_);

    char *lbuf = g_bufman->GetBuffer(lbufid);
    char *rbuf = g_bufman->GetBuffer(rbufid);
    char *pbuf = g_bufman->GetBuffer(pbufid);
    VarlenDataPage lpg(lbuf);
    VarlenDataPage rpg(rbuf);
    VarlenDataPage ppg(pbuf);
    auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();
    auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
    auto phdr = (BTreePageHeaderData*) ppg.GetUserData();

    // neither the left page or the right page may be empty
    ASSERT(lpg.GetMaxSlotId() >= lpg.GetMinSlotId());
    ASSERT(rpg.GetMaxSlotId() >= rpg.GetMinSlotId());

    // compute the page usage after merging
    SlotId num_recs_left = lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1;
    SlotId num_recs_right = rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1;
    SlotId num_recs_total = num_recs_left + num_recs_right;

    // For leaf page, the record length after merge is simple the total record
    // lengths of both pages. But for an internal page, we also need to pull
    // the key in the internal record pointing to this page into this page,
    // which adds its payload length to the total length.
    FieldOffset totrlen = lhdr->m_totrlen + rhdr->m_totrlen;
    if (!lhdr->IsLeafPage()) {
        FieldOffset preclen = ppg.GetRecord(lsid + 1).GetLength();
        totrlen += preclen - BTreeInternalRecordHeaderSize;
    }
    FieldOffset page_usage_after_merge = BTreeComputePageUsage(num_recs_total,
                                                               totrlen);

    if (page_usage_after_merge > (FieldOffset) PAGE_SIZE) {
        // can't merge because the data after merge won't fit
        lbufid.Release();
        rbufid.Release();
        return false;
    }

    // Do the actual merging below. Move all records on the right page to the
    // left page, and then delete the right page.
    SlotId sid_r = rpg.GetMinSlotId();
    SlotId sid_l = lpg.GetMaxSlotId() + 1;

    if (!lhdr->IsLeafPage()) {
        // For the first key on an internal page, we must copy the key from the
        // internal record pointing to the right page here and update its child
        // page pointer.
        Record prec = ppg.GetRecord(lsid + 1);
        lpg.InsertRecordAt(sid_l, prec);
        if (prec.GetRecordId().sid == INVALID_SID) {
            LOG(kFatal, "unable insert record during merge");
        }
        auto rechdr_l = (BTreeInternalRecordHeaderData*)
            lpg.GetRecordBuffer(sid_l, nullptr);
        auto rechdr_r = (BTreeInternalRecordHeaderData*)
            rpg.GetRecordBuffer(sid_r, nullptr);
        rechdr_l->m_child_pid = rechdr_r->m_child_pid;
        ++sid_r;
        ++sid_l;
    }

    // copy the remaining records on the right page to the left
    for (; sid_r <= rpg.GetMaxSlotId(); ++sid_r, ++sid_l ) {
        Record rec_r = rpg.GetRecord(sid_r);
        lpg.InsertRecordAt(sid_l, rec_r);
        if (rec_r.GetRecordId().sid == INVALID_SID) {
            LOG(kFatal, "unable insert record during merge");
        }
    }

    // update total reclen on the left page
    lhdr->m_totrlen = totrlen;

    // remove the slot for the right page from the parent page
    phdr->m_totrlen -= ppg.GetRecord(lsid + 1).GetLength();
    ppg.RemoveSlot(lsid + 1);

    // update the sibling pointers
    lhdr->m_next_pid = rhdr->m_next_pid;
    if (lhdr->m_next_pid != INVALID_PID) {
        char *old_rbuf;
        ScopedBufferId old_rbufid = g_bufman->PinPage(lhdr->m_next_pid,
                                                      &old_rbuf);
        VarlenDataPage old_rpg(old_rbuf);
        auto old_rhdr = (BTreePageHeaderData*) old_rpg.GetUserData();
        old_rhdr->m_prev_pid = g_bufman->GetPageNumber(lbufid);
        g_bufman->MarkDirty(old_rbufid);
    }

    // mark dirty bits
    g_bufman->MarkDirty(lbufid);
    g_bufman->MarkDirty(pbufid);
    g_bufman->UnpinPage(lbufid);

    // and free the right page, this must be last, see below.
    m_file->FreePage(rbufid);
    return true;
}

bool
BTree::RebalancePages(BufferId lbufid_,
                      BufferId rbufid_,
                      BufferId pbufid,
                      SlotId lsid) {
    ScopedBufferId lbufid(lbufid_);
    ScopedBufferId rbufid(rbufid_);
    char *lbuf = g_bufman->GetBuffer(lbufid);
    char *rbuf = g_bufman->GetBuffer(rbufid);
    char *pbuf = g_bufman->GetBuffer(pbufid);
    VarlenDataPage lpg(lbuf);
    VarlenDataPage rpg(rbuf);
    VarlenDataPage ppg(pbuf);
    auto lhdr = (BTreePageHeaderData*) lpg.GetUserData();
    auto rhdr = (BTreePageHeaderData*) rpg.GetUserData();
    auto phdr = (BTreePageHeaderData*) ppg.GetUserData();

    // neither the left page or the right page may be empty
    ASSERT(lpg.GetMaxSlotId() >= lpg.GetMinSlotId());
    ASSERT(rpg.GetMaxSlotId() >= rpg.GetMinSlotId());

    SlotId num_recs_left = lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1;
    SlotId num_recs_right = rpg.GetMaxSlotId() - rpg.GetMinSlotId() + 1;

    FieldOffset page_usage_left = BTreeComputePageUsage(num_recs_left,
                                                        lhdr->m_totrlen);
    FieldOffset page_usage_right = BTreeComputePageUsage(num_recs_right,
                                                         rhdr->m_totrlen);


    // true for left to right; false for right to left
    bool l2r = page_usage_left > page_usage_right;
    // best_sep_sid is the current best choice of separator sid, such that
    // 1) for left to right case, all records at or after best_sep_sid will be
    // moved to the right; for right to left case, all records at or before
    // best_sep_sid will be moved to the left;
    // 2) the new separator key will fit on the parent page with an in-place
    // update;
    // 3) it brings the page usage of left and right above the minimum;
    // 4) its choice minimizes the difference between the page usages of left
    // and right pages.
    //
    // The default choice is not moving any records.
    SlotId default_sep_sid = l2r ? (lpg.GetMaxSlotId() + 1):
                                   (rpg.GetMinSlotId() - 1);
    SlotId best_sep_sid = default_sep_sid;
    FieldOffset best_diff = l2r ? (page_usage_left - page_usage_right) :
                                  (page_usage_right - page_usage_left);

    int direction = l2r ? -1 : 1;
    FieldOffset totrlen_left = lhdr->m_totrlen;
    // we include the length of the truncated separator key into the right page
    // total record length as well here if we are balancing internal pages
    FieldOffset totrlen_right = rhdr->m_totrlen;
    if (!rhdr->IsLeafPage()) {
        totrlen_right = totrlen_right
            - BTreeInternalRecordHeaderSize
            + ppg.GetRecord(lsid + 1).GetLength();
    }

    SlotId num_recs_parent = ppg.GetMaxSlotId() - ppg.GetMinSlotId() + 1;
    // the total record length of the parent page without counting
    // the internal record pointing to the right page.
    FieldOffset totrlen_parent_no_rrec = phdr->m_totrlen -
        ppg.GetRecord(lsid + 1).GetLength();

    // we also modify num_recs_left and num_recs_right below
    for (SlotId sid = best_sep_sid + direction;
            // we should never empty either one of the pages
            (l2r) ? (sid > lpg.GetMinSlotId()) : (sid < rpg.GetMaxSlotId());
            sid += direction) {
        FieldOffset reclen;

        if (!lhdr->IsLeafPage() && sid == rpg.GetMinSlotId()) {
            // The first record of an internal page does not store the key
            // hence, we should use the record length of parent page instead.
            // This may only happen when we are moving records from right to
            // left.
            ASSERT(!l2r);
            reclen = ppg.GetRecord(lsid + 1).GetLength();
        } else {
            reclen = (l2r ? lpg : rpg).GetRecord(sid).GetLength();
        }

        // compute the new left and right page total reclen
        if (l2r) {
            totrlen_left -= reclen;
            totrlen_right += reclen;
            --num_recs_left;
            ++num_recs_right;
        } else {
            totrlen_right -= reclen;
            totrlen_left += reclen;
            --num_recs_right;
            ++num_recs_left;
        }

        // check if the left page usage is allowable
        FieldOffset new_page_usage_left = BTreeComputePageUsage(
            num_recs_left, totrlen_left);
        if (new_page_usage_left > (FieldOffset) PAGE_SIZE) {
            ASSERT(!l2r);
            // we've moved too many from the right
            break;
        }

        // check if the new first separator key would fit into the parent page
        FieldOffset new_first_reclen_right;
        if (l2r) {
            new_first_reclen_right = reclen;
        } else {
            new_first_reclen_right = rpg.GetRecord(sid + 1).GetLength();
        }
        FieldOffset sepkey_reclen;
        if (lhdr->IsLeafPage()) {
            sepkey_reclen = new_first_reclen_right
                - BTreeLeafRecordHeaderSize
                + BTreeInternalRecordHeaderSize;
        } else {
            sepkey_reclen = new_first_reclen_right;
        }
        FieldOffset new_totrlen_parent = totrlen_parent_no_rrec + sepkey_reclen;
        FieldOffset new_page_usage_parent = BTreeComputePageUsage(
            num_recs_parent, new_totrlen_parent);
        if (new_page_usage_parent > (FieldOffset) PAGE_SIZE) {
            // the new separator key won't fit in the parent
            continue;
        }

        // adjust the actual right page's record length total to exclude the
        // payload portion of its new first record in case it's an internal
        // page
        FieldOffset totrlen_right_actual = totrlen_right -
            (rhdr->IsLeafPage() ? 0 :
             (new_first_reclen_right - BTreeInternalRecordHeaderSize));
        FieldOffset new_page_usage_right = BTreeComputePageUsage(
            num_recs_right, totrlen_right_actual);
        if (new_page_usage_right > (FieldOffset) PAGE_SIZE) {
            ASSERT(l2r);
            // we've moved too many from the left
            break;
        }

        // sid is a valid candidate for sep_sid at this point. Check if this is
        // better than our previous best choice.
        FieldOffset diff = std::abs(new_page_usage_left - new_page_usage_right);
        if (diff < best_diff) {
            // prefer not rebalancing if there's no better choice
            best_diff = diff;
            best_sep_sid = sid;
        } else {
            if (lhdr->IsLeafPage()) {
                // diff function is still convex even for leaf page (even
                // though some sid prior to the min in the function may be
                // invalid).
                break;
            }
        }
    }

    if (best_sep_sid == default_sep_sid) {
        // can't rebalance the pages
        lbufid.Release();
        rbufid.Release();
        return false;
    }

    // Do the actual rebalance with best_sep_sid.
    SlotId n;
    SlotId src_sid, last_src_sid;
    SlotId tgt_sid;
    if (l2r) {
        // for left-to-right move on an internal page, first replace the first
        // record with a complete record from the parent page
        if (!lhdr->IsLeafPage()) {
            auto oldrechdr = (BTreeInternalRecordHeaderData*)
                rpg.GetRecordBuffer(rpg.GetMinSlotId(), nullptr);
            PageNumber child_pid = oldrechdr->m_child_pid;

            // replace the first slot on the right with the parent page
            // internal record
            Record prec = ppg.GetRecord(lsid + 1);
            rpg.UpdateRecord(rpg.GetMinSlotId(), prec);
            if (prec.GetRecordId().sid == INVALID_SID) {
                LOG(kFatal, "failed to update the first record on the right "
                            "page during rebalance");
            }

            // update the child page number in place
            auto newrechdr = (BTreeInternalRecordHeaderData*)
                rpg.GetRecordBuffer(rpg.GetMinSlotId(), nullptr);
            newrechdr->m_child_pid = child_pid;

            rhdr->m_totrlen +=
                prec.GetLength() - BTreeInternalRecordHeaderSize;
        }

        n = lpg.GetMaxSlotId() - best_sep_sid + 1;
        src_sid = best_sep_sid;
        last_src_sid = src_sid + n - 1;
        tgt_sid = rpg.GetMinSlotId();
        ASSERT(n > 0);
        // reserve the slots first for left to right move
        rpg.ShiftSlots(n, false);

        // The first record on the right page should have its key truncated for
        // an internal page.
        if (!lhdr->IsLeafPage()) {
            Record rec = lpg.GetRecord(src_sid++);
            FieldOffset old_reclen = rec.GetLength();
            rec.GetLength() = BTreeInternalRecordHeaderSize;
            rpg.InsertRecordAt(tgt_sid++, rec);
            if (rec.GetRecordId().sid == INVALID_SID) {
                LOG(kFatal, "failed to insert record during rebalance");
            }
            rhdr->m_totrlen += BTreeInternalRecordHeaderSize;

            // do the accounting for left page here since we have moved src_sid
            // past the best_sep_sid
            lhdr->m_totrlen -= old_reclen;
        }
    } else {
        n = best_sep_sid - rpg.GetMinSlotId() + 1;
        ASSERT(n > 0);
        src_sid = rpg.GetMinSlotId();
        last_src_sid = src_sid + n - 1;
        tgt_sid = lpg.GetMaxSlotId() + 1;

        // for right-to-left move, the first record needs to be copied from
        // the parent page if this is an internal page
        if (!lhdr->IsLeafPage()) {
            Record prec = ppg.GetRecord(lsid + 1);
            lpg.InsertRecordAt(tgt_sid, prec);
            if (prec.GetRecordId().sid == INVALID_SID) {
                LOG(kFatal, "failed to insert record during rebalance");
            }
            lhdr->m_totrlen += prec.GetLength();

            // do the accounting here for the right page since we have moved
            // src_sid past the min slot
            rhdr->m_totrlen -= BTreeInternalRecordHeaderSize;

            // update the child pid in place in the copied record on the left
            // page
            auto tgt_rechdr = (BTreeInternalRecordHeaderData*)
                lpg.GetRecordBuffer(tgt_sid, nullptr);
            auto src_rechdr = (BTreeInternalRecordHeaderData*)
                rpg.GetRecordBuffer(src_sid, nullptr);
            tgt_rechdr->m_child_pid = src_rechdr->m_child_pid;
            ++src_sid;
            ++tgt_sid;
        }
    }

    VarlenDataPage src_pg(l2r ? lbuf : rbuf);
    VarlenDataPage tgt_pg(l2r ? rbuf : lbuf);
    auto src_hdr = (BTreePageHeaderData *) src_pg.GetUserData();
    auto tgt_hdr = (BTreePageHeaderData *) tgt_pg.GetUserData();
    for (; src_sid <= last_src_sid; ++src_sid, ++tgt_sid) {
        ASSERT(tgt_sid > tgt_pg.GetMaxSlotId() || !tgt_pg.IsOccupied(tgt_sid));
        Record rec = src_pg.GetRecord(src_sid);
        tgt_pg.InsertRecordAt(tgt_sid, rec);
        if (rec.GetRecordId().sid == INVALID_SID) {
            LOG(kFatal, "failed to insert record during rebalance");
        }
        src_hdr->m_totrlen -= rec.GetLength();
        tgt_hdr->m_totrlen += rec.GetLength();
    }

    // Deduct the old parent tuple's record length from the page header,
    // before we actually update it.
    FieldOffset old_preclen = ppg.GetRecord(lsid + 1).GetLength();
    phdr->m_totrlen -= old_preclen;

    // update the parent tuple and remove the copied slots from the source
    // pages
    if (l2r) {
        // for left-to-right move, the parent tuple's key is still at
        // best_sep_sid on the left page for both leaf and iternal page case
        Record rec = lpg.GetRecord(best_sep_sid);

        if (lhdr->IsLeafPage()) {
            // We have to create a new internal record with a copy of
            // the payload as leaf record and internal record have different
            // header lengths.
            maxaligned_char_buf precbuf;
            CreateInternalRecord(rec, g_bufman->GetPageNumber(rbufid),
                                 true, precbuf);

            Record prec(precbuf);
            ppg.UpdateRecord(lsid + 1, prec);
            if (prec.GetRecordId().sid == INVALID_SID) {
                LOG(kFatal,
                    "failed to update the parent page record during rebalance");
            }
            phdr->m_totrlen += prec.GetLength();
        } else {
            // For internal page, we can just make a copy of the record on the
            // left page and update the child pid in place
            ppg.UpdateRecord(lsid + 1, rec);
            if (rec.GetRecordId().sid == INVALID_SID) {
                LOG(kFatal,
                    "failed to update the parent page record during rebalance");
            }
            phdr->m_totrlen += rec.GetLength();

            // update the parent page child pointer in place
            auto prechdr = (BTreeInternalRecordHeaderData*)
                ppg.GetRecordBuffer(lsid + 1, nullptr);
            prechdr->m_child_pid = g_bufman->GetPageNumber(rbufid);
        }

        // now we can remove all the moved slots on the left page
        while (lpg.GetMaxSlotId() >= best_sep_sid) {
            lpg.RemoveSlot(lpg.GetMaxSlotId());
        }
    } else {
        // for right-to-left move, we can first remove all the moved slots
        rpg.ShiftSlots(n, true);

        // get the new separator key in the internal record pointing to
        // the right page
        Record rec = rpg.GetRecord(rpg.GetMinSlotId());

        // then we need to copy the first key into the parent page as the
        // separator key
        if (lhdr->IsLeafPage()) {
            // leaf page: create a new internal record with a copy
            // of the key and update the parent page
            maxaligned_char_buf precbuf;
            CreateInternalRecord(rec, g_bufman->GetPageNumber(rbufid),
                                 true, precbuf);

            Record prec(precbuf);
            ppg.UpdateRecord(lsid + 1, prec);
            if (prec.GetRecordId().sid == INVALID_SID) {
                LOG(kFatal,
                    "failed to update the parent page record during rebalance");
            }
            phdr->m_totrlen += prec.GetLength();
        } else {
            // internal page: just copy the record on the right page and
            // update the new record in the parent page in place
            ppg.UpdateRecord(lsid + 1, rec);
            if (rec.GetRecordId().sid == INVALID_SID) {
                LOG(kFatal,
                    "failed to update the parent page record during rebalance");
            }
            phdr->m_totrlen += rec.GetLength();

            // update the child pointer in place
            auto prechdr = (BTreeInternalRecordHeaderData*)
                ppg.GetRecordBuffer(lsid + 1, nullptr);
            prechdr->m_child_pid = g_bufman->GetPageNumber(rbufid);

            // and then we can truncate the record on the right page now
            rec = rpg.GetRecord(rpg.GetMinSlotId());
            rhdr->m_totrlen = rhdr->m_totrlen - rec.GetLength() +
                BTreeInternalRecordHeaderSize;
            rec.GetLength() = BTreeInternalRecordHeaderSize;
            rpg.UpdateRecord(rpg.GetMinSlotId(), rec);
            if (rec.GetRecordId().sid == INVALID_SID) {
                LOG(kFatal, "failed to update the first record on the right "
                            "page during rebalance");
            }
        }
    }

    ASSERT(best_diff == std::abs(
        BTreeComputePageUsage(rpg.GetMaxSlotId() - rpg.GetMinSlotId() +1,
                              rhdr->m_totrlen) -
        BTreeComputePageUsage(lpg.GetMaxSlotId() - lpg.GetMinSlotId() + 1,
                              lhdr->m_totrlen)));

    // mark the dirty bits
    g_bufman->MarkDirty(lbufid);
    g_bufman->MarkDirty(rbufid);
    g_bufman->MarkDirty(pbufid);

    // and unpin left and right pages
    g_bufman->UnpinPage(lbufid);
    g_bufman->UnpinPage(rbufid);
    return true;
}

}    // namespace taco
