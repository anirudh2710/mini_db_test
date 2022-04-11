#include "index/btree/BTreeInternal.h"

#include <absl/flags/flag.h>

ABSL_FLAG(bool, btree_enable_rebalancing_pages,
          // disable rebalancing pages by default in case rebalance
          // is not implemented
          false,
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
                                BufferId &bufid,
                                std::vector<PathItem> &search_path) {
    // TODO implement it
    return INVALID_SID;
}

void
BTree::DeleteSlotOnPage(BufferId bufid,
                        SlotId sid) {
    // TODO implement it
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
    }

    if (pg.GetMaxSlotId() < pg.GetMinSlotId()) {
        // the page is empty, just delete it from the parent page
        DeleteSlotOnPage(pbufid, sid);

        // free the deleted child page
        m_file->FreePage(bufid);

        // check if the parent page also needs to be merged/rebalanced
        HandleMinPageUsage(std::move(pbufid), std::move(search_path));
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
BTree::MergePages(BufferId lbufid,
                  BufferId rbufid,
                  BufferId pbufid,
                  SlotId lsid) {
    // TODO implement it

    // Hint: if this function succeeds, you should free the right page. You
    // must do so after unpinning the left page.
    //
    // The reason is that freeing pages requires pinning two additional pages
    // in the file manager. BasicTestBTreeMergePages has exactly 4 buffer
    // frames, so it will complain about no evictable buffer frames if you call
    // FileManager::FreePage() when you have fewer than 2 free frames.
    return false;
}

bool
BTree::RebalancePages(BufferId lbufid,
                      BufferId rbufid,
                      BufferId pbufid,
                      SlotId lsid) {
    // TODO implement it
    return false;
}

}    // namespace taco