#include "index/btree/BTreeInternal.h"
#include "catalog/CatCache.h"
#include "storage/FileManager.h"
#include "storage/BufferManager.h"
#include "utils/builtin_funcs.h"

namespace taco {

/*!
 * Checks if the function \p opfuncid is a boolean binary operator over type
 * \p typid.
 */
static void BTreeCheckComparisonOperatorPrototype(Oid typid, Oid opfuncid);

void
BTree::Initialize(const IndexDesc *idxdesc) {
    // check if the comparison functions look good in the index entry
    FieldOffset idxncols = idxdesc->GetIndexEntry()->idxncols();
    for (FieldOffset idxcolid = 0; idxcolid < idxncols; ++idxcolid) {
        Oid idxcoltypid = idxdesc->GetIndexColumnEntry(idxcolid)->idxcoltypid();

        // check if the < operator is set and applies to the key type
        Oid idxcolltfuncid =
            idxdesc->GetIndexColumnEntry(idxcolid)->idxcolltfuncid();
        if (idxcolltfuncid  == InvalidOid) {
            LOG(kError, "can't build BTree index over type " OID_FORMAT
                        " without \"<\" operator",
                        idxdesc->GetKeySchema()->GetFieldTypeId(idxcolid));
        }
        BTreeCheckComparisonOperatorPrototype(idxcoltypid, idxcolltfuncid);

        Oid idxcoleqfuncid =
            idxdesc->GetIndexColumnEntry(idxcolid)->idxcoleqfuncid();
        if (idxcoleqfuncid == InvalidOid) {
            LOG(kError, "can't build BTree index over type " OID_FORMAT
                        " without \"=\" operator",
                        idxdesc->GetKeySchema()->GetFieldTypeId(idxcolid));
        }
        BTreeCheckComparisonOperatorPrototype(idxcoltypid, idxcoleqfuncid);
    }

    //TODO implement
    char* buffer;
    FileId file_id;
    PageNumber pg_no;
    char *meta_buffer;    

    file_id = idxdesc->GetIndexEntry()->idxfid();

    std::unique_ptr<File> f = g_fileman->Open(file_id);
    pg_no = f->AllocatePage();
    
    
    BufferId bufid = g_bufman->PinPage(pg_no, &buffer);

    BTreePageHeaderData::Initialize(buffer, BTREE_PAGE_ISROOT|BTREE_PAGE_ISLEAF, InvalidOid, InvalidOid);
    PageNumber page = f->GetFirstPageNumber();
    
    ScopedBufferId sbuf = g_bufman->PinPage(page, &meta_buffer);
    BTreeMetaPageData::Initialize(meta_buffer,pg_no);
    g_bufman->UnpinPage(sbuf);
    g_bufman->UnpinPage(bufid);
}


std::unique_ptr<BTree>
BTree::Create(std::shared_ptr<const IndexDesc> idxdesc) {
    FileId fid = idxdesc->GetIndexEntry()->idxfid();
    std::unique_ptr<File> f = g_fileman->Open(fid);

    FieldId idxncols = idxdesc->GetIndexEntry()->idxncols();
    std::vector<FunctionInfo> lt_funcs;
    std::vector<FunctionInfo> eq_funcs;
    lt_funcs.reserve(idxncols);
    eq_funcs.reserve(idxncols);
    for (FieldOffset idxcolid = 0; idxcolid < idxncols; ++idxcolid) {
        Oid idxcolltfuncid =
            idxdesc->GetIndexColumnEntry(idxcolid)->idxcolltfuncid();
        FunctionInfo lt_finfo = FindBuiltinFunction(idxcolltfuncid);
        ASSERT((bool) lt_finfo);
        lt_funcs.emplace_back(std::move(lt_finfo));

        Oid idxcoleqfuncid =
            idxdesc->GetIndexColumnEntry(idxcolid)->idxcoleqfuncid();
        FunctionInfo eq_finfo = FindBuiltinFunction(idxcoleqfuncid);
        ASSERT((bool) eq_finfo);
        eq_funcs.push_back(std::move(eq_finfo));
    }

    return absl::WrapUnique(new BTree(std::move(idxdesc), std::move(f),
                                      std::move(lt_funcs),
                                      std::move(eq_funcs)));
}

BTree::BTree(std::shared_ptr<const IndexDesc> idxdesc,
             std::unique_ptr<File> file,
             std::vector<FunctionInfo> lt_funcs,
             std::vector<FunctionInfo> eq_funcs):
    Index(std::move(idxdesc)),
    m_file(std::move(file)),
    m_lt_funcs(std::move(lt_funcs)),
    m_eq_funcs(std::move(eq_funcs)) {
}

BTree::~BTree() {
}

bool
BTree::IsEmpty() {
    // TODO implement it
    char *meta_buf; 
    PageNumber page_no;
    ScopedBufferId meta_bufid;
    ScopedBufferId root_id;
    BTreeMetaPageData* meta_page;
    char* root_buf;

    page_no = m_file->GetFirstPageNumber();   
    meta_bufid = g_bufman->PinPage(page_no, &meta_buf);

    meta_page = (BTreeMetaPageData*)meta_buf;
    
    root_id = g_bufman->PinPage(meta_page->m_root_pid, &root_buf);
    VarlenDataPage varlen(root_buf);

    g_bufman->UnpinPage(meta_bufid);
    g_bufman->UnpinPage(root_id);

    return varlen.GetRecordCount() == 0;
}

uint32_t
BTree::GetTreeHeight() {
    std::vector<PathItem> search_path;
    BufferId bufid = FindLeafPage(nullptr, RecordId(), &search_path);
    g_bufman->UnpinPage(bufid);
    return (uint32_t) search_path.size()+1;
}

void
BTreeCheckComparisonOperatorPrototype(Oid typid, Oid opfuncid) {
    if (!g_db->is_open() || g_catcache == nullptr) {
        return ;
    }
    auto func = g_catcache->FindFunction(opfuncid);
    ASSERT(func.get());
    if (func->funcnargs() != 2 || func->funcrettypid() != initoids::TYP_BOOL) {
        LOG(kError, "function %s is not a binary boolean operator",
                    func->funcname());
    }

    for (int16_t funcargid = 0; funcargid < 2; ++funcargid) {
        auto funcarg = g_catcache->FindFunctionArgs(opfuncid, funcargid);
        ASSERT(funcarg.get());
        if (funcarg->funcargtypid() != typid) {
            LOG(kError, "Argument %d of function %s is %s, but expecting %s",
                        funcargid, func->funcname(),
                        g_catcache->GetTypeName(funcarg->funcargtypid()),
                        g_catcache->GetTypeName(typid));
        }
    }
}

}   // namespace taco

