// dbmain/Database.cpp
#include "dbmain/Database.h"

#include <absl/flags/flag.h>
#include <absl/strings/str_join.h>

#include "catalog/CatCache.h"
#include "storage/BufferManager.h"
#include "storage/FileManager.h"
#include "storage/Table.h"
#include "query/expr/optypes.h"
#include "utils/builtin_funcs.h"
#include "utils/fsutils.h"

ABSL_FLAG(std::string, init_data,
          BUILDDIR "/generated_source/catalog/systables/init.dat",
          "The path to the init data file init.dat");

namespace taco {

static Database s_db_instance;
Database * const g_db = &s_db_instance;

static bool s_init_global_called = false;
bool g_test_no_bufman = false;
bool g_test_no_catcache = false;

bool g_test_no_index = true;

bool g_test_catcache_use_volatiletree = false;

void
Database::init_global() {
    if (s_init_global_called) {
        LOG(kFatal,
            "taco::Database::init_global() must not be called more than once");
    }

    s_init_global_called = true;
    InitBuiltinFunctions();
    InitOpTypes();
}

void
Database::open(const std::string &path,
               size_t bpool_size,
               bool create,
               bool allow_overwrite)
{
    if (!s_init_global_called) {
        LOG(kFatal, "taco::Database::init_global() must be called before "
                    "opening a database");
    }
    if (m_initialized) {
        close();
    }

    m_db_path = path;

    if (allow_overwrite && create) {
        // We don't have to remove the directory if it is empty.
        if (dir_exists(path.c_str()) && !dir_empty(path.c_str())) {
            remove_dir(path.c_str());
        }
        // If the specified path is a file, it does nothing here but the file
        // manager will throw a fatal error.
    }

    m_file_manager = new FileManager();
    m_file_manager->Init(path, 256 * PAGE_SIZE, create);

    if (!g_test_no_bufman) {
        m_buf_manager = new BufferManager();
        m_buf_manager->Init(bpool_size);
    } else {
        m_buf_manager = nullptr;
    }

    if (!g_test_no_catcache) {
        m_catcache = new CatCache();
        if (create) {
            std::string init_data = absl::GetFlag(FLAGS_init_data);
            m_catcache->InitializeFromInitData(init_data);
        } else {
            m_catcache->InitializeFromExistingData();
        }
    } else {
        m_catcache = nullptr;
    }

    m_initialized = true;
}

void
Database::close()
{
    if (m_catcache)
    {
        delete m_catcache;
        m_catcache = nullptr;
    }

    if (m_buf_manager)
    {
        m_buf_manager->Destroy();
        delete m_buf_manager;
        m_buf_manager = nullptr;
    }

    if (m_file_manager)
    {
        delete m_file_manager;
        m_file_manager = nullptr;
    }

    m_initialized = false;
}

void
Database::CreateTable(absl::string_view tabname,
                      std::vector<Oid> coltypid,
                      std::vector<uint64_t> coltypparam,
                      const std::vector<absl::string_view> &field_names,
                      std::vector<bool> colisnullable,
                      std::vector<bool> colisarray) {

    // Create a new file.
    std::unique_ptr<File> f = file_manager()->Open(NEW_REGULAR_FID);

    // Put the table into the system catalog.
    std::vector<std::string> field_names_copy;
    field_names_copy.reserve(field_names.size());
    std::transform(field_names.begin(), field_names.end(),
        std::back_inserter(field_names_copy),
        [](absl::string_view s) -> std::string { return cast_as_string(s); });
    Oid tabid = catcache()->AddTable(tabname, std::move(coltypid),
                                     std::move(coltypparam),
                                     std::move(field_names_copy),
                                     std::move(colisnullable),
                                     std::move(colisarray),
                                     f->GetFileId());

    // Initializes the heap file.
    std::shared_ptr<const TableDesc> tabdesc = catcache()->FindTableDesc(tabid);
    ASSERT(tabdesc.get());
    Table::Initialize(tabdesc.get());
}

void
Database::CreateIndex(absl::string_view idxname,
                      Oid idxtabid,
                      IdxType idxtyp,
                      bool idxunique,
                      std::vector<FieldId> idxcoltabcolids,
                      std::vector<Oid> idxcolltfuncids,
                      std::vector<Oid> idxcoleqfuncids) {
    LOG(kFatal, "not available until btree project");
}

}   // namespace taco
