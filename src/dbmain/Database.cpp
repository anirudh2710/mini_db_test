// dbmain/Database.cpp
#include "dbmain/Database.h"

#include <absl/flags/flag.h>
#include <absl/strings/str_join.h>

#include "catalog/CatCache.h"
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

    m_initialized = false;
}

void
Database::CreateTable(absl::string_view tabname,
                      std::vector<Oid> coltypid,
                      std::vector<uint64_t> coltypparam,
                      const std::vector<absl::string_view> &field_names,
                      std::vector<bool> colisnullable,
                      std::vector<bool> colisarray) {

    LOG(kFatal, "not available until heap file is implemented");
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
