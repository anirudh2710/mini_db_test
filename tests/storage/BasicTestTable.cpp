#include "base/TDBDBTest.h"

#include "TestTable_common.h"

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>

#include "catalog/BootstrapCatCache.h"
#include "catalog/CatCacheBase.h"
#include "storage/BufferManager.h"
#include "storage/FileManager.h"
#include "utils/builtin_funcs.h"

ABSL_DECLARE_FLAG(std::string, init_data);

namespace taco {

class TestTableWithVolatileCatCache: public TDBDBTest,
                                     public CatCacheInternalAccess {
protected:
    using CatCache = BootstrapCatCache;

    void SetUp() override {
        g_test_no_catcache = true;
        g_test_no_bufman = false;
        TDBDBTest::SetUp();

        BootstrapCatCache catcache;
        catcache.Init();

        std::unique_ptr<File> f_a = g_fileman->Open(NEW_REGULAR_FID);
        ASSERT_NE(f_a.get(), nullptr);
        FileId fid_a = f_a->GetFileId();
        f_a->Close();

        std::shared_ptr<SysTable_Table> tabentry_a(
            ConstructSysTableStruct<SysTable_Table>(
            /*tabid=*/50000,
            /*tabissys=*/false,
            /*tabisvarlen=*/true,
            /*tabncols=*/s_A_ncols,
            /*tabfid=*/fid_a,
            /*tabname = */"A"
        ));
        ASSERT_NE(tabentry_a.get(), nullptr);
        std::unique_ptr<Schema> sch_a =
            absl::WrapUnique(Schema::Create(s_A_coltypids, s_A_coltypparam,
                                            s_A_colisnullable));
        ASSERT_NE(sch_a.get(), nullptr);
        ASSERT_NO_ERROR(sch_a->ComputeLayout(&catcache));

        m_tabdesc_a.reset(TableDesc::Create(std::move(tabentry_a),
                                            std::move(sch_a)));
        ASSERT_NE(m_tabdesc_a.get(), nullptr);

        auto typ = catcache.FindType(initoids::TYP_VARCHAR);
        ASSERT_NE((void*) typ, nullptr);
        m_varchar_infunc = FindBuiltinFunction(typ->typinfunc());
        ASSERT_TRUE((bool) m_varchar_infunc);
    }

    void TearDown() override {
        m_tabdesc_a = nullptr;
        TDBDBTest::TearDown();
    }

    void RecreateTabDescForDBReopen(absl::string_view callsite_file,
                                    uint64_t callsite_lineno) {
    }

    static constexpr bool
    TableInitialized() {
        return false;
    }

    std::shared_ptr<const TableDesc> m_tabdesc_a;
    FunctionInfo m_varchar_infunc;
};

}   // namespace taco

#define TEST_SUITE_NAME BasicTestTable
#define TEST_CLS TestTableWithVolatileCatCache
#include "TestTable_common.inc"

