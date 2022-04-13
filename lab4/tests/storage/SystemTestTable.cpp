#include "base/TDBDBTest.h"

#include "storage/TestTable_common.h"

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>

#include "catalog/CatCache.h"
#include "storage/BufferManager.h"
#include "storage/FileManager.h"
#include "utils/builtin_funcs.h"

ABSL_DECLARE_FLAG(std::string, init_data);

namespace taco {

class TestTableWithPersistentCatCache: public TDBDBTest {
protected:
    void SetUp() override {
        g_test_no_catcache = false;
        g_test_no_bufman = false;
        TDBDBTest::SetUp();

        ASSERT_NO_ERROR(g_db->CreateTable("A",
                                          s_A_coltypids,
                                          s_A_coltypparam,
                                          /*field_names=*/{},
                                          s_A_colisnullable));
        RecreateTabDescForDBReopen(__FILE__, __LINE__);

        auto typ = g_catcache->FindType(initoids::TYP_VARCHAR);
        ASSERT_NE(typ.get(), nullptr);
        m_varchar_infunc = FindBuiltinFunction(typ->typinfunc());
        ASSERT_TRUE((bool) m_varchar_infunc);
    }

    void TearDown() override {
        TDBDBTest::TearDown();
    }

    void RecreateTabDescForDBReopen(absl::string_view callsite_file,
                                    uint64_t callsite_lineno) {
        Oid tabid = g_catcache->FindTableByName("A");
        ASSERT_NE(tabid, InvalidOid)
            << "failed to find table A from the catalog. Note called from "
            << callsite_file << ":" << callsite_lineno;
        m_tabdesc_a = g_catcache->FindTableDesc(tabid);
        ASSERT_NE(m_tabdesc_a.get(), nullptr)
            << "failed to find the table descriptor of table A. "
               "Note called from "
            << callsite_file << ":" << callsite_lineno;
    }

    static constexpr bool
    TableInitialized() {
        return true;
    }

    std::shared_ptr<const TableDesc> m_tabdesc_a;
    FunctionInfo m_varchar_infunc;
};

}   // namespace taco

#define TEST_SUITE_NAME SystemTestTable
#define TEST_CLS TestTableWithPersistentCatCache
#include "TestTable_common.inc"

