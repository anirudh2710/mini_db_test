#ifndef SORT_EXTERNALSORT_H
#define SORT_EXTERNALSORT_H

#include "tdb.h"

#include <functional>

#include "extsort/ItemIterator.h"
#include "storage/VarlenDataPage.h"
#include "storage/FileManager.h"

namespace taco {

/*!
 * SortCompareFunction compares \p item1 and \p item2 and returns -1 if \p
 * item1 < \p item2, 0 if they equal, or 1 if \p item1 > \p item2. It might be
 * a lambda expression that captures its environment, or an std::bind() over
 * a member function of a class and etc.
 *
 * Note: it does not need length information of items since they are going
 * to be implicitly known by the users through schema and encoded in the function
 * already.
 */
typedef std::function<int(const char *item1, const char *item2)>
        SortCompareFunction;


/*!
 * `ExternalSort` is a general utility class to do external sorting
 * on any number of general bytes. It takes a user-defined comparison
 * function to compare the content of two binary buffers and the memory
 * budget for this external sort operator. After calling `Sort()` function
 * it returns an iterator iterating through sorted items in ascending order.
 */
class ExternalSort {
public:
    /*!
     * Create an `ExternalSort` instance where \p comp provides the method of
     * comparision function of two items. \p merge_ways indicate the memory
     * budget of this external sorting operator. Specifically, the memory budget
     * for this operator should be roughtly (merge_ways + 1) * PAGE_SIZE
     */
    static std::unique_ptr<ExternalSort>
    Create(const SortCompareFunction& comp, size_t merge_ways);

    //! Deconstructor of `ExternalSort`
    ~ExternalSort();

    /*!
     * Given an \p input_iter, it iterates over \p input_iter in one pass, and
     * sorts all the returned items using the SortCompareFunction provided
     * during construction. Returns a ItemIterator over the sorted results.
     *
     * The \p input_iter is allowed to not support rewinding, but the returned
     * output ItemIterator must support rewinding.
     */
    std::unique_ptr<ItemIterator> Sort(ItemIterator *input_iter);

private:

    /*!
     * The output operator that can used to scan through the output of this `ExternalSort`
     * instance in ascending order defined by \p comp.
     */
    class OutputIterator : public ItemIterator {
    public:
        /*!
         * Construct an iterator from the tmp file containing the final
         * sorted items. You also want to remember which is the page to stop
         * for this tmp file.
         */
        OutputIterator(const File* tmpfile,
                       PageNumber final_num_pages);

        //! Deconstructor to clean all the states.
        ~OutputIterator() override;

        /*!
         * Move the iterator to next output record. Returns `true` if there is such
         * a record. The current record of this iterator stores the page and sid
         * information in its state. Even this tmp file is not managed by the buffer
         * manager. You should manage something more like a single-page buffer manager
         * inside the state of this iterator.
         */
        bool Next() override;

        /*!
         * Return the item pointed by this iterator. It returns the buffer location through
         * return value and update \p p_reclen to be the length of this item.
         */
        const char* GetCurrentItem(FieldOffset& p_reclen) override;

        /*!
         * Indicate if this iterator supports rewind. As the output iterator of sorting, it should
         * be true;
         */
        bool SupportsRewind() const override { return true; }

        /*!
         * Save the position of current iterator so that later on can be rewind to this position.
         */
        uint64_t SavePosition() const override {
            // TODO implement it
            return 0;
        }

        /*!
         * Rewind to a particular position saved before. Specifically, if \p pos equals to 0, it
         * rewinds to the state as if the iterator just been constructed.
         *
         * \p pos must be a number returned by `SavePosition()` by the same iterator before. Otherwise,
         * the behavior of this function is undefined.
         */
        void Rewind(uint64_t pos) override;

        /*
         * Clean up state.
         */
        void EndScan() override;
    private:

        // You can add your own states for the iterator here.
    };

    ExternalSort(const SortCompareFunction& comp, size_t merge_ways);

    /*!
     * Iterate through \p item_iterator and create the initial runs based on
     * memory budget. You can use `merge_ways * PAGE_SIZE` bytes for storing
     * the actual record payloads and another array for storing Record
     * structures that points to payloads. Persist it on \p m_file[0] and page
     * boundaries of all sorted runs after this call should be saved in \p
     * m_run_boundaries.
     */
    void SortInitialRuns(ItemIterator* item_iter);

    /*!
     * Generate a new sorted pass through a \p m_merge_ways merge from
     * `m_file[1 - m_current_pass]`. Persist the new sorted runs in
     * `m_file[m_current_pass]` and save the new run boundaries generated after
     * the merge in \p new_run_boudaries.
     */
    void GenerateNewPass(std::vector<PageNumber>& new_run_boundaries);

    /*!
     * Merge the runs \p low_run (inclusive) to \p high_run (inclusive) from
     * last pass whose boundaries are stored in m_run_boundaries. After this,
     * the new merged run should be persistent at the end of
     * `m_file[m_current_pass]` and return the page boundary of newly generated
     * sorted pass through return value.
     */
    PageNumber MergeInternalRuns(size_t low_run, size_t high_run);

    void WriteOutInitialPass(std::vector<Record>& pass);
    void WriteOutRecord(Record& rec);

    //! 0 or 1, indicates which file current pass is using.
    uint8_t m_current_pass;
    //! tmp files used in external sorting
    std::unique_ptr<File> m_file[2];
    /*!
     * how many ways of merge this operator allows, implicitly define memory budget
     * as (m_merge_ways + 1) pages.
     */
    size_t m_merge_ways;
    //! compare functions for sorting items.
    SortCompareFunction m_cmp;
    //! Saves the page boundaries of last pass sorted runs.
    std::vector<PageNumber> m_run_boundaries;

    char* m_inputbuf;
    char* m_outbuf;
    VarlenDataPage m_outpg;
    PageNumber m_output_pos;
};

}   // namespace taco

#endif      // SORT_EXTERNALSORT_H
