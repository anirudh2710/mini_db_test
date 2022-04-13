#ifndef TESTS_EXTSORT_PSEUDORANDOMITEMITERATOR_H
#define TESTS_EXTSORT_PSEUDORANDOMITEMITERATOR_H

#include "base/tdb_test.h"

#include "extsort/ItemIterator.h"

namespace taco {

namespace prii_impl {

template<class T, typename = void>
struct Traits {};

template<class T>
struct Traits<T, typename std::enable_if<std::is_integral<T>::value>::type> {
    static const char*
    GetCurrentItemImpl(const T *x, FieldOffset &p_reclen) {
        p_reclen = sizeof(T);
        return (const char *) x;
    }
};

template<>
struct Traits<std::string, void> {
    static const char*
    GetCurrentItemImpl(const std::string *x, FieldOffset &p_reclen) {
        p_reclen = x->length() + 1;
        return x->c_str();
    }
};

}   // prii_impl

class RandomStringDistribution {
public:
    RandomStringDistribution(size_t min_length, size_t max_length):
        m_min_length(min_length),
        m_max_length(max_length) {}

    template<class RNG>
    std::string
    operator()(RNG &rng) const {
        return GenerateRandomAlphaNumString(rng, m_min_length, m_max_length);
    }

private:
    size_t m_min_length;
    size_t m_max_length;
};

template<class T>
class PseudoRandomItemIterator: public ItemIterator {
public:
    template <class Dist,
              class RNG>
    PseudoRandomItemIterator(Dist dist,
                             RNG rng, size_t n):
        m_i(0),
        m_ended(false) {

        m_items.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            m_items.emplace_back(dist(rng));
        }
    }

    ~PseudoRandomItemIterator() {}

    bool
    Next() override {
        if (m_i < m_items.size()) {
            ++m_i;
            return true;
        }
        return false;
    }

    const char *
    GetCurrentItem(FieldOffset &p_reclen) override {
        return prii_impl::Traits<T>::GetCurrentItemImpl(&m_items[m_i - 1],
                                                        p_reclen);
    }

    bool
    SupportsRewind() const override {
        return true;
    }

    uint64_t
    SavePosition() const override {
        return (uint64_t) m_i;
    }

    void
    Rewind(uint64_t pos) override {
        m_i = (size_t) pos;
    }

    void
    EndScan() override {
        m_ended = true;
    }

    const T *
    GetSortedItems(const std::function<bool(const T&, const T&)> &less) {
        if (!m_ended) {
            // as GetSortedItems destroy the random ordering the items
            LOG(kFatal, "must end scan before getting the sorted items");
        }
        std::sort(m_items.begin(), m_items.end(), less);
        return m_items.data();
    }

private:
    size_t  m_i;

    bool    m_ended;

    std::vector<T>  m_items;
};

extern template class PseudoRandomItemIterator<int64_t>;
extern template class PseudoRandomItemIterator<std::string>;

}   // namespace taco

#endif  // TESTS_EXTSORT_PSEUDORANDOMITEMITERATOR_H
