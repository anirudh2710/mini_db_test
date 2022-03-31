#ifndef UTILS_WALKER_H
#define UTILS_WALKER_H

// This implements the Walker's algorithm.

#include "tdb.h"

#include <random>

namespace taco {

/*!
 * A discrete distribution over non-negative integer set [n] for some n over
 * a given probability distribution.
 *
 * Note that this class does not satisfy RandomNumberDistribution requirement
 * due to missing many required interfaces.
 */
class Walker {
public:
    using result_type = uint32_t;

    // RandomNumberDistribution requires a param_type and a few functions that
    // accept param_type. These are not implemented since we currently don't
    // have use for them.
    //using param_type = /* some struct */;

    /*!
     * Default constructs walker in an invalid state.
     */
    Walker():
        m_N(0) {}

    /*!
     * Constructs a discrete distribution over the integer set [0, prob.size()-
     * 1] with the specified distribution. \p prob needs to be normalized.
     * Otherwise it is undefined.
     */
    Walker(const std::vector<double> &prob);

    Walker(const Walker&) = default;

    Walker(Walker&& other):
        m_N(other.m_N),
        m_IA(std::move(other.m_IA)),
        m_F(std::move(other.m_F)),
        m_unif_a(other.m_unif_a.param()),
        m_unif_b(other.m_unif_b.param()) {
        other.m_N = 0;
    }

    Walker& operator=(const Walker&) = default;

    Walker& operator=(Walker&& other) {
        m_N = other.m_N;
        m_IA = std::move(other.m_IA);
        m_F = std::move(other.m_F);
        m_unif_a.param(other.m_unif_a.param());
        m_unif_b.param(other.m_unif_b.param());
        other.m_N = 0;
        return *this;
    }

    template<typename RNG>
    inline uint32_t
    operator()(RNG &rng) {
        uint32_t IX = m_unif_a(rng);
        double x = m_unif_b(rng);
        if (x >= m_F[IX]) {
            // returns the alias
            return m_IA[IX];
        }
        // otherwise, returns the index itself
        return IX;
    }

private:
    uint32_t               m_N;
    std::vector<uint32_t>  m_IA;
    std::vector<double>    m_F;

    std::uniform_int_distribution<uint32_t> m_unif_a;
    std::uniform_real_distribution<double> m_unif_b;
};

/*
template<typename RNG>
inline uint32_t walker_sample(WalkerParam* param, RNG &rng) {

    uint32_t IX = param->unif_a(rng);
    if (param->unif_b(rng) >= param->F[IX]) {
        return param->IA[IX];
    }
    return IX;
} */

}   // namespace taco

#endif
