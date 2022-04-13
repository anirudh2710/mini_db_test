#include "base/tdb_test.h"
#include "extsort/PseudoRandomItemIterator.h"

namespace taco {

template class PseudoRandomItemIterator<int64_t>;
template class PseudoRandomItemIterator<std::string>;

}   // namespace taco

