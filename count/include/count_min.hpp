#ifndef COUNT_MIN_HPP_
#define COUNT_MIN_HPP_

#include <cstdint>
#include <vector>

namespace datasketches {

class count_min_sketch{
public:
  uint64_t num_hashes ;
  count_min_sketch(uint64_t num_hashes) ;

  // Getters
  uint64_t get_num_hashes() ;
};

} /* namespace datasketches */

#include "count_min_impl.hpp"

#endif