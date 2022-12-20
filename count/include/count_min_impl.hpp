#ifndef COUNT_MIN_IMPL_HPP_
#define COUNT_MIN_IMPL_HPP_

//#include "count_min.hpp"
namespace datasketches {
count_min_sketch::count_min_sketch(const uint64_t num_hashes):num_hashes(num_hashes){
};

uint64_t count_min_sketch::get_num_hashes() {
    return num_hashes ;
}

} /* namespace datasketches */

#endif