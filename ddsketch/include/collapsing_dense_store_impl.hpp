//
// Created by geonove on 6/28/25.
//

#ifndef COLLAPSING_DENSE_STORE_IMPL_HPP
#define COLLAPSING_DENSE_STORE_IMPL_HPP

#include "collapsing_dense_store.hpp"

namespace datasketches {
template<typename Allocator>
CollapsingDenseStore<Allocator>::CollapsingDenseStore(size_type max_num_bins) : max_num_bins(max_num_bins), is_collapsed(false) {}

template<typename Allocator>
typename CollapsingDenseStore<Allocator>::size_type CollapsingDenseStore<Allocator>::get_new_length(size_type new_min_index, size_type new_max_index) const {
  return std::min(DenseStore<Allocator>::get_new_length(new_min_index, new_max_index), max_num_bins);
}

template<typename Allocator>
void CollapsingDenseStore<Allocator>::clear() {
  DenseStore<Allocator>::clear();
  is_collapsed = false;
}

}
#endif //COLLAPSING_DENSE_STORE_IMPL_HPP
