//
// Created by geonove on 6/28/25.
//

#ifndef COLLAPSING_DENSE_STORE_HPP
#define COLLAPSING_DENSE_STORE_HPP

#include "dense_store.hpp"

namespace datasketches {
template<typename Allocator>
class CollapsingDenseStore : public DenseStore<Allocator> {
public:

  using size_type = typename DenseStore<Allocator>::size_type;
  explicit CollapsingDenseStore(size_type max_num_bins);
  CollapsingDenseStore(const CollapsingDenseStore<Allocator>& other) = default;

  ~CollapsingDenseStore() override = default;

private:
  const size_type max_num_bins;
  bool is_collapsed;

  size_type get_new_length(size_type new_min_index, size_type new_max_index) const override;
  void clear() override;
};
}

#endif //COLLAPSING_DENSE_STORE_HPP
