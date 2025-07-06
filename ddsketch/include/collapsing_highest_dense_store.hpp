//
// Created by geonove on 6/28/25.
//

#ifndef COLLAPSING_HIGHEST_DENSE_STORE_HPP
#define COLLAPSING_HIGHEST_DENSE_STORE_HPP

#include "collapsing_dense_store.hpp"

namespace datasketches {
template<typename Allocator>
class CollapsingHighestDenseStore : public CollapsingDenseStore<Allocator> {
public:
  using size_type = typename CollapsingDenseStore<Allocator>::size_type;

  explicit CollapsingHighestDenseStore(size_type max_num_bins);

  CollapsingHighestDenseStore* copy() const override;
  void merge(const DenseStore<Allocator>& other) override;
  void merge(const CollapsingHighestDenseStore& other);

protected:
  size_type normalize(size_type index) override;
  void adjust(size_type new_min_index, size_type new_max_index) override;
};
}

#include "collapsing_highest_dense_store_impl.hpp"
#endif //COLLAPSING_HIGHEST_DENSE_STORE_HPP