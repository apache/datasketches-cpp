//
// Created by Andrea Novellini on 12/07/2025.
//

#ifndef UNBOUNDED_SIZE_DENSE_STORE_HPP
#define UNBOUNDED_SIZE_DENSE_STORE_HPP
#include "dense_store.hpp"

namespace datasketches {
template<typename Allocator>
class UnboundedSizeDenseStore: public DenseStore<Allocator> {
public:
  using size_type = typename DenseStore<Allocator>::size_type;

  UnboundedSizeDenseStore();
  explicit UnboundedSizeDenseStore(const int& array_length_growth_increment);
  explicit UnboundedSizeDenseStore(const int& array_length_growth_increment, const int& array_length_overhead);
  explicit UnboundedSizeDenseStore(const UnboundedSizeDenseStore& other) = default;

  UnboundedSizeDenseStore* copy() const override;
  ~UnboundedSizeDenseStore() override = default;

  void merge(const DenseStore<Allocator>& other) override;

protected:
  size_type normalize(size_type index) override;
  void adjust(size_type new_min_index, size_type new_max_index) override;
  void merge(const UnboundedSizeDenseStore<Allocator>& other);
};
}

#include "unbounded_size_dense_store_impl.hpp"

#endif //UNBOUNDED_SIZE_DENSE_STORE_HPP
