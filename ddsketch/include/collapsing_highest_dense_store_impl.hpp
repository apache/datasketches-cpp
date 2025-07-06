//
// Created by geonove on 6/28/25.
//

#ifndef COLLAPSING_HIGHEST_DENSE_STORE_IMPL_HPP
#define COLLAPSING_HIGHEST_DENSE_STORE_IMPL_HPP

#include "collapsing_highest_dense_store.hpp"

namespace datasketches {
template<typename Allocator>
CollapsingHighestDenseStore<Allocator>::CollapsingHighestDenseStore(size_type max_num_bins): CollapsingDenseStore<Allocator>(max_num_bins){}

template<typename Allocator>
CollapsingHighestDenseStore<Allocator>* CollapsingHighestDenseStore<Allocator>::copy() const {
  // TODO to implement
  return nullptr;
}

template<typename Allocator>
void CollapsingHighestDenseStore<Allocator>::merge(const DenseStore<Allocator>& other) {
  const auto* store = dynamic_cast<const CollapsingHighestDenseStore<Allocator>*>(&other);
  if (store != nullptr) {
    this->merge(*store);
  } else {
    for (const Bin& bin : other) {
      this->add(bin);
    }
  }
}

template<typename Allocator>
void CollapsingHighestDenseStore<Allocator>::merge(const CollapsingHighestDenseStore<Allocator>& other) {
  if (other.is_empty()) {
    return;
  }

  if (other.min_index < this->min_index || other.max_index > this->max_index) {
    this->extend_range(other.min_index, other.max_index);
  }

  size_type index = other.max_index;
  for (; index > this->max_index && index >= other.min_index; index--) {
    this->bins[this->bins.size() - 1] += other.bins[index - other.offset];
  }
  for (; index > other.min_index; index--) {
    this->bins[index - this->offset] += other.bins[index - other.offset];
  }
  // This is a separate test so that the comparison in the previous loop is strict (>) and handles
  // other.min_index = Integer.MIN_VALUE.
  if (index == other.min_index) {
    this->bins[index - this->offset] += other.bins[index - other.offset];
  }
}

template <typename Allocator>
typename CollapsingHighestDenseStore<Allocator>::size_type CollapsingHighestDenseStore<Allocator>::normalize(size_type index) {
  if (index > this->max_index) {
    if (this->is_collapsed) {
      return this->bins.size() - 1;
    }
    this->extend_range(index);
    if (this->is_collapsed) {
      return this->bins.size() - 1;
    }
  } else if (index < this->min_index) {
    this->extend_range(index);
  }

  return index - this->offset;
}

template <typename Allocator>
void CollapsingHighestDenseStore<Allocator>::adjust(size_type new_min_index, size_type new_max_index) {
  if (new_max_index - new_min_index + 1 > this->bins.size()) {
    // The range of indices is too wide, buckets of lowest indices need to be collapsed.
    new_max_index = new_min_index + this->bins.size() - 1;

    if (new_max_index <= this->min_index) {
      // There will be only one non-empty bucket.
      const double total_count = this->get_total_count(new_min_index, new_max_index);
      this->reset_bins();
      this->offset = new_min_index;
      this->max_index = new_max_index;
      this->bins[this->bins.size() - 1] = total_count;
    } else {
      const size_type shift = this->offset - new_min_index;
      if (shift > 0) {
        // Collapse the buckets.
        const double collapsed_count = this->get_total_count(new_max_index + 1, this->max_index);
        this->reset_bins(new_max_index + 1, this->max_index);
        this->bins[new_max_index - this->offset] += collapsed_count;
        this->max_index = new_max_index;
        // Shift the buckets to make room for new_min_index.
        this->shift_bins(shift);
      } else {
        // Shift the buckets to make room for new_max_index.
        this->shift_bins(shift);
        this->max_index = new_max_index;
      }
    }
    this->min_index = new_min_index;
    this->is_collapsed = true;
  } else {
    this->center_bins(new_min_index, new_max_index);
  }
}
}

#endif //COLLAPSING_HIGHEST_DENSE_STORE_IMPL_HPP