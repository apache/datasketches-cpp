//
// Created by Andrea Novellini on 12/07/2025.
//

#ifndef SPARSE_STORE_HPP
#define SPARSE_STORE_HPP

#include <map>

#include "bin.hpp"

namespace datasketches {
template<typename Allocator>
class SparseStore {
public:
  using bins_type = std::map<
      int,
      uint64_t,
      std::less<int>,
      typename std::allocator_traits<Allocator>::template rebind_alloc<std::pair<const int, uint64_t>>
  >;
  class iterator;

  SparseStore() = default;

  void add(int index);
  void add(int index, uint64_t count);
  void add(const Bin& bin);
  SparseStore<Allocator>* copy() const;
  void clear();
  int get_min_index() const;
  int get_max_index() const;

private:
  bins_type bins;
};
}

#endif //SPARSE_STORE_HPP
