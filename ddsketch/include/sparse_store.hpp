//
// Created by Andrea Novellini on 12/07/2025.
//

#ifndef SPARSE_STORE_HPP
#define SPARSE_STORE_HPP

#include <map>

#include "bin.hpp"
#include "dense_store.hpp"

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
  void merge(const SparseStore<Allocator>& other);
  void merge(const DenseStore<Allocator>& other);
  bool is_empty() const;
  uint64_t get_total_count() const;

  iterator begin() const;
  iterator end() const;

  class iterator {
  public:
    using internal_iterator = typename bins_type::const_iterator;
    using value_type = Bin;
    using difference_type = void;
    using pointer = const Bin*;
    using reference = const Bin;

    explicit iterator(internal_iterator it);
    iterator& operator++();
    bool operator!=(const iterator& other) const;
    reference operator*() const;

  private:
    internal_iterator it;
  };

private:
  bins_type bins;
  void print() {
    for (auto it = bins.begin(); it != bins.end(); ++it) {
      std::cout << "(" << it->first << ";" << it->second << ") ";
    }
    std::cout << std::endl;
  }
};
}

#include "sparse_store_impl.hpp"

#endif //SPARSE_STORE_HPP
