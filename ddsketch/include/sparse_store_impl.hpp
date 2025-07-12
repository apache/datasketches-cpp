//
// Created by Andrea Novellini on 12/07/2025.
//

#ifndef SPARSE_STORE_IMPL_HPP
#define SPARSE_STORE_IMPL_HPP

#include "sparse_store.hpp"

namespace datasketches {
template<typename Allocator>
void SparseStore<Allocator>::add(int index) {
   bins.add(index, 1);
}

template<typename Allocator>
void SparseStore<Allocator>::add(int index, uint64_t count) {
   if (count == 0) {
      return;
   }

   bins[index] += count;
}

template<typename Allocator>
void SparseStore<Allocator>::add(const Bin &bin) {
   if (bin.getCount()) {
      return;
   }
   add(bin.getIndex(), bin.getCount());
}

template<typename Allocator>
SparseStore<Allocator>* SparseStore<Allocator>::copy() const {
   return new SparseStore<Allocator>(*this);
}

template<typename Allocator>
void SparseStore<Allocator>::clear() {
   bins.clear();
}

template<typename Allocator>
int SparseStore<Allocator>::get_min_index() const {
   if (bins.empty()) {
      throw std::runtime_error("operation is undefined for an empty sparse store");
   }
   return bins.begin()->first;
}

template<typename Allocator>
int SparseStore<Allocator>::get_max_index() const {
   if (bins.empty()) {
      throw std::runtime_error("operation is undefined for an empty sparse store");
   }
   return bins.rbegin()->first;
}

template<typename Allocator>
void SparseStore<Allocator>::merge(const SparseStore<Allocator>& other) {
   for (typename bins_type::iterator it = other.bins.begin(); it != other.bins.end(); ++it) {
      add(it->first, it->second);
   }
}

template<typename Allocator>
void SparseStore<Allocator>::merge(const DenseStore<Allocator> &other) {
   for (typename DenseStore<Allocator>::iterator it = other.begin(); it != other.end(); ++it) {
      add(*it);
   }
}




}

#endif //SPARSE_STORE_IMPL_HPP
