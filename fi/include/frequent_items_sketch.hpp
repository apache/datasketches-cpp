/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef FREQUENT_ITEMS_SKETCH_HPP_
#define FREQUENT_ITEMS_SKETCH_HPP_

#include <memory>
#include <vector>

#include "reverse_purge_hash_map.hpp"

namespace datasketches {

/*
 * Based on Java implementation here:
 * https://github.com/DataSketches/sketches-core/blob/master/src/main/java/com/yahoo/sketches/frequencies/ItemsSketch.java
 * author Alexander Saydakov
 */

template<typename T, typename H = std::hash<T>, typename E = std::equal_to<T>, typename A = std::allocator<void>>
class frequent_items_sketch {
public:
  enum error_type { NO_FALSE_POSITIVES, NO_FALSE_NEGATIVES };
  explicit frequent_items_sketch(uint8_t lg_max_map_size);
  class row;
  typedef typename std::allocator_traits<A>::template rebind_alloc<row> AllocRow;
  void update(const T& item, uint64_t weight = 1);
  void update(T&& item, uint64_t weight = 1);
  void merge(const frequent_items_sketch<T, H, E, A>& other);
  bool is_empty() const;
  uint64_t get_stream_length() const;
  uint64_t get_estimate(const T& item) const;
  uint64_t get_lower_bound(const T& item) const;
  uint64_t get_upper_bound(const T& item) const;
  uint64_t get_maximum_error() const;
  uint32_t get_num_active_items() const;
  uint32_t get_current_map_capacity() const;
  std::vector<row, AllocRow> get_frequent_items(error_type err_type) const;
  std::vector<row, AllocRow> get_frequent_items(uint64_t threshold, error_type err_type) const;
  void reset();
private:
  static constexpr uint8_t LG_MIN_MAP_SIZE = 3;
  uint8_t lg_max_map_size;
  uint64_t stream_length;
  uint64_t offset;
  reverse_purge_hash_map<T, H, E, A> map;
};

template<typename T, typename H, typename E, typename A>
frequent_items_sketch<T, H, E, A>::frequent_items_sketch(uint8_t lg_max_map_size):
lg_max_map_size(std::max(lg_max_map_size, LG_MIN_MAP_SIZE)),
stream_length(0),
offset(0),
map(LG_MIN_MAP_SIZE, lg_max_map_size)
{
}

template<typename T, typename H, typename E, typename A>
void frequent_items_sketch<T, H, E, A>::update(const T& item, uint64_t weight) {
  if (weight == 0) return;
  stream_length += weight;
  offset += map.adjust_or_insert(item, weight);
}

template<typename T, typename H, typename E, typename A>
void frequent_items_sketch<T, H, E, A>::update(T&& item, uint64_t weight) {
  if (weight == 0) return;
  stream_length += weight;
  offset += map.adjust_or_insert(std::move(item), weight);
}

template<typename T, typename H, typename E, typename A>
void frequent_items_sketch<T, H, E, A>::merge(const frequent_items_sketch<T, H, E, A>& other) {
  if (other.is_empty()) return;
  const uint64_t total_stream_length = stream_length + other.get_stream_length(); // for correction at the end
  for (auto it: other.map) {
    update(it.first, it.second);
  }
  offset += other.offset;
  stream_length = total_stream_length;
}

template<typename T, typename H, typename E, typename A>
bool frequent_items_sketch<T, H, E, A>::is_empty() const {
  return map.get_num_active() == 0;
}

template<typename T, typename H, typename E, typename A>
uint64_t frequent_items_sketch<T, H, E, A>::get_stream_length() const {
  return stream_length;
}

template<typename T, typename H, typename E, typename A>
uint64_t frequent_items_sketch<T, H, E, A>::get_estimate(const T& item) const {
  // if item is tracked estimate = weight + offset, otherwise 0
  const uint64_t weight = map.get(item);
  if (weight > 0) return weight + offset;
  return 0;
}

template<typename T, typename H, typename E, typename A>
uint64_t frequent_items_sketch<T, H, E, A>::get_lower_bound(const T& item) const {
  return map.get(item);
}

template<typename T, typename H, typename E, typename A>
uint64_t frequent_items_sketch<T, H, E, A>::get_upper_bound(const T& item) const {
  return map.get(item) + offset;
}

template<typename T, typename H, typename E, typename A>
uint64_t frequent_items_sketch<T, H, E, A>::get_maximum_error() const {
  return offset;
}

template<typename T, typename H, typename E, typename A>
uint32_t frequent_items_sketch<T, H, E, A>::get_num_active_items() const {
  return map.get_num_active();
}

template<typename T, typename H, typename E, typename A>
std::vector<typename frequent_items_sketch<T, H, E, A>::row, typename std::allocator_traits<A>::template rebind_alloc<typename frequent_items_sketch<T, H, E, A>::row>>
frequent_items_sketch<T, H, E, A>::get_frequent_items(error_type err_type) const {
  return get_frequent_items(get_maximum_error(), err_type);
}

template<typename T, typename H, typename E, typename A>
class frequent_items_sketch<T, H, E, A>::row {
public:
  row(const T& item, uint64_t estimate, uint64_t lower_bound, uint64_t upper_bound):
    item(item), estimate(estimate), lower_bound(lower_bound), upper_bound(upper_bound) {}
  const T& get_item() const { return item; }
  uint64_t get_estimate() const { return estimate; }
  uint64_t get_lower_bound() const { return lower_bound; }
  uint64_t get_upper_bound() const { return upper_bound; }
private:
  T item;
  uint64_t estimate;
  uint64_t lower_bound;
  uint64_t upper_bound;
};

template<typename T, typename H, typename E, typename A>
std::vector<typename frequent_items_sketch<T, H, E, A>::row, typename frequent_items_sketch<T, H, E, A>::AllocRow>
frequent_items_sketch<T, H, E, A>::get_frequent_items(uint64_t threshold, error_type err_type) const {
  std::vector<row, AllocRow> items;
  for (auto it: map) {
    const uint64_t est = it.second + offset;
    const uint64_t lb = it.second;
    const uint64_t ub = est;
    if ((err_type == NO_FALSE_NEGATIVES and ub > threshold) or (err_type == NO_FALSE_POSITIVES and lb > threshold)) {
      items.push_back(row(it.first, est, lb, ub));
    }
  }
  // sort by estimate in descending order
  std::sort(items.begin(), items.end(), [](row a, row b){ return a.get_estimate() > b.get_estimate(); });
  return items;
}

} /* namespace datasketches */

# endif
