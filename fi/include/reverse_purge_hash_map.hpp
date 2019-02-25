/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef REVERSE_PURGE_HASH_MAP_HPP_
#define REVERSE_PURGE_HASH_MAP_HPP_

#include <memory>
#include <algorithm>
#include <iterator>

namespace datasketches {

/*
 * Based on Java implementation here:
 * https://github.com/DataSketches/sketches-core/blob/master/src/main/java/com/yahoo/sketches/frequencies/ReversePurgeItemHashMap.java
 * author Alexander Saydakov
 */

template<typename T, typename H = std::hash<T>, typename E = std::equal_to<T>, typename A = std::allocator<void>>
class reverse_purge_hash_map {
  typedef typename std::allocator_traits<A>::template rebind_alloc<T> AllocT;
  typedef typename std::allocator_traits<A>::template rebind_alloc<uint16_t> AllocU16;
  typedef typename std::allocator_traits<A>::template rebind_alloc<uint64_t> AllocU64;

public:
  reverse_purge_hash_map(uint8_t lg_size, uint8_t lg_max_size);
  ~reverse_purge_hash_map();
  uint64_t adjust_or_put_value(const T& key, uint64_t value);
  uint64_t get(const T& key) const;
  void resize(uint8_t new_lg_size);
  uint64_t purge();
  uint8_t get_lg_size() const;
  uint32_t get_capacity() const;
  uint32_t get_num_active() const;
  class const_iterator;
  const_iterator begin() const;
  const_iterator end() const;
private:
  static constexpr double LOAD_FACTOR = 0.75;
  static constexpr uint16_t DRIFT_LIMIT = 1024; // used only for stress testing
  static constexpr uint32_t MAX_SAMPLE_SIZE = 1024; // number of samples to compute approximate median during purge

  uint8_t lg_size;
  uint8_t lg_max_size;
  uint32_t num_active;
  T* keys;
  uint64_t* values;
  uint16_t* states;

  inline bool is_active(uint32_t probe) const;
  void subtract_and_keep_positive_only(uint64_t amount);
  void hash_delete(uint32_t probe);
};

template<typename T, typename H, typename E, typename A>
class reverse_purge_hash_map<T, H, E, A>::const_iterator: public std::iterator<std::input_iterator_tag, T> {
public:
  friend class reverse_purge_hash_map<T, H, E, A>;
  const_iterator(const const_iterator& other) : map(other.map), index(other.index) {}
  const_iterator& operator++() { while (++index < (1U << map->lg_size) and !map->is_active(index)); return *this; }
  const_iterator operator++(int) { const_iterator tmp(*this); operator++(); return tmp; }
  bool operator==(const const_iterator& rhs) const { return index == rhs.index; }
  bool operator!=(const const_iterator& rhs) const { return index != rhs.index; }
  const std::pair<const T&, const uint64_t> operator*() { return std::pair<const T&, const uint64_t>(map->keys[index], map->values[index]); }
private:
  const reverse_purge_hash_map<T>* map;
  uint32_t index;
  const_iterator(const reverse_purge_hash_map<T>* map, uint32_t index): map(map), index(index) {}
};

template<typename T, typename H, typename E, typename A>
reverse_purge_hash_map<T, H, E, A>::reverse_purge_hash_map(uint8_t lg_size, uint8_t lg_max_size):
lg_size(lg_size),
lg_max_size(lg_max_size),
num_active(0),
keys(AllocT().allocate(1 << lg_size)),
values(AllocU64().allocate(1 << lg_size)),
states(AllocU16().allocate(1 << lg_size))
{
  std::fill(states, &states[1 << lg_size], 0);
}

template<typename T, typename H, typename E, typename A>
reverse_purge_hash_map<T, H, E, A>::~reverse_purge_hash_map() {
  const uint32_t size = 1 << lg_size;
  for (uint32_t i = 0; i < size; i++) {
    if (is_active(i)) keys[i].~T();
  }
  AllocT().deallocate(keys, size);
  AllocU64().deallocate(values, size);
  AllocU16().deallocate(states, size);
}

template<typename T, typename H, typename E, typename A>
uint64_t reverse_purge_hash_map<T, H, E, A>::adjust_or_put_value(const T& key, uint64_t value) {
  const uint32_t mask = (1 << lg_size) - 1;
  uint32_t probe = H()(key) & mask;
  uint16_t drift = 1;
  while (is_active(probe) and !E()(keys[probe], key)) {
    probe = (probe + 1) & mask;
    drift++;
    // only used for theoretical analysis
    if (drift >= DRIFT_LIMIT) throw std::logic_error("drift limit reached");
  }
  if (is_active(probe)) {
    // adjusting the value of an existing key
    values[probe] += value;
  } else {
    // adding the key and value to the table
    if (num_active > get_capacity()) {
      throw std::logic_error("num_active " + std::to_string(num_active) + " > capacity " + std::to_string(get_capacity()));
    }
    new (&keys[probe]) T(key);
    values[probe] = value;
    states[probe] = drift;
    num_active++;

    if (num_active > get_capacity()) {
      if (lg_size < lg_max_size) { // can grow
        resize(lg_size + 1);
      } else { // at target size, must purge
        const uint64_t offset = purge();
        if (num_active > get_capacity()) {
          throw std::logic_error("purge did not reduce number of active items");
        }
        return offset;
      }
    }
  }
  return 0;
}

template<typename T, typename H, typename E, typename A>
uint64_t reverse_purge_hash_map<T, H, E, A>::get(const T& key) const {
  const uint32_t mask = (1 << lg_size) - 1;
  uint32_t probe = H()(key) & mask;
  while (is_active(probe) and !E()(keys[probe], key)) {
    probe = (probe + 1) & mask;
  }
  if (states[probe] > 0) {
    return values[probe];
  }
  return 0;
}

template<typename T, typename H, typename E, typename A>
void reverse_purge_hash_map<T, H, E, A>::resize(uint8_t new_lg_size) {
  const uint32_t old_size = 1 << lg_size;
  T* old_keys = keys;
  uint64_t* old_values = values;
  uint16_t* old_states = states;
  const uint32_t new_size = 1 << new_lg_size;
  keys = AllocT().allocate(new_size);
  values = AllocU64().allocate(new_size);
  states = AllocU16().allocate(new_size);
  std::fill(states, &states[new_size], 0);
  num_active = 0;
  lg_size = new_lg_size;
  for (uint32_t i = 0; i < old_size; i++) {
    if (old_states[i] > 0) {
      adjust_or_put_value(old_keys[i], old_values[i]);
      old_keys[i].~T();
    }
  }
  AllocT().deallocate(old_keys, old_size);
  AllocU64().deallocate(old_values, old_size);
  AllocU16().deallocate(old_states, old_size);
}

template<typename T, typename H, typename E, typename A>
uint64_t reverse_purge_hash_map<T, H, E, A>::purge() {
  const uint32_t limit = std::min(MAX_SAMPLE_SIZE, num_active);
  uint32_t num_samples = 0;
  uint32_t i = 0;
  uint64_t* samples = AllocU64().allocate(limit);
  while (num_samples < limit) {
    if (is_active(i)) {
      samples[num_samples++] = values[i];
    }
    i++;
  }
  std::nth_element(&samples[0], &samples[num_samples / 2], &samples[num_samples - 1]);
  const uint64_t median = samples[num_samples / 2];
  subtract_and_keep_positive_only(median);
  return median;
}

template<typename T, typename H, typename E, typename A>
uint8_t reverse_purge_hash_map<T, H, E, A>::get_lg_size() const {
  return lg_size;
}

template<typename T, typename H, typename E, typename A>
uint32_t reverse_purge_hash_map<T, H, E, A>::get_capacity() const {
  return (1 << lg_size) * LOAD_FACTOR;
}

template<typename T, typename H, typename E, typename A>
uint32_t reverse_purge_hash_map<T, H, E, A>::get_num_active() const {
  return num_active;
}

template<typename T, typename H, typename E, typename A>
typename reverse_purge_hash_map<T, H, E, A>::const_iterator reverse_purge_hash_map<T, H, E, A>::begin() const {
  const uint32_t size = 1 << lg_size;
  uint32_t i = 0;
  while (i < size and !is_active(i)) i++;
  return reverse_purge_hash_map<T, H, E, A>::const_iterator(this, i);
}

template<typename T, typename H, typename E, typename A>
typename reverse_purge_hash_map<T, H, E, A>::const_iterator reverse_purge_hash_map<T, H, E, A>::end() const {
  return reverse_purge_hash_map<T, H, E, A>::const_iterator(this, 1 << lg_size);
}

template<typename T, typename H, typename E, typename A>
bool reverse_purge_hash_map<T, H, E, A>::is_active(uint32_t index) const {
  return states[index] > 0;
}

template<typename T, typename H, typename E, typename A>
void reverse_purge_hash_map<T, H, E, A>::subtract_and_keep_positive_only(uint64_t amount) {
  // starting from the back, find the first empty cell,
  // which establishes the high end of a cluster.
  uint32_t first_probe = (1 << lg_size) - 1;
  while (is_active(first_probe)) first_probe--;
  // when we find the next non-empty cell, we know we are at the high end of a cluster
  // work towards the front, delete any non-positive entries.
  for (uint32_t probe = first_probe; probe-- > 0;) {
    if (is_active(probe)) {
      if (values[probe] <= amount) {
        hash_delete(probe); // does the work of deletion and moving higher items towards the front
        num_active--;
      } else {
        values[probe] -= amount;
      }
    }
  }
  // now work on the first cluster that was skipped
  for (uint32_t probe = (1 << lg_size); probe-- > first_probe;) {
    if (is_active(probe)) {
      if (values[probe] <= amount) {
        hash_delete(probe);
        num_active--;
      } else {
        values[probe] -= amount;
      }
    }
  }
}

template<typename T, typename H, typename E, typename A>
void reverse_purge_hash_map<T, H, E, A>::hash_delete(uint32_t delete_index) {
  // Looks ahead in the table to search for another
  // item to move to this location
  // if none are found, the status is changed
  states[delete_index] = 0; // mark as empty
  keys[delete_index].~T();
  uint32_t drift = 1;
  const uint32_t mask = (1 << lg_size) - 1;
  uint32_t probe = (delete_index + drift) & mask; // map length must be a power of 2
  // advance until we find a free location replacing locations as needed
  while (is_active(probe)) {
    if (states[probe] > drift) {
      // move current element
      new (&keys[delete_index]) T(std::move(keys[probe]));
      values[delete_index] = values[probe];
      states[delete_index] = states[probe] - drift;
      states[probe] = 0; // mark as empty
      keys[probe].~T();
      drift = 0;
      delete_index = probe;
    }
    probe = (probe + 1) & mask;
    drift++;
    // only used for theoretical analysis
    if (drift >= DRIFT_LIMIT) throw std::logic_error("drift: " + std::to_string(drift) + " >= DRIFT_LIMIT");
  }
}

} /* namespace datasketches */

# endif
