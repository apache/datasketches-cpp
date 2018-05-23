/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "kll_quantile_calculator.hpp"

#include <assert.h>
#include <cmath>

namespace sketches {

// assumes that all levels are sorted including level 0
kll_quantile_calculator::kll_quantile_calculator(const float* items, const uint32_t* levels, uint8_t num_levels, uint64_t n) {
  n_ = n;
  const uint32_t num_items(levels[num_levels] - levels[0]);
  items_ = new float[num_items];
  weights_ = new uint64_t[num_items + 1]; // one more is intentional
  levels_ = new uint32_t[num_levels + 1];
  populate_from_sketch(items, num_items, levels, num_levels);
  blocky_tandem_merge_sort(items_, weights_, num_items, levels_, num_levels_);
  convert_to_preceding_cummulative(weights_, num_items + 1);
}

kll_quantile_calculator::~kll_quantile_calculator() {
  delete [] items_;
  delete [] weights_;
  delete [] levels_;
}

float kll_quantile_calculator::get_quantile(double phi) const {
  return approximately_answer_positional_query(pos_of_phi(phi, n_));
}

void kll_quantile_calculator::populate_from_sketch(const float* items, uint32_t num_items, const uint32_t* levels, uint8_t num_levels) {
  std::copy(&items[levels[0]], &items[levels[num_levels]], items_);
  uint8_t src_level(0);
  uint8_t dst_level(0);
  uint64_t weight(1);
  uint32_t offset(levels[0]);
  while (src_level < num_levels) {
    const uint32_t from_index(levels[src_level] - offset);
    const uint32_t to_index(levels[src_level + 1] - offset); // exclusive
    if (from_index < to_index) { // skip empty levels
      std::fill(&weights_[from_index], &weights_[to_index], weight);
      levels_[dst_level] = from_index;
      levels_[dst_level + 1] = to_index;
      dst_level++;
    }
    src_level++;
    weight *= 2;
  }
  weights_[num_items] = 0;
  num_levels_ = dst_level;
}

float kll_quantile_calculator::approximately_answer_positional_query(uint64_t pos) const {
  assert (pos < n_);
  const uint32_t weights_size(levels_[num_levels_] + 1);
  const uint32_t index = chunk_containing_pos(weights_, weights_size, pos);
  return items_[index];
}

void kll_quantile_calculator::convert_to_preceding_cummulative(uint64_t* weights, uint32_t weights_size) {
  uint64_t subtotal(0);
  for (uint32_t i = 0; i < weights_size; i++) {
    const uint32_t new_subtotal = subtotal + weights[i];
    weights[i] = subtotal;
    subtotal = new_subtotal;
  }
}

uint64_t kll_quantile_calculator::pos_of_phi(double phi, uint64_t n) {
  const uint64_t pos = std::floor(phi * n);
  return (pos == n) ? n - 1 : pos;
}

uint32_t kll_quantile_calculator::chunk_containing_pos(uint64_t* weights, uint32_t weights_size, uint64_t pos) {
  assert (weights_size > 1); // remember, weights_ contains an "extra" position
  const uint32_t nominal_length(weights_size - 1);
  assert (weights[0] <= pos);
  assert (pos < weights[nominal_length]);
  return search_for_chunk_containing_pos(weights, pos, 0, nominal_length);
}

uint32_t kll_quantile_calculator::search_for_chunk_containing_pos(const uint64_t* arr, uint64_t pos, uint32_t l, uint32_t r) {
  if (l + 1 == r) {
    return l;
  }
  const uint32_t m(l + (r - l) / 2);
  if (arr[m] <= pos) {
    return search_for_chunk_containing_pos(arr, pos, m, r);
  }
  return search_for_chunk_containing_pos(arr, pos, l, m);
}

void kll_quantile_calculator::blocky_tandem_merge_sort(float* items, uint64_t* weights, uint32_t num_items, const uint32_t* levels, uint8_t num_levels) {
  if (num_levels == 1) return;

  // duplicate the input in preparation for the "ping-pong" copy reduction strategy.
  std::unique_ptr<float[]> items_tmp(new float[num_items]);
  std::copy(items, &items[num_items], items_tmp.get());
  std::unique_ptr<uint64_t[]> weights_tmp(new uint64_t[num_items]); // don't need the extra one here
  std::copy(weights, &weights[num_items], weights_tmp.get());
  blocky_tandem_merge_sort_recursion(items_tmp.get(), weights_tmp.get(), items, weights, levels, 0, num_levels);
}

void kll_quantile_calculator::blocky_tandem_merge_sort_recursion(float* items_src, uint64_t* weights_src, float* items_dst, uint64_t* weights_dst, const uint32_t* levels, uint8_t starting_level, uint8_t num_levels) {
  if (num_levels == 1) return;
  const uint8_t num_levels_1 = num_levels / 2;
  const uint8_t num_levels_2 = num_levels - num_levels_1;
  assert (num_levels_1 >= 1);
  assert (num_levels_2 >= num_levels_1);
  const uint8_t starting_level_1 = starting_level;
  const uint8_t starting_level_2 = starting_level + num_levels_1;
  // swap roles of src and dst
  blocky_tandem_merge_sort_recursion(items_dst, weights_dst, items_src, weights_src, levels, starting_level_1, num_levels_1);
  blocky_tandem_merge_sort_recursion(items_dst, weights_dst, items_src, weights_src, levels, starting_level_2, num_levels_2);
  tandem_merge(items_src, weights_src, items_dst, weights_dst, levels, starting_level_1, num_levels_1, starting_level_2, num_levels_2);
}

void kll_quantile_calculator::tandem_merge(const float* items_src, const uint64_t* weights_src, float* items_dst, uint64_t* weights_dst, const uint32_t* levels, uint8_t starting_level_1, uint8_t num_levels_1, uint8_t starting_level_2, uint8_t num_levels_2) {
  const auto from_index_1 = levels[starting_level_1];
  const auto to_index_1 = levels[starting_level_1 + num_levels_1]; // exclusive
  const auto from_index_2 = levels[starting_level_2];
  const auto to_index_2 = levels[starting_level_2 + num_levels_2]; // exclusive
  auto i_src_1 = from_index_1;
  auto i_src_2 = from_index_2;
  auto i_dst = from_index_1;

  while ((i_src_1 < to_index_1) and (i_src_2 < to_index_2)) {
    if (items_src[i_src_1] < items_src[i_src_2]) {
      items_dst[i_dst] = items_src[i_src_1];
      weights_dst[i_dst] = weights_src[i_src_1];
      i_src_1++;
    } else {
      items_dst[i_dst] = items_src[i_src_2];
      weights_dst[i_dst] = weights_src[i_src_2];
      i_src_2++;
    }
    i_dst++;
  }
  if (i_src_1 < to_index_1) {
    std::copy(&items_src[i_src_1], &items_src[to_index_1], &items_dst[i_dst]);
    std::copy(&weights_src[i_src_1], &weights_src[to_index_1], &weights_dst[i_dst]);
  } else if (i_src_2 < to_index_2) {
    std::copy(&items_src[i_src_2], &items_src[to_index_2], &items_dst[i_dst]);
    std::copy(&weights_src[i_src_2], &weights_src[to_index_2], &weights_dst[i_dst]);
  }
}

}
