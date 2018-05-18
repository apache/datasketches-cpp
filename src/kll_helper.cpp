/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "kll_helper.hpp"

#include <assert.h>
#include <algorithm>

namespace sketches {

bool kll_helper::is_even(size_t value) {
  return (value & 1) == 0;
}

bool kll_helper::is_odd(size_t value) {
  return (value & 1) > 0;
}

uint32_t kll_helper::compute_total_capacity(uint16_t k, uint8_t m, uint8_t numLevels) {
  uint32_t total(0);
  for (uint8_t h = 0; h < numLevels; h++) {
    total += level_capacity(k, numLevels, h, m);
  }
  return total;
}

uint32_t kll_helper::level_capacity(uint16_t k, uint8_t numLevels, uint8_t height, uint8_t minWid) {
  assert (height < numLevels);
  const uint8_t depth(numLevels - height - 1);
  return std::max((uint32_t) minWid, int_cap_aux(k, depth));
}

uint32_t kll_helper::int_cap_aux(uint16_t k, uint8_t depth) {
  assert (k <= (1 << 30));
  assert (depth <= 60);
  if (depth <= 30) return int_cap_aux_aux(k, depth);
  const uint8_t half(depth / 2);
  const uint8_t rest(depth - half);
  const uint32_t tmp(int_cap_aux_aux(k, half));
  return int_cap_aux_aux(tmp, rest);
}

// 0 <= power <= 30
static const long long powers_of_three[] =  {1, 3, 9, 27, 81, 243, 729, 2187, 6561, 19683, 59049, 177147, 531441,
1594323, 4782969, 14348907, 43046721, 129140163, 387420489, 1162261467,
3486784401, 10460353203, 31381059609, 94143178827, 282429536481,
847288609443, 2541865828329, 7625597484987, 22876792454961, 68630377364883,
205891132094649};

uint32_t kll_helper::int_cap_aux_aux(uint16_t k, uint8_t depth) {
  assert (k <= (1 << 30));
  assert (depth <= 30);
  const size_t twok(k << 1); // for rounding, we pre-multiply by 2
  const size_t tmp((size_t) (((size_t) twok << depth) / powers_of_three[depth]));
  const size_t result((tmp + 1) >> 1); // then here we add 1 and divide by 2
  assert (result <= k);
  return result;
}

void kll_helper::randomly_halve_down(float* buf, size_t start, size_t length) {
  assert (is_even(length));
  const size_t half_length(length / 2);
  const size_t offset(random_bit());
  //const size_t offset(deterministic_offset()); // for validation
  size_t j(start + offset);
  for (size_t i = start; i < (start + half_length); i++) {
    buf[i] = buf[j];
    j += 2;
  }
}

void kll_helper::randomly_halve_up(float* buf, size_t start, size_t length) {
  assert (is_even(length));
  const size_t half_length(length / 2);
  const size_t offset(random_bit());
  //const size_t offset(deterministic_offset()); // for validation
  size_t j((start + length) - 1 - offset);
  for (size_t i = (start + length) - 1; i >= (start + half_length); i--) {
    buf[i] = buf[j];
    j -= 2;
  }
}

void kll_helper::merge_sorted_arrays(float* buf_a, size_t start_a, size_t len_a, float* buf_b, size_t start_b, size_t len_b, float* buf_c, size_t start_c) {
  const size_t len_c(len_a + len_b);
  const size_t lim_a(start_a + len_a);
  const size_t lim_b(start_b + len_b);
  const size_t lim_c(start_c + len_c);

  size_t a(start_a);
  size_t b(start_b);

  for (size_t c = start_c; c < lim_c; c++) {
    if (a == lim_a) {
      buf_c[c] = buf_b[b];
      b++;
    } else if (b == lim_b) {
      buf_c[c] = buf_a[a];
      a++;
    } else if (buf_a[a] < buf_b[b]) {
      buf_c[c] = buf_a[a];
      a++;
    } else {
      buf_c[c] = buf_b[b];
      b++;
    }
  }
  assert (a == lim_a);
  assert (b == lim_b);
}

static size_t next_offset = 0;

size_t kll_helper::deterministic_offset() {
  const int result(next_offset);
  next_offset = 1 - next_offset;
  return result;
}

}
