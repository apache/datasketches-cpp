/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef KLL_HELPER_HPP_
#define KLL_HELPER_HPP_

#include <random>

namespace sketches {

static std::independent_bits_engine<std::mt19937, 1, size_t> random_bit;

class kll_helper {
  public:
    static bool is_even(size_t value);
    static bool is_odd(size_t value);
    static uint32_t compute_total_capacity(uint16_t k, uint8_t m, uint8_t num_levels);
    static uint32_t level_capacity(uint16_t k, uint8_t numLevels, uint8_t height, uint8_t minWid);
    static uint32_t int_cap_aux(uint16_t k, uint8_t depth);
    static uint32_t int_cap_aux_aux(uint16_t k, uint8_t depth);
    static void randomly_halve_down(float* buf, size_t start, size_t length);
    static void randomly_halve_up(float* buf, size_t start, size_t length);
    static void merge_sorted_arrays(float* buf_a, size_t start_a, size_t len_a, float* buf_b, size_t start_b, size_t len_b, float* buf_c, size_t start_c);

  private:
    static size_t deterministic_offset();

};

}

#endif
