/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef KLL_QUANTILE_CALCULATOR_HPP_
#define KLL_QUANTILE_CALCULATOR_HPP_

#include <memory>

namespace sketches {

class kll_quantile_calculator {
  public:
    kll_quantile_calculator(const float* items, const uint32_t* levels, uint8_t num_levels, uint64_t n);
    ~kll_quantile_calculator();
    float get_quantile(double fraction) const;

  private:
    uint64_t n_;
    float* items_;
    uint64_t* weights_;
    uint32_t* levels_;
    uint8_t num_levels_;

    void populate_from_sketch(const float* items, uint32_t num_items, const uint32_t* levels, uint8_t num_levels);
    float approximately_answer_positional_query(uint64_t pos) const;

    static void convert_to_preceding_cummulative(uint64_t* weights, uint32_t weights_size);
    static uint64_t pos_of_phi(double phi, uint64_t n);
    static uint32_t chunk_containing_pos(uint64_t* weights, uint32_t num_items, uint64_t pos);
    static uint32_t search_for_chunk_containing_pos(const uint64_t* weights, uint64_t pos, uint32_t l, uint32_t r);
    static void blocky_tandem_merge_sort(float* items, uint64_t* weights, uint32_t num_items, const uint32_t* levels, uint8_t num_levels);
    static void blocky_tandem_merge_sort_recursion(float* items_src, uint64_t* weights_src, float* items_dst, uint64_t* weights_dst, const uint32_t* levels, uint8_t starting_level, uint8_t num_levels);
    static void tandem_merge(const float* items_src, const uint64_t* weights_src, float* items_dst, uint64_t* weights_dst, const uint32_t* levels, uint8_t starting_level_1, uint8_t num_levels_1, uint8_t starting_level_2, uint8_t num_levels_2);
};

}

#endif
