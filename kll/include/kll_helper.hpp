/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef KLL_HELPER_HPP_
#define KLL_HELPER_HPP_

#include <random>
#include <stdexcept>
#include <algorithm>

namespace datasketches {

static std::independent_bits_engine<std::mt19937, 1, uint32_t> random_bit;

#ifdef KLL_VALIDATION
extern uint32_t kll_next_offset;
#endif

// 0 <= power <= 30
static const uint64_t powers_of_three[] =  {1, 3, 9, 27, 81, 243, 729, 2187, 6561, 19683, 59049, 177147, 531441,
1594323, 4782969, 14348907, 43046721, 129140163, 387420489, 1162261467,
3486784401, 10460353203, 31381059609, 94143178827, 282429536481,
847288609443, 2541865828329, 7625597484987, 22876792454961, 68630377364883,
205891132094649};

class kll_helper {
  public:
    static bool is_even(uint32_t value) {
      return (value & 1) == 0;
    }

    static bool is_odd(uint32_t value) {
      return (value & 1) > 0;
    }

    static uint8_t floor_of_log2_of_fraction(uint64_t numer, uint64_t denom) {
      if (denom > numer) return 0;
      uint8_t count(0);
      while (true) {
        denom <<= 1;
        if (denom > numer) return count;
        count++;
      }
    }

    static uint8_t ub_on_num_levels(uint64_t n) {
      if (n == 0) return 1;
      return 1 + floor_of_log2_of_fraction(n, 1);
    }

    static uint32_t compute_total_capacity(uint16_t k, uint8_t m, uint8_t num_levels) {
      uint32_t total(0);
      for (uint8_t h = 0; h < num_levels; h++) {
        total += level_capacity(k, num_levels, h, m);
      }
      return total;
    }

    static uint32_t level_capacity(uint16_t k, uint8_t numLevels, uint8_t height, uint8_t min_wid) {
      if (height >= numLevels) throw std::invalid_argument("height >= numLevels");
      const uint8_t depth(numLevels - height - 1);
      return std::max((uint32_t) min_wid, int_cap_aux(k, depth));
    }

    static uint32_t int_cap_aux(uint16_t k, uint8_t depth) {
      if (depth > 60) throw std::invalid_argument("depth > 60");
      if (depth <= 30) return int_cap_aux_aux(k, depth);
      const uint8_t half(depth / 2);
      const uint8_t rest(depth - half);
      const uint32_t tmp(int_cap_aux_aux(k, half));
      return int_cap_aux_aux(tmp, rest);
    }

    static uint32_t int_cap_aux_aux(uint16_t k, uint8_t depth) {
      if (depth > 30) throw std::invalid_argument("depth > 30");
      const uint64_t twok(k << 1); // for rounding, we pre-multiply by 2
      const uint64_t tmp((uint64_t) (((uint64_t) twok << depth) / powers_of_three[depth]));
      const uint64_t result((tmp + 1) >> 1); // then here we add 1 and divide by 2
      if (result > k) throw std::logic_error("result > k");
      return result;
    }

    static uint64_t sum_the_sample_weights(uint8_t num_levels, const uint32_t* levels) {
      uint64_t total(0);
      uint64_t weight(1);
      for (uint8_t lvl = 0; lvl < num_levels; lvl++) {
        total += weight * (levels[lvl + 1] - levels[lvl]);
        weight *= 2;
      }
      return total;
    }

    /*
     * This version is for floating point types
     * Checks the sequential validity of the given array of values.
     * They must be unique, monotonically increasing and not NaN.
     */
    template <typename T>
    static typename std::enable_if<std::is_floating_point<T>::value, void>::type
    validate_values(const T* values, uint32_t size) {
      for (uint32_t i = 0; i < size ; i++) {
        if (std::isnan(values[i])) {
          throw std::invalid_argument("Values must not be NaN");
        }
        if ((i < (size - 1)) and !(values[i] < values[i + 1])) {
          throw std::invalid_argument("Values must be unique and monotonically increasing");
        }
      }
    }
    /*
     * This version is for non-floating point types
     * Checks the sequential validity of the given array of values.
     * They must be unique and monotonically increasing.
     */
    template <typename T>
    static typename std::enable_if<!std::is_floating_point<T>::value, void>::type
    validate_values(const T* values, uint32_t size) {
      for (uint32_t i = 0; i < size ; i++) {
        if ((i < (size - 1)) and !(values[i] < values[i + 1])) {
          throw std::invalid_argument("Values must be unique and monotonically increasing");
        }
      }
    }

    template <typename T>
    static void randomly_halve_down(T* buf, uint32_t start, uint32_t length) {
      if (!is_even(length)) throw std::invalid_argument("length must be even");
      const uint32_t half_length(length / 2);
    #ifdef KLL_VALIDATION
      const uint32_t offset(deterministic_offset());
    #else
      const uint32_t offset(random_bit());
    #endif
      uint32_t j(start + offset);
      for (uint32_t i = start; i < (start + half_length); i++) {
        buf[i] = std::move(buf[j]);
        j += 2;
      }
    }

    template <typename T>
    static void randomly_halve_up(T* buf, uint32_t start, uint32_t length) {
      if (!is_even(length)) throw std::invalid_argument("length must be even");
      const uint32_t half_length(length / 2);
    #ifdef KLL_VALIDATION
      const uint32_t offset(deterministic_offset());
    #else
      const uint32_t offset(random_bit());
    #endif
      uint32_t j((start + length) - 1 - offset);
      for (uint32_t i = (start + length) - 1; i >= (start + half_length); i--) {
        buf[i] = std::move(buf[j]);
        j -= 2;
      }
    }

    template <typename T>
    static void merge_sorted_arrays(const T* buf_a, uint32_t start_a, uint32_t len_a, const T* buf_b, uint32_t start_b, uint32_t len_b, T* buf_c, uint32_t start_c) {
      const uint32_t len_c(len_a + len_b);
      const uint32_t lim_a(start_a + len_a);
      const uint32_t lim_b(start_b + len_b);
      const uint32_t lim_c(start_c + len_c);

      uint32_t a(start_a);
      uint32_t b(start_b);

      for (uint32_t c = start_c; c < lim_c; c++) {
        if (a == lim_a) {
          buf_c[c] = std::move(buf_b[b]);
          b++;
        } else if (b == lim_b) {
          buf_c[c] = std::move(buf_a[a]);
          a++;
        } else if (buf_a[a] < buf_b[b]) {
          buf_c[c] = std::move(buf_a[a]);
          a++;
        } else {
          buf_c[c] = std::move(buf_b[b]);
          b++;
        }
      }
      if (a != lim_a || b != lim_b) throw std::logic_error("inconsistent state");
    }

    struct compress_result {
      uint8_t final_num_levels;
      uint32_t final_capacity;
      uint32_t final_pop;
    };

    /*
     * Here is what we do for each level:
     * If it does not need to be compacted, then simply copy it over.
     *
     * Otherwise, it does need to be compacted, so...
     *   Copy zero or one guy over.
     *   If the level above is empty, halve up.
     *   Else the level above is nonempty, so...
     *        halve down, then merge up.
     *   Adjust the boundaries of the level above.
     *
     * It can be proved that generalCompress returns a sketch that satisfies the space constraints
     * no matter how much data is passed in.
     * We are pretty sure that it works correctly when inBuf and outBuf are the same.
     * All levels except for level zero must be sorted before calling this, and will still be
     * sorted afterwards.
     * Level zero is not required to be sorted before, and may not be sorted afterwards.
     *
     * trashes inBuf and inLevels
     * modifies outBuf and outLevels
     *
     * returns (finalNumLevels, finalCapacity, finalItemCount)
     */
    template <typename T>
    static compress_result general_compress(uint16_t k, uint8_t m, uint8_t num_levels_in, T* in_buf,
            uint32_t* in_levels, T* out_buf, uint32_t* out_levels, bool is_level_zero_sorted)
    {
      if (num_levels_in == 0) throw std::invalid_argument("num_levels_in == 0"); // things are too weird if zero levels are allowed
      uint8_t num_levels(num_levels_in);
      uint32_t current_item_count(in_levels[num_levels] - in_levels[0]); // decreases with each compaction
      uint32_t target_item_count = compute_total_capacity(k, m, num_levels); // increases if we add levels
      bool done_yet = false;
      out_levels[0] = 0;
      uint8_t cur_level = 0;
      while (!done_yet) {

        // If we are at the current top level, add an empty level above it for convenience,
        // but do not increment num_levels until later
        if (cur_level == (num_levels - 1)) {
          in_levels[cur_level + 2] = in_levels[cur_level + 1];
        }

        const auto raw_beg(in_levels[cur_level]);
        const auto raw_lim(in_levels[cur_level + 1]);
        const auto raw_pop(raw_lim - raw_beg);

        if ((current_item_count < target_item_count) or (raw_pop < level_capacity(k, num_levels, cur_level, m))) {
          // move level over as is
          // because in_buf and out_buf could be the same, make sure we are not moving data upwards!
          if (raw_beg < out_levels[cur_level]) throw std::logic_error("wrong move");
          std::move(&in_buf[raw_beg], &in_buf[raw_lim], &out_buf[out_levels[cur_level]]);
          out_levels[cur_level + 1] = out_levels[cur_level] + raw_pop;
        } else {
          // The sketch is too full AND this level is too full, so we compact it
          // Note: this can add a level and thus change the sketches capacities

          const auto pop_above(in_levels[cur_level + 2] - raw_lim);
          const bool odd_pop(is_odd(raw_pop));
          const auto adj_beg(odd_pop ? 1 + raw_beg : raw_beg);
          const auto adj_pop(odd_pop ? raw_pop - 1 : raw_pop);
          const auto half_adj_pop(adj_pop / 2);

          if (odd_pop) { // copy one guy over
            out_buf[out_levels[cur_level]] = in_buf[raw_beg];
            out_levels[cur_level + 1] = out_levels[cur_level] + 1;
          } else { // copy zero guys over
            out_levels[cur_level + 1] = out_levels[cur_level];
          }

          // level zero might not be sorted, so we must sort it if we wish to compact it
          if ((cur_level == 0) and !is_level_zero_sorted) {
            std::sort(&in_buf[adj_beg], &in_buf[adj_beg + adj_pop]);
          }

          if (pop_above == 0) { // Level above is empty, so halve up
            randomly_halve_up(in_buf, adj_beg, adj_pop);
          } else { // Level above is nonempty, so halve down, then merge up
            randomly_halve_down(in_buf, adj_beg, adj_pop);
            merge_sorted_arrays(in_buf, adj_beg, half_adj_pop, in_buf, raw_lim, pop_above, in_buf, adj_beg + half_adj_pop);
          }

          // track the fact that we just eliminated some data
          current_item_count -= half_adj_pop;

          // adjust the boundaries of the level above
          in_levels[cur_level + 1] = in_levels[cur_level + 1] - half_adj_pop;

          // Increment numLevels if we just compacted the old top level
          // This creates some more capacity (the size of the new bottom level)
          if (cur_level == (num_levels - 1)) {
            num_levels++;
            target_item_count += level_capacity(k, num_levels, 0, m);
          }

        } // end of code for compacting a level

        // determine whether we have processed all levels yet (including any new levels that we created)

        if (cur_level == (num_levels - 1)) done_yet = true;
        cur_level++;
      } // end of loop over levels

      if ((out_levels[num_levels] - out_levels[0]) != current_item_count) throw std::logic_error("inconsistent state");

      compress_result result;
      result.final_num_levels = num_levels;
      result.final_capacity = target_item_count;
      result.final_pop = current_item_count;
      return result;
    }

#ifdef KLL_VALIDATION
  private:

    static uint32_t deterministic_offset() {
      const uint32_t result(kll_next_offset);
      kll_next_offset = 1 - kll_next_offset;
      return result;
    }
#endif

};

} /* namespace datasketches */

#endif // KLL_HELPER_HPP_
