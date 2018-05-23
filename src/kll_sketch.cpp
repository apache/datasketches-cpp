/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "kll_sketch.hpp"
#include "kll_helper.hpp"

#include <assert.h>
#include <limits>
#include <algorithm>
#include <iostream>
#include <iomanip>
#include <stdexcept>

namespace sketches {

kll_sketch::kll_sketch(uint16_t k) {
  k_ = k;
  m_ = DEFAULT_M;
  min_k_ = k;
  n_ = 0;
  num_levels_ = 1;
  levels_size_ = 2;
  levels_ = new uint32_t[2] {k_, k_};
  items_size_ = k_;
  items_ = new float[k_];
  min_value_ = std::numeric_limits<float>::quiet_NaN();
  max_value_ = std::numeric_limits<float>::quiet_NaN();
  is_level_zero_sorted_ = false;
}

kll_sketch::~kll_sketch() {
  delete [] levels_;
  delete [] items_;
}

void kll_sketch::update(float value) {
  if (is_empty()) {
    min_value_ = value;
    max_value_ = value;
  } else {
    if (value < min_value_) min_value_ = value;
    if (value > max_value_) max_value_ = value;
  }
  if (levels_[0] == 0) compress_while_updating();
  n_++;
  is_level_zero_sorted_ = false;
  const uint32_t nextPos(levels_[0] - 1);
  levels_[0] = nextPos;
  items_[nextPos] = value;
}

void kll_sketch::merge(const kll_sketch& other) {
  if (other.is_empty()) return;
  if (m_ != other.m_) {
    throw std::invalid_argument("incompatible M: " + std::to_string(m_) + " and " + std::to_string(other.m_));
  }
  if (is_empty()) {
    min_value_ = other.min_value_;
    max_value_ = other.max_value_;
  } else {
    if (other.min_value_ < min_value_) min_value_ = other.min_value_;
    if (other.max_value_ > max_value_) max_value_ = other.max_value_;
  }
  const uint64_t final_n(n_ + other.n_);
  if (other.num_levels_ >= 1) {
    for (uint32_t i = other.levels_[0]; i < other.levels_[1]; i++) {
      update(other.items_[i]);
    }
  }
  if (other.num_levels_ >= 2) {
    merge_higher_levels(other, final_n);
  }
  n_ = final_n;
  assert_correct_total_weight();
  min_k_ = std::min(min_k_, other.min_k_);
}

bool kll_sketch::is_empty() const {
  return n_ == 0;
}

uint64_t kll_sketch::get_n() const {
  return n_;
}

uint32_t kll_sketch::get_num_retained() const {
  return levels_[num_levels_] - levels_[0];
}

bool kll_sketch::is_estimation_mode() const {
  return num_levels_ > 1;
}

float kll_sketch::get_min_value() const {
  return min_value_;
}

float kll_sketch::get_max_value() const {
  return max_value_;
}

float kll_sketch::get_quantile(double fraction) const {
  if (is_empty()) return std::numeric_limits<double>::quiet_NaN();
  if (fraction == 0.0) return min_value_;
  if (fraction == 1.0) return max_value_;
  if ((fraction < 0.0) or (fraction > 1.0)) {
    throw std::invalid_argument("Fraction cannot be less than zero or greater than 1.0");
  }
  // has side effect of sorting level zero if needed
  auto quantile_calculator(const_cast<kll_sketch*>(this)->get_quantile_calculator());
  return quantile_calculator->get_quantile(fraction);
}

std::unique_ptr<float[]> kll_sketch::get_quantiles(const double* fractions, uint32_t size) const {
  if (is_empty()) { return nullptr; }
  std::unique_ptr<kll_quantile_calculator> quantile_calculator;
  std::unique_ptr<float[]> quantiles(new float[size]);
  for (uint32_t i = 0; i < size; i++) {
    const double fraction = fractions[i];
    if      (fraction == 0.0) quantiles[i] = min_value_;
    else if (fraction == 1.0) quantiles[i] = max_value_;
    else {
      if (!quantile_calculator) {
        // has side effect of sorting level zero if needed
        quantile_calculator = const_cast<kll_sketch*>(this)->get_quantile_calculator();
      }
      quantiles[i] = quantile_calculator->get_quantile(fraction);
    }
  }
  return std::move(quantiles);
}

double kll_sketch::get_rank(float value) const {
  if (is_empty()) return std::numeric_limits<double>::quiet_NaN();
  uint8_t level(0);
  uint64_t weight(1);
  uint64_t total(0);
  while (level < num_levels_) {
    const auto from_index(levels_[level]);
    const auto to_index(levels_[level + 1]); // exclusive
    for (uint32_t i = from_index; i < to_index; i++) {
      if (items_[i] < value) {
        total += weight;
      } else if ((level > 0) or is_level_zero_sorted_) {
        break; // levels above 0 are sorted, no point comparing further
      }
    }
    level++;
    weight *= 2;
  }
  return (double) total / n_;
}

std::unique_ptr<double[]> kll_sketch::get_PMF(const float* split_points, uint32_t size) const {
  return get_PMF_or_CDF(split_points, size, false);
}

std::unique_ptr<double[]> kll_sketch::get_CDF(const float* split_points, uint32_t size) const {
  return get_PMF_or_CDF(split_points, size, true);
}

double kll_sketch::get_normalized_rank_error(bool pmf) const {
  return get_normalized_rank_error(min_k_, pmf);
}

uint32_t kll_sketch::get_serialized_size_bytes() const {
  if (is_empty()) { return EMPTY_SIZE_BYTES; }
  return get_serialized_size_bytes(num_levels_, get_num_retained());
}

void kll_sketch::serialize(std::ostream& os) const {
  const uint8_t preamble_ints(is_empty() ? PREAMBLE_INTS_EMPTY : PREAMBLE_INTS_NONEMPTY);
  os.write((char*)&preamble_ints, sizeof(preamble_ints));
  const uint8_t serial_version(SERIAL_VERSION);
  os.write((char*)&serial_version, sizeof(serial_version));
  const uint8_t family(FAMILY);
  os.write((char*)&family, sizeof(family));
  const uint8_t flags(
      (is_empty() ? 1 << kll_flags::IS_EMPTY : 0)
    | (is_level_zero_sorted_ ? 1 << kll_flags::IS_LEVEL_ZERO_SORTED : 0)
  );
  os.write((char*)&flags, sizeof(flags));
  os.write((char*)&k_, sizeof(k_));
  os.write((char*)&m_, sizeof(m_));
  const uint8_t unused(0);
  os.write((char*)&unused, sizeof(unused));
  if (is_empty()) return;
  os.write((char*)&n_, sizeof(n_));
  os.write((char*)&min_k_, sizeof(min_k_));
  os.write((char*)&num_levels_, sizeof(num_levels_));
  os.write((char*)&unused, sizeof(unused));
  os.write((char*)levels_, sizeof(levels_[0]) * num_levels_);
  os.write((char*)&min_value_, sizeof(min_value_));
  os.write((char*)&max_value_, sizeof(max_value_));
  os.write((char*)&(items_[levels_[0]]), sizeof(items_[0]) * get_num_retained());
}

std::unique_ptr<kll_sketch> kll_sketch::deserialize(std::istream& is) {
  std::unique_ptr<kll_sketch> sketch_ptr(new kll_sketch);
  uint8_t preamble_ints;
  is.read((char*)&preamble_ints, sizeof(preamble_ints));
  uint8_t serial_version;
  is.read((char*)&serial_version, sizeof(serial_version));
  uint8_t family_id;
  is.read((char*)&family_id, sizeof(family_id));
  uint8_t flags;
  is.read((char*)&flags, sizeof(flags));
  is.read((char*)&(sketch_ptr->k_), sizeof(sketch_ptr->k_));
  is.read((char*)&(sketch_ptr->m_), sizeof(sketch_ptr->m_));
  const bool is_empty = flags & (1 << kll_flags::IS_EMPTY);
  uint8_t unused;
  is.read((char*)&unused, sizeof(unused));

  if (sketch_ptr->m_ != DEFAULT_M) {
    throw std::invalid_argument("Possible corruption: M must be " + std::to_string(DEFAULT_M)
        + ": " + std::to_string(sketch_ptr->m_));
  }
  if (is_empty) {
    if (preamble_ints != PREAMBLE_INTS_EMPTY) {
      throw std::invalid_argument("Possible corruption: preamble ints must be "
          + std::to_string(PREAMBLE_INTS_EMPTY) + " for an empty sketch: " + std::to_string(preamble_ints));
    }
  } else {
    if (preamble_ints != PREAMBLE_INTS_NONEMPTY) {
      throw std::invalid_argument("Possible corruption: preamble ints must be "
          + std::to_string(PREAMBLE_INTS_NONEMPTY) + " for a non-empty sketch: " + std::to_string(preamble_ints));
    }
  }
  if (serial_version != SERIAL_VERSION) {
    throw std::invalid_argument("Possible corruption: serial version mismatch: expected "
        + std::to_string(SERIAL_VERSION) + ", got " + std::to_string(serial_version));
  }
  if (family_id != FAMILY) {
    throw std::invalid_argument("Possible corruption: family mismatch: expected "
        + std::to_string(FAMILY) + ", got " + std::to_string(family_id));
  }

  if (is_empty) {
    sketch_ptr->min_k_ = sketch_ptr->k_;
    sketch_ptr->n_ = 0;
    sketch_ptr->num_levels_ = 1;
    sketch_ptr->levels_size_ = 2;
    sketch_ptr->levels_ = new uint32_t[2] {sketch_ptr->k_, sketch_ptr->k_};
    sketch_ptr->items_size_ = sketch_ptr->k_;
    sketch_ptr->items_ = new float[sketch_ptr->k_];
    sketch_ptr->min_value_ = std::numeric_limits<float>::quiet_NaN();
    sketch_ptr->max_value_ = std::numeric_limits<float>::quiet_NaN();
    sketch_ptr->is_level_zero_sorted_ = false;
  } else {
    is.read((char*)&(sketch_ptr->n_), sizeof(sketch_ptr->n_));
    is.read((char*)&(sketch_ptr->min_k_), sizeof(sketch_ptr->min_k_));
    is.read((char*)&(sketch_ptr->num_levels_), sizeof(sketch_ptr->num_levels_));
    is.read((char*)&unused, sizeof(unused));
    sketch_ptr->levels_ = new uint32_t[sketch_ptr->num_levels_ + 1];
    // the last integer in levels_ is not serialized because it can be derived
    is.read((char*)sketch_ptr->levels_, sizeof(sketch_ptr->levels_[0]) * sketch_ptr->num_levels_);
    const uint32_t capacity(kll_helper::compute_total_capacity(sketch_ptr->k_, sketch_ptr->m_, sketch_ptr->num_levels_));
    sketch_ptr->levels_[sketch_ptr->num_levels_] = capacity;
    is.read((char*)&(sketch_ptr->min_value_), sizeof(sketch_ptr->min_value_));
    is.read((char*)&(sketch_ptr->max_value_), sizeof(sketch_ptr->max_value_));
    sketch_ptr->items_ = new float[capacity];
    is.read((char*)&(sketch_ptr->items_[sketch_ptr->levels_[0]]), sizeof(sketch_ptr->items_[0]) * sketch_ptr->get_num_retained());
    sketch_ptr->is_level_zero_sorted_ = (flags & (1 << kll_flags::IS_LEVEL_ZERO_SORTED)) > 0;
  }
  return std::move(sketch_ptr);
}

/*
 * Gets the normalized rank error given k and pmf.
 * k - the configuration parameter
 * pmf - if true, returns the "double-sided" normalized rank error for the get_PMF() function.
 * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
 * Constants were derived as the best fit to 99 percentile empirically measured max error in thousands of trials
 */
double kll_sketch::get_normalized_rank_error(uint16_t k, bool pmf) {
  return pmf
      ? 2.446 / pow(k, 0.9433)
      : 2.296 / pow(k, 0.9723);
}

// The following code is only valid in the special case of exactly reaching capacity while updating.
// It cannot be used while merging, while reducing k, or anything else.
void kll_sketch::compress_while_updating(void) {
  const uint8_t level(find_level_to_compact());

  // It is important to do add the new top level right here. Be aware that this operation
  // grows the buffer and shifts the data and also the boundaries of the data and grows the
  // levels array and increments numLevels_
  if (level == (num_levels_ - 1)) {
    add_empty_top_level_to_completely_full_sketch();
  }

  const uint32_t raw_beg(levels_[level]);
  const uint32_t raw_lim(levels_[level + 1]);
  // +2 is OK because we already added a new top level if necessary
  const uint32_t pop_above(levels_[level + 2] - raw_lim);
  const uint32_t raw_pop(raw_lim - raw_beg);
  const bool odd_pop(kll_helper::is_odd(raw_pop));
  const uint32_t adj_beg(odd_pop ? raw_beg + 1 : raw_beg);
  const uint32_t adj_pop(odd_pop ? raw_pop - 1 : raw_pop);
  const uint32_t half_adj_pop(adj_pop / 2);

  // level zero might not be sorted, so we must sort it if we wish to compact it
  if (level == 0) {
    std::sort(&items_[adj_beg], &items_[adj_beg + adj_pop]);
  }
  if (pop_above == 0) {
    kll_helper::randomly_halve_up(items_, adj_beg, adj_pop);
  } else {
    kll_helper::randomly_halve_down(items_, adj_beg, adj_pop);
    kll_helper::merge_sorted_arrays(items_, adj_beg, half_adj_pop, items_, raw_lim, pop_above, items_, adj_beg + half_adj_pop);
  }
  levels_[level + 1] -= half_adj_pop; // adjust boundaries of the level above
  if (odd_pop) {
    levels_[level] = levels_[level + 1] - 1; // the current level now contains one item
    items_[levels_[level]] = items_[raw_beg]; // namely this leftover guy
  } else {
    levels_[level] = levels_[level + 1]; // the current level is now empty
  }

  // verify that we freed up halfAdjPop array slots just below the current level
  assert (levels_[level] == (raw_beg + half_adj_pop));

  // finally, we need to shift up the data in the levels below
  // so that the freed-up space can be used by level zero
  if (level > 0) {
    const uint32_t amount(raw_beg - levels_[0]);
    std::copy_backward(&items_[levels_[0]], &items_[levels_[0] + amount], &items_[levels_[0] + half_adj_pop + amount]);
    for (uint8_t lvl = 0; lvl < level; lvl++) {
      levels_[lvl] += half_adj_pop;
    }
  }
}

uint8_t kll_sketch::find_level_to_compact() const {
  uint8_t level(0);
  while (true) {
    assert (level < num_levels_);
    const uint32_t pop(levels_[level + 1] - levels_[level]);
    const uint32_t cap(kll_helper::level_capacity(k_, num_levels_, level, m_));
    if (pop >= cap) {
      return level;
    }
    level++;
  }
}

void kll_sketch::add_empty_top_level_to_completely_full_sketch() {
  const uint32_t cur_total_cap(levels_[num_levels_]);

  // make sure that we are following a certain growth scheme
  assert (levels_[0] == 0);
  assert (items_size_ == cur_total_cap);

  // note that merging MIGHT over-grow levels_, in which case we might not have to grow it here
  if (levels_size_ < (num_levels_ + 2)) {
    uint32_t* new_levels(new uint32_t[num_levels_ + 2]);
    std::copy(&levels_[0], &levels_[levels_size_], new_levels);
    delete [] levels_;
    levels_ = new_levels;
    levels_size_ = num_levels_ + 2;
  }

  const uint32_t delta_cap(kll_helper::level_capacity(k_, num_levels_ + 1, 0, m_));
  const uint32_t new_total_cap(cur_total_cap + delta_cap);

  float* new_buf(new float[new_total_cap]);

  // copy (and shift) the current data into the new buffer
  std::copy(&items_[levels_[0]], &items_[levels_[0] + cur_total_cap], &new_buf[levels_[0] + delta_cap]);
  delete [] items_;
  items_ = new_buf;
  items_size_ = new_total_cap;

  // this loop includes the old "extra" index at the top
  for (uint8_t i = 0; i <= num_levels_; i++) {
    levels_[i] += delta_cap;
  }

  assert (levels_[num_levels_] == new_total_cap);

  num_levels_++;
  levels_[num_levels_] = new_total_cap; // initialize the new "extra" index at the top
}

void kll_sketch::sort_level_zero() {
  if (!is_level_zero_sorted_) {
    std::sort(&items_[levels_[0]], &items_[levels_[1]]);
    is_level_zero_sorted_ = true;
  }
}

std::unique_ptr<kll_quantile_calculator> kll_sketch::get_quantile_calculator() {
  sort_level_zero();
  std::unique_ptr<kll_quantile_calculator> quantile_calculator(new kll_quantile_calculator(items_, levels_, num_levels_, n_));
  return std::move(quantile_calculator);
}

std::unique_ptr<double[]> kll_sketch::get_PMF_or_CDF(const float* split_points, uint32_t size, bool is_CDF) const {
  if (is_empty()) return nullptr;
  kll_helper::validate_values(split_points, size);
  std::unique_ptr<double[]> buckets(new double[size + 1]);
  std::fill(&buckets.get()[0], &buckets.get()[size + 1], 0);
  uint8_t level(0);
  uint64_t weight(1);
  while (level < num_levels_) {
    const auto from_index = levels_[level];
    const auto to_index = levels_[level + 1]; // exclusive
    if ((level == 0) and !is_level_zero_sorted_) {
      increment_buckets_unsorted_level(from_index, to_index, weight, split_points, size, buckets.get());
    } else {
      increment_buckets_sorted_level(from_index, to_index, weight, split_points, size, buckets.get());
    }
    level++;
    weight *= 2;
  }
  // normalize and, if CDF, convert to cumulative
  if (is_CDF) {
    double subtotal = 0;
    for (uint32_t i = 0; i <= size; i++) {
      subtotal += buckets[i];
      buckets[i] = subtotal / n_;
    }
  } else {
    for (uint32_t i = 0; i <= size; i++) {
      buckets[i] /= n_;
    }
  }
  return std::move(buckets);
}

void kll_sketch::increment_buckets_unsorted_level(uint32_t from_index, uint32_t to_index, uint64_t weight,
    const float* split_points, uint32_t size, double* buckets) const
{
  for (uint32_t i = from_index; i < to_index; i++) {
    uint32_t j;
    for (j = 0; j < size; j++) {
      if (items_[i] < split_points[j]) {
        break;
      }
    }
    buckets[j] += weight;
  }
}

void kll_sketch::increment_buckets_sorted_level(uint32_t from_index, uint32_t to_index, uint64_t weight,
    const float* split_points, uint32_t size, double* buckets) const
{
  uint32_t i = from_index;
  uint32_t j = 0;
  while ((i <  to_index) and (j < size)) {
    if (items_[i] < split_points[j]) {
      buckets[j] += weight; // this sample goes into this bucket
      i++; // move on to next sample and see whether it also goes into this bucket
    } else {
      j++; // no more samples for this bucket
    }
  }
  // now either i == to_index (we are out of samples), or
  // j == size (we are out of buckets, but there are more samples remaining)
  // we only need to do something in the latter case
  if (j == size) {
    buckets[j] += weight * (to_index - i);
  }
}

void kll_sketch::merge_higher_levels(const kll_sketch& other, uint64_t final_n) {
  const uint32_t tmp_space_needed(get_num_retained() + other.get_num_retained_above_level_zero());
  const std::unique_ptr<float[]> workbuf(new float[tmp_space_needed]);
  const uint8_t ub = kll_helper::ub_on_num_levels(final_n);
  const std::unique_ptr<uint32_t[]> worklevels(new uint32_t[ub + 2]); // ub+1 does not work
  const std::unique_ptr<uint32_t[]> outlevels(new uint32_t[ub + 2]);

  const uint8_t provisional_num_levels = std::max(num_levels_, other.num_levels_);

  populate_work_arrays(other, workbuf.get(), worklevels.get(), provisional_num_levels);

  // notice that workbuf is being used as both the input and output here
  const kll_helper::compress_result result = kll_helper::general_compress(k_, m_, provisional_num_levels, workbuf.get(),
      worklevels.get(), workbuf.get(), outlevels.get(), is_level_zero_sorted_);
  const uint8_t final_num_levels = result.final_num_levels;
  const uint32_t final_capacity = result.final_capacity;
  const uint32_t final_pop = result.final_pop;

  assert (final_num_levels <= ub); // can sometimes be much bigger

  // now we need to transfer the results back into the "self" sketch
  if (final_capacity != items_size_) {
    delete [] items_;
    items_ = new float[final_capacity];
  }
  const uint32_t free_space_at_bottom = final_capacity - final_pop;
  std::copy(&workbuf[outlevels[0]], &workbuf[outlevels[0] + final_pop], &items_[free_space_at_bottom]);
  const uint32_t the_shift(free_space_at_bottom - outlevels[0]);

  if (levels_size_ < (final_num_levels + 1)) {
    delete [] levels_;
    levels_ = new uint32_t[final_num_levels + 1];
  }

  for (uint8_t lvl = 0; lvl < (final_num_levels + 1); lvl++) { // includes the "extra" index
    levels_[lvl] = outlevels[lvl] + the_shift;
  }

  num_levels_ = final_num_levels;
}

void kll_sketch::populate_work_arrays(const kll_sketch& other, float* workbuf, uint32_t* worklevels, uint8_t provisional_num_levels) {
  worklevels[0] = 0;

  // Note: the level zero data from "other" was already inserted into "self"
  const uint32_t self_pop_zero(safe_level_size(0));
  std::copy(&items_[levels_[0]], &items_[levels_[0] + self_pop_zero], &workbuf[worklevels[0]]);
  worklevels[1] = worklevels[0] + self_pop_zero;

  for (uint8_t lvl = 1; lvl < provisional_num_levels; lvl++) {
    const uint32_t self_pop = safe_level_size(lvl);
    const uint32_t other_pop = other.safe_level_size(lvl);
    worklevels[lvl + 1] = worklevels[lvl] + self_pop + other_pop;

    if ((self_pop > 0) and (other_pop == 0)) {
      std::copy(&items_[levels_[lvl]], &items_[levels_[lvl] + self_pop], &workbuf[worklevels[lvl]]);
    } else if ((self_pop == 0) and (other_pop > 0)) {
      std::copy(&other.items_[other.levels_[lvl]], &other.items_[other.levels_[lvl] + other_pop], &workbuf[worklevels[lvl]]);
    } else if ((self_pop > 0) and (other_pop > 0)) {
      kll_helper::merge_sorted_arrays(items_, levels_[lvl], self_pop, other.items_,
          other.levels_[lvl], other_pop, workbuf, worklevels[lvl]);
    }
  }
}

void kll_sketch::assert_correct_total_weight() const {
  const uint64_t total(kll_helper::sum_the_sample_weights(num_levels_, levels_));
  assert (total == n_);
}

uint32_t kll_sketch::safe_level_size(uint8_t level) const {
  if (level >= num_levels_) return 0;
  return levels_[level + 1] - levels_[level];
}

uint32_t kll_sketch::get_num_retained_above_level_zero() const {
  if (num_levels_ == 1) return 0;
  return levels_[num_levels_] - levels_[1];
}

std::ostream& operator<<(std::ostream& os, kll_sketch const& sketch) {
  os << "### KLL sketch summary:" << std::endl;
  os << "   K              : " << sketch.k_ << std::endl;
  os << "   min K          : " << sketch.min_k_ << std::endl;
  os << "   M              : " << (unsigned int) sketch.m_ << std::endl;
  os << "   N              : " << sketch.n_ << std::endl;
  os << "   Epsilon        : " << std::setprecision(3) << sketch.get_normalized_rank_error(false) * 100 << "%" << std::endl;
  os << "   Epsilon PMF    : " << sketch.get_normalized_rank_error(true) * 100 << "%" << std::endl;
  os << "   Empty          : " << (sketch.is_empty() ? "true" : "false") << std::endl;
  os << "   Estimation mode: " << (sketch.is_estimation_mode() ? "true" : "false") << std::endl;
  os << "   Levels         : " << (unsigned int) sketch.num_levels_ << std::endl;
  os << "   Sorted         : " << (sketch.is_level_zero_sorted_ ? "true" : "false") << std::endl;
  os << "   Capacity items : " << sketch.items_size_ << std::endl;
  os << "   Retained items : " << sketch.get_num_retained() << std::endl;
  os << "   Storage bytes  : " << sketch.get_serialized_size_bytes() << std::endl;
  os << "   Min value      : " << sketch.get_min_value() << std::endl;
  os << "   Max value      : " << sketch.get_max_value() << std::endl;
  os << "### End sketch summary" << std::endl;
  return os;
}

// the last integer in the levels_ array is not serialized because it can be derived
// +2 items for min and max
uint32_t kll_sketch::get_serialized_size_bytes(uint8_t num_levels, uint32_t num_retained) {
  return DATA_START + (num_levels * sizeof(uint32_t)) + ((num_retained + 2) * sizeof(float));
}

}
