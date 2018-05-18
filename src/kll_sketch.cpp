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
  const size_t nextPos(levels_[0] - 1);
  levels_[0] = nextPos;
  items_[nextPos] = value;
}

void kll_sketch::merge(const kll_sketch& other) {

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

std::unique_ptr<float[]> kll_sketch::get_quantiles(const double* fractions, size_t size) const {
  if (is_empty()) { return nullptr; }
  std::unique_ptr<kll_quantile_calculator> quantile_calculator;
  std::unique_ptr<float[]> quantiles(new float[size]);
  for (size_t i = 0; i < size; i++) {
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

  // TODO: checking

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
  const bool is_empty = flags & (11 << kll_flags::IS_EMPTY);
  uint8_t unused;
  is.read((char*)&unused, sizeof(unused));
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

  const size_t rawBeg(levels_[level]);
  const size_t rawLim(levels_[level + 1]);
  // +2 is OK because we already added a new top level if necessary
  const size_t popAbove(levels_[level + 2] - rawLim);
  const size_t rawPop(rawLim - rawBeg);
  const bool oddPop(kll_helper::is_odd(rawPop));
  const size_t adjBeg(oddPop ? rawBeg + 1 : rawBeg);
  const size_t adjPop(oddPop ? rawPop - 1 : rawPop);
  const size_t halfAdjPop(adjPop / 2);

  // level zero might not be sorted, so we must sort it if we wish to compact it
  if (level == 0) {
    std::sort(&items_[adjBeg], &items_[adjBeg + adjPop]);
  }
  if (popAbove == 0) {
    kll_helper::randomly_halve_up(items_, adjBeg, adjPop);
  } else {
    kll_helper::randomly_halve_down(items_, adjBeg, adjPop);
    kll_helper::merge_sorted_arrays(items_, adjBeg, halfAdjPop, items_, rawLim, popAbove, items_, adjBeg + halfAdjPop);
  }
  levels_[level + 1] -= halfAdjPop; // adjust boundaries of the level above
  if (oddPop) {
    levels_[level] = levels_[level + 1] - 1; // the current level now contains one item
    items_[levels_[level]] = items_[rawBeg]; // namely this leftover guy
  } else {
    levels_[level] = levels_[level + 1]; // the current level is now empty
  }

  // verify that we freed up halfAdjPop array slots just below the current level
  assert (levels_[level] == (rawBeg + halfAdjPop));

  // finally, we need to shift up the data in the levels below
  // so that the freed-up space can be used by level zero
  if (level > 0) {
    const size_t amount(rawBeg - levels_[0]);
    const float* src_beg(&items_[levels_[0]]);
    const float* src_end(&items_[levels_[0] + amount]);
    float* dst_end(&items_[levels_[0] + halfAdjPop + amount]);
    std::copy_backward(src_beg, src_end, dst_end);
    for (size_t lvl = 0; lvl < level; lvl++) {
      levels_[lvl] += halfAdjPop;
    }
  }
}

uint8_t kll_sketch::find_level_to_compact() const {
  uint8_t level(0);
  while (true) {
    assert (level < num_levels_);
    const size_t pop(levels_[level + 1] - levels_[level]);
    const size_t cap(kll_helper::level_capacity(k_, num_levels_, level, m_));
    if (pop >= cap) {
      return level;
    }
    level++;
  }
}

void kll_sketch::add_empty_top_level_to_completely_full_sketch() {
  const size_t cur_total_cap(levels_[num_levels_]);

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

  const size_t delta_cap(kll_helper::level_capacity(k_, num_levels_ + 1, 0, m_));
  const size_t new_total_cap(cur_total_cap + delta_cap);

  float* new_buf(new float[new_total_cap]);

  // copy (and shift) the current data into the new buffer
  std::copy(&items_[levels_[0]], &items_[levels_[0] + cur_total_cap], &new_buf[levels_[0] + delta_cap]);
  delete [] items_;
  items_ = new_buf;
  items_size_ = new_total_cap;

  // this loop includes the old "extra" index at the top
  for (size_t i = 0; i <= num_levels_; i++) {
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
