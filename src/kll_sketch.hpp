/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef KLL_SKETCH_HPP_
#define KLL_SKETCH_HPP_

#include <ostream>
#include <memory>
#include <limits>
#include <iostream>
#include <iomanip>

#include "kll_quantile_calculator.hpp"
#include "kll_helper.hpp"

namespace datasketches {

/*
 * Implementation of a very compact quantiles sketch with lazy compaction scheme
 * and nearly optimal accuracy per retained item.
 * See <a href="https://arxiv.org/abs/1603.05346v2">Optimal Quantile Approximation in Streams</a>.
 *
 * <p>This is a stochastic streaming sketch that enables near-real time analysis of the
 * approximate distribution of values from a very large stream in a single pass, requiring only
 * that the values are comparable.
 * The analysis is obtained using <i>get_quantile()</i> or <i>get_quantiles()</i> functions or the
 * inverse functions get_rank(), get_PMF() (Probability Mass Function), and get_CDF()
 * (Cumulative Distribution Function).
 *
 * <p>Given an input stream of <i>N</i> numeric values, the <i>absolute rank</i> of any specific
 * value is defined as its index <i>(0 to N-1)</i> in the hypothetical sorted stream of all
 * <i>N</i> input values.
 *
 * <p>The <i>normalized rank</i> (<i>rank</i>) of any specific value is defined as its
 * <i>absolute rank</i> divided by <i>N</i>.
 * Thus, the <i>normalized rank</i> is a value between zero and one.
 * In the documentation for this sketch <i>absolute rank</i> is never used so any
 * reference to just <i>rank</i> should be interpreted to mean <i>normalized rank</i>.
 *
 * <p>This sketch is configured with a parameter <i>k</i>, which affects the size of the sketch
 * and its estimation error.
 *
 * <p>The estimation error is commonly called <i>epsilon</i> (or <i>eps</i>) and is a fraction
 * between zero and one. Larger values of <i>k</i> result in smaller values of epsilon.
 * Epsilon is always with respect to the rank and cannot be applied to the
 * corresponding values.
 *
 * <p>The relationship between the normalized rank and the corresponding values can be viewed
 * as a two dimensional monotonic plot with the normalized rank on one axis and the
 * corresponding values on the other axis. If the y-axis is specified as the value-axis and
 * the x-axis as the normalized rank, then <i>y = get_quantile(x)</i> is a monotonically
 * increasing function.
 *
 * <p>The functions <i>get_quantile(rank)</i> and get_quantiles(...) translate ranks into
 * corresponding values. The functions <i>get_rank(value),
 * get_CDF(...) (Cumulative Distribution Function), and get_PMF(...)
 * (Probability Mass Function)</i> perform the opposite operation and translate values into ranks.
 *
 * <p>The <i>getPMF(...)</i> function has about 13 to 47% worse rank error (depending
 * on <i>k</i>) than the other queries because the mass of each "bin" of the PMF has
 * "double-sided" error from the upper and lower edges of the bin as a result of a subtraction,
 * as the errors from the two edges can sometimes add.
 *
 * <p>The default <i>k</i> of 200 yields a "single-sided" epsilon of about 1.33% and a
 * "double-sided" (PMF) epsilon of about 1.65%.
 *
 * <p>A <i>get_quantile(rank)</i> query has the following guarantees:
 * <ul>
 * <li>Let <i>v = get_quantile(r)</i> where <i>r</i> is the rank between zero and one.</li>
 * <li>The value <i>v</i> will be a value from the input stream.</li>
 * <li>Let <i>trueRank</i> be the true rank of <i>v</i> derived from the hypothetical sorted
 * stream of all <i>N</i> values.</li>
 * <li>Let <i>eps = get_normalized_rank_error(false)</i>.</li>
 * <li>Then <i>r - eps &le; trueRank &le; r + eps</i> with a confidence of 99%. Note that the
 * error is on the rank, not the value.</li>
 * </ul>
 *
 * <p>A <i>get_rank(value)</i> query has the following guarantees:
 * <ul>
 * <li>Let <i>r = get_rank(v)</i> where <i>v</i> is a value between the min and max values of
 * the input stream.</li>
 * <li>Let <i>true_rank</i> be the true rank of <i>v</i> derived from the hypothetical sorted
 * stream of all <i>N</i> values.</li>
 * <li>Let <i>eps = get_normalized_rank_error(false)</i>.</li>
 * <li>Then <i>r - eps &le; trueRank &le; r + eps</i> with a confidence of 99%.</li>
 * </ul>
 *
 * <p>A <i>get_PMF()</i> query has the following guarantees:
 * <ul>
 * <li>Let <i>{r1, r2, ..., r(m+1)} = get_PMF(v1, v2, ..., vm)</i> where <i>v1, v2</i> are values
 * between the min and max values of the input stream.
 * <li>Let <i>mass<sub>i</sub> = estimated mass between v<sub>i</sub> and v<sub>i+1</sub></i>.</li>
 * <li>Let <i>trueMass</i> be the true mass between the values of <i>v<sub>i</sub>,
 * v<sub>i+1</sub></i> derived from the hypothetical sorted stream of all <i>N</i> values.</li>
 * <li>Let <i>eps = get_normalized_rank_error(true)</i>.</li>
 * <li>then <i>mass - eps &le; trueMass &le; mass + eps</i> with a confidence of 99%.</li>
 * <li>r(m+1) includes the mass of all points larger than vm.</li>
 * </ul>
 *
 * <p>A <i>get_CDF(...)</i> query has the following guarantees;
 * <ul>
 * <li>Let <i>{r1, r2, ..., r(m+1)} = get_CDF(v1, v2, ..., vm)</i> where <i>v1, v2</i> are values
 * between the min and max values of the input stream.
 * <li>Let <i>mass<sub>i</sub> = r<sub>i+1</sub> - r<sub>i</sub></i>.</li>
 * <li>Let <i>trueMass</i> be the true mass between the true ranks of <i>v<sub>i</sub>,
 * v<sub>i+1</sub></i> derived from the hypothetical sorted stream of all <i>N</i> values.</li>
 * <li>Let <i>eps = get_normalized_rank_error(true)</i>.</li>
 * <li>then <i>mass - eps &le; trueMass &le; mass + eps</i> with a confidence of 99%.</li>
 * <li>1 - r(m+1) includes the mass of all points larger than vm.</li>
 * </ul>
 *
 * <p>From the above, it might seem like we could make some estimates to bound the
 * <em>value</em> returned from a call to <em>get_quantile()</em>. The sketch, however, does not
 * let us derive error bounds or confidences around values. Because errors are independent, we
 * can approximately bracket a value as shown below, but there are no error estimates available.
 * Additionally, the interval may be quite large for certain distributions.
 * <ul>
 * <li>Let <i>v = get_quantile(r)</i>, the estimated quantile value of rank <i>r</i>.</li>
 * <li>Let <i>eps = get_normalized_rank_error(false)</i>.</li>
 * <li>Let <i>v<sub>lo</sub></i> = estimated quantile value of rank <i>(r - eps)</i>.</li>
 * <li>Let <i>v<sub>hi</sub></i> = estimated quantile value of rank <i>(r + eps)</i>.</li>
 * <li>Then <i>v<sub>lo</sub> &le; v &le; v<sub>hi</sub></i>, with 99% confidence.</li>
 * </ul>
 *
 * author Kevin Lang
 * author Alexander Saydakov
 * author Lee Rhodes
 */

// this should work for fixed size types, but needs to be specialized for other types
template <typename T>
void serialize_items(std::ostream& os, const T* items, unsigned num) {
  os.write((char*)items, sizeof(T) * num);
}

 // this should work for fixed size types, but needs to be specialized for other types
template <typename T>
void deserialize_items(std::istream& is, T* items, unsigned num) {
  is.read((char*)items, sizeof(T) * num);
}


template <typename T>
class kll_sketch {
  public:
    static const uint8_t DEFAULT_M = 8;
    static const uint16_t DEFAULT_K = 200;
    static const uint16_t MIN_K = DEFAULT_M;
    static const uint16_t MAX_K = (1 << 16) - 1; // serialized as an uint16_t

    explicit kll_sketch(uint16_t k = DEFAULT_K) {
      if (k < MIN_K or k > MAX_K) {
        throw std::invalid_argument("K must be >= " + std::to_string(MIN_K) + " and <= " + std::to_string(MAX_K) + ": " + std::to_string(k));
      }
      k_ = k;
      m_ = DEFAULT_M;
      min_k_ = k;
      n_ = 0;
      num_levels_ = 1;
      levels_size_ = 2;
      levels_ = new uint32_t[2] {k_, k_};
      items_size_ = k_;
      items_ = new T[k_];
      if (std::is_floating_point<T>::value) {
        min_value_ = std::numeric_limits<T>::quiet_NaN();
        max_value_ = std::numeric_limits<T>::quiet_NaN();
      }
      is_level_zero_sorted_ = false;
    }

    ~kll_sketch() {
      delete [] levels_;
      delete [] items_;
    }

    void update(T value) {
      if (is_empty()) {
        min_value_ = value;
        max_value_ = value;
      } else {
        if (value < min_value_) min_value_ = value;
        if (max_value_ < value) max_value_ = value;
      }
      if (levels_[0] == 0) compress_while_updating();
      n_++;
      is_level_zero_sorted_ = false;
      const uint32_t next_pos(levels_[0] - 1);
      levels_[0] = next_pos;
      items_[next_pos] = value;
    }

    void merge(const kll_sketch& other) {
      if (other.is_empty()) return;
      if (m_ != other.m_) {
        throw std::invalid_argument("incompatible M: " + std::to_string(m_) + " and " + std::to_string(other.m_));
      }
      const uint64_t final_n(n_ + other.n_);
      for (uint32_t i = other.levels_[0]; i < other.levels_[1]; i++) {
        update(other.items_[i]);
      }
      if (other.num_levels_ >= 2) {
        merge_higher_levels(other, final_n);
      }
      n_ = final_n;
      min_k_ = std::min(min_k_, other.min_k_);
      if (is_empty() or other.min_value_ < min_value_) min_value_ = other.min_value_;
      if (is_empty() or max_value_ < other.max_value_) max_value_ = other.max_value_;
      assert_correct_total_weight();
    }

    bool is_empty() const {
      return n_ == 0;
    }

    uint64_t get_n() const {
      return n_;
    }

    uint32_t get_num_retained() const {
      return levels_[num_levels_] - levels_[0];
    }

    bool is_estimation_mode() const {
      return num_levels_ > 1;
    }

    T get_min_value() const {
      if (is_empty()) {
        if (std::is_floating_point<T>::value) {
          return std::numeric_limits<T>::quiet_NaN();
        }
        throw std::runtime_error("getting quantiles from empty sketch is not supported for this type of values");
      }
      return min_value_;
    }

    T get_max_value() const {
      if (is_empty()) {
        if (std::is_floating_point<T>::value) {
          return std::numeric_limits<T>::quiet_NaN();
        }
        throw std::runtime_error("getting quantiles from empty sketch is not supported for this type of values");
      }
      return max_value_;
    }

    T get_quantile(double fraction) const {
      if (is_empty()) {
        if (std::is_floating_point<T>::value) {
          return std::numeric_limits<T>::quiet_NaN();
        }
        throw std::runtime_error("getting quantiles from empty sketch is not supported for this type of values");
      }
      if (fraction == 0.0) return min_value_;
      if (fraction == 1.0) return max_value_;
      if ((fraction < 0.0) or (fraction > 1.0)) {
        throw std::invalid_argument("Fraction cannot be less than zero or greater than 1.0");
      }
      // has side effect of sorting level zero if needed
      auto quantile_calculator(const_cast<kll_sketch<T>*>(this)->get_quantile_calculator());
      return quantile_calculator->get_quantile(fraction);
    }

    std::unique_ptr<T[]> get_quantiles(const double* fractions, uint32_t size) const {
      if (is_empty()) { return nullptr; }
      std::unique_ptr<kll_quantile_calculator<T>> quantile_calculator;
      std::unique_ptr<T[]> quantiles(new T[size]);
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

    double get_rank(T value) const {
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

    std::unique_ptr<double[]> get_PMF(const T* split_points, uint32_t size) const {
      return get_PMF_or_CDF(split_points, size, false);
    }

    std::unique_ptr<double[]> get_CDF(const T* split_points, uint32_t size) const {
      return get_PMF_or_CDF(split_points, size, true);
    }

    double get_normalized_rank_error(bool pmf) const {
      return get_normalized_rank_error(min_k_, pmf);
    }

    // this needs to be specialized to return correct size if sizeof(T) doesn't match the actual serialized size
    uint32_t get_serialized_size_bytes() const {
      if (is_empty()) { return EMPTY_SIZE_BYTES; }
      // the last integer in the levels_ array is not serialized because it can be derived
      // +2 items for min and max
      return DATA_START + (num_levels_ * sizeof(uint32_t)) + ((get_num_retained() + 2) * sizeof(T));
    }

    void serialize(std::ostream& os) const {
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
      serialize_items<T>(os, &min_value_, 1);
      serialize_items<T>(os, &max_value_, 1);
      serialize_items<T>(os, &items_[levels_[0]], get_num_retained());
    }

    static std::unique_ptr<kll_sketch<T>> deserialize(std::istream& is) {
      uint8_t preamble_ints;
      is.read((char*)&preamble_ints, sizeof(preamble_ints));
      uint8_t serial_version;
      is.read((char*)&serial_version, sizeof(serial_version));
      uint8_t family_id;
      is.read((char*)&family_id, sizeof(family_id));
      uint8_t flags;
      is.read((char*)&flags, sizeof(flags));
      uint16_t k;
      is.read((char*)&k, sizeof(k));
      uint8_t m;
      is.read((char*)&m, sizeof(m));
      uint8_t unused;
      is.read((char*)&unused, sizeof(unused));

      if (m != DEFAULT_M) {
        throw std::invalid_argument("Possible corruption: M must be " + std::to_string(DEFAULT_M)
            + ": " + std::to_string(m));
      }
      const bool is_empty(flags & (1 << kll_flags::IS_EMPTY));
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

      std::unique_ptr<kll_sketch<T>> sketch_ptr(is_empty ? new kll_sketch<T>(k) : new kll_sketch<T>(k, flags, is));
      return std::move(sketch_ptr);
    }

    /*
     * Gets the normalized rank error given k and pmf.
     * k - the configuration parameter
     * pmf - if true, returns the "double-sided" normalized rank error for the get_PMF() function.
     * Otherwise, it is the "single-sided" normalized rank error for all the other queries.
     * Constants were derived as the best fit to 99 percentile empirically measured max error in thousands of trials
     */
    static double get_normalized_rank_error(uint16_t k, bool pmf) {
      return pmf
          ? 2.446 / pow(k, 0.9433)
          : 2.296 / pow(k, 0.9723);
    }

    template <typename TT>
    friend std::ostream& operator<<(std::ostream& os, kll_sketch<TT> const& sketch);

#ifdef KLL_VALIDATION
    uint8_t get_num_levels() { return num_levels_; }
    uint32_t* get_levels() { return levels_; }
    T* get_items() { return items_; }
#endif

  private:
    /* Serialized sketch layout:
     *  Adr:
     *      ||    7    |   6   |    5   |    4   |    3   |    2    |    1   |      0       |
     *  0   || unused  |   M   |--------K--------|  Flags |  FamID  | SerVer | PreambleInts |
     *      ||   15    |   14  |   13   |   12   |   11   |   10    |    9   |      8       |
     *  1   ||-----------------------------------N------------------------------------------|
     *      ||   23    |   22  |   21   |   20   |   19   |    18   |   17   |      16      |
     *  2   ||---------------data----------------|--------|numLevels|-------min K-----------|
     */

    static const size_t EMPTY_SIZE_BYTES = 8;
    static const size_t DATA_START = 20;

    static const uint8_t SERIAL_VERSION = 1;
    static const uint8_t FAMILY = 15;

    enum kll_flags { IS_EMPTY, IS_LEVEL_ZERO_SORTED };

    static const uint8_t PREAMBLE_INTS_EMPTY = 2;
    static const uint8_t PREAMBLE_INTS_NONEMPTY = 5;

    uint16_t k_;
    uint8_t m_; // minimum buffer "width"
    uint16_t min_k_; // for error estimation after merging with different k
    uint64_t n_;
    uint8_t num_levels_;
    uint32_t* levels_;
    uint8_t levels_size_;
    T* items_;
    uint32_t items_size_;
    T min_value_;
    T max_value_;
    bool is_level_zero_sorted_;

    // for deserialization
    kll_sketch(uint16_t k, uint8_t flags, std::istream& is) {
      k_ = k;
      m_ = DEFAULT_M;
      is.read((char*)&n_, sizeof(n_));
      is.read((char*)&min_k_, sizeof(min_k_));
      is.read((char*)&num_levels_, sizeof(num_levels_));
      uint8_t unused;
      is.read((char*)&unused, sizeof(unused));
      levels_ = new uint32_t[num_levels_ + 1];
      levels_size_ = num_levels_ + 1;
      // the last integer in levels_ is not serialized because it can be derived
      is.read((char*)levels_, sizeof(levels_[0]) * num_levels_);
      const uint32_t capacity(kll_helper::compute_total_capacity(k_, m_, num_levels_));
      levels_[num_levels_] = capacity;
      deserialize_items<T>(is, &min_value_, 1);
      deserialize_items<T>(is, &max_value_, 1);
      items_ = new T[capacity];
      items_size_ = capacity;
      const auto num_items(levels_[num_levels_] - levels_[0]);
      deserialize_items<T>(is, &items_[levels_[0]], num_items);
      is_level_zero_sorted_ = (flags & (1 << kll_flags::IS_LEVEL_ZERO_SORTED)) > 0;
    }

    // The following code is only valid in the special case of exactly reaching capacity while updating.
    // It cannot be used while merging, while reducing k, or anything else.
    void compress_while_updating(void) {
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

    uint8_t find_level_to_compact() const {
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

    void add_empty_top_level_to_completely_full_sketch() {
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

      T* new_buf(new T[new_total_cap]);

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

    void sort_level_zero() {
      if (!is_level_zero_sorted_) {
        std::sort(&items_[levels_[0]], &items_[levels_[1]]);
        is_level_zero_sorted_ = true;
      }
    }

    std::unique_ptr<kll_quantile_calculator<T>> get_quantile_calculator() {
      sort_level_zero();
      std::unique_ptr<kll_quantile_calculator<T>> quantile_calculator(new kll_quantile_calculator<T>(items_, levels_, num_levels_, n_));
      return std::move(quantile_calculator);
    }

    std::unique_ptr<double[]> get_PMF_or_CDF(const T* split_points, uint32_t size, bool is_CDF) const {
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

    void increment_buckets_unsorted_level(uint32_t from_index, uint32_t to_index, uint64_t weight,
        const T* split_points, uint32_t size, double* buckets) const
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

    void increment_buckets_sorted_level(uint32_t from_index, uint32_t to_index, uint64_t weight,
        const T* split_points, uint32_t size, double* buckets) const
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

    void merge_higher_levels(const kll_sketch& other, uint64_t final_n) {
      const uint32_t tmp_space_needed(get_num_retained() + other.get_num_retained_above_level_zero());
      const std::unique_ptr<T[]> workbuf(new T[tmp_space_needed]);
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
        items_ = new T[final_capacity];
        items_size_ = final_capacity;
      }
      const uint32_t free_space_at_bottom = final_capacity - final_pop;
      std::copy(&workbuf[outlevels[0]], &workbuf[outlevels[0] + final_pop], &items_[free_space_at_bottom]);
      const uint32_t the_shift(free_space_at_bottom - outlevels[0]);

      if (levels_size_ < (final_num_levels + 1)) {
        delete [] levels_;
        levels_ = new uint32_t[final_num_levels + 1];
        levels_size_ = final_num_levels + 1;
      }

      for (uint8_t lvl = 0; lvl < (final_num_levels + 1); lvl++) { // includes the "extra" index
        levels_[lvl] = outlevels[lvl] + the_shift;
      }

      num_levels_ = final_num_levels;
    }

    void populate_work_arrays(const kll_sketch& other, T* workbuf, uint32_t* worklevels, uint8_t provisional_num_levels) {
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

    void assert_correct_total_weight() const {
      const uint64_t total(kll_helper::sum_the_sample_weights(num_levels_, levels_));
      assert (total == n_);
    }

    uint32_t safe_level_size(uint8_t level) const {
      if (level >= num_levels_) return 0;
      return levels_[level + 1] - levels_[level];
    }

    uint32_t get_num_retained_above_level_zero() const {
      if (num_levels_ == 1) return 0;
      return levels_[num_levels_] - levels_[1];
    }

};

template <typename T>
std::ostream& operator<<(std::ostream& os, kll_sketch<T> const& sketch) {
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
  using std::to_string; // to allow overriding to_string() for a custom type
  if (!sketch.is_empty()) {
    os << "   Min value      : " << to_string(sketch.get_min_value()) << std::endl;
    os << "   Max value      : " << to_string(sketch.get_max_value()) << std::endl;
  }
  os << "### End sketch summary" << std::endl;

  // for debugging
  const bool with_levels(false);
  const bool with_data(false);

  if (with_levels) {
    os << "### KLL sketch levels:" << std::endl;
    os << "   index: nominal capacity, actual size" << std::endl;
    for (uint8_t i = 0; i < sketch.num_levels_; i++) {
      os << "   " << (unsigned int) i << ": " << kll_helper::level_capacity(sketch.k_, sketch.num_levels_, i, sketch.m_) << ", " << sketch.safe_level_size(i) << std::endl;
    }
    os << "### End sketch levels" << std::endl;
  }

  if (with_data) {
    os << "### KLL sketch data:" << std::endl;
    uint8_t level(0);
    while (level < sketch.num_levels_) {
      const uint32_t from_index = sketch.levels_[level];
      const uint32_t to_index = sketch.levels_[level + 1]; // exclusive
      if (from_index < to_index) {
        os << " level " << (unsigned int) level << ":" << std::endl;
      }
      for (uint32_t i = from_index; i < to_index; i++) {
        os << "   " << to_string(sketch.items_[i]) << std::endl;
      }
      level++;
    }
    os << "### End sketch data" << std::endl;
  }

  return os;
}

} /* namespace datasketches */

#endif
