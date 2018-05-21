/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef KLL_SKETCH_HPP_
#define KLL_SKETCH_HPP_

#include <ostream>
#include <memory>

#include "kll_quantile_calculator.hpp"

namespace sketches {

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
 * In the documentation and Javadocs for this sketch <i>absolute rank</i> is never used so any
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

class kll_sketch {
  public:
    static const uint16_t DEFAULT_K = 200;

    kll_sketch(uint16_t k = DEFAULT_K);
    ~kll_sketch();
    void update(float value);
    void merge(const kll_sketch& other);
    bool is_empty() const;
    uint64_t get_n() const;
    uint32_t get_num_retained() const;
    bool is_estimation_mode() const;
    float get_min_value() const;
    float get_max_value() const;
    float get_quantile(double fraction) const;
    std::unique_ptr<float[]> get_quantiles(const double* fractions, size_t size) const;
    double get_rank(float vlaue) const;
    std::unique_ptr<double[]> get_PMF(const float* split_points, size_t size) const;
    std::unique_ptr<double[]> get_CDF(const float* split_points, size_t size) const;
    double get_normalized_rank_error(bool pmf) const;
    uint32_t get_serialized_size_bytes() const;
    void serialize(std::ostream& os) const;

    static std::unique_ptr<kll_sketch> deserialize(std::istream& is);
    static double get_normalized_rank_error(uint16_t k, bool pmf);

    friend std::ostream& operator<<(std::ostream& os, kll_sketch const& sketch);

  private:
    static const uint8_t DEFAULT_M = 8;
    static const uint16_t MIN_K = DEFAULT_M;
    static const uint16_t MAX_K = (1 << 16) - 1; // serialized as an uint16_t

    /* Serialized sketch layout:
     *  Adr:
     *      ||    7    |   6   |    5   |    4   |    3   |    2    |    1   |      0       |
     *  0   || unused  |   M   |--------K--------|  Flags |  FamID  | SerVer | PreambleInts |
     *      ||   15    |   14  |   13   |   12   |   11   |   10    |    9   |      8       |
     *  1   ||---------------------------------N_LONG---------------------------------------|
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
    float* items_;
    uint32_t items_size_;
    float min_value_;
    float max_value_;
    bool is_level_zero_sorted_;

    void compress_while_updating();
    uint8_t find_level_to_compact() const;
    void add_empty_top_level_to_completely_full_sketch();
    void sort_level_zero();
    std::unique_ptr<kll_quantile_calculator> get_quantile_calculator();
    std::unique_ptr<double[]> get_PMF_or_CDF(const float* split_points, size_t size, bool is_CDF) const;
    void increment_buckets_unsorted_level(uint32_t from_index, uint32_t to_index, uint64_t weight, const float* split_points, size_t size, double* buckets) const;
    void increment_buckets_sorted_level(uint32_t from_index, uint32_t to_index, uint64_t weight, const float* split_points, size_t size, double* buckets) const;
    void merge_higher_levels(const kll_sketch& other, uint64_t final_n);
    void populate_work_arrays(const kll_sketch& other, float* workbuf, uint32_t* worklevels, uint8_t provisional_num_levels);
    void assert_correct_total_weight() const;
    uint32_t safe_level_size(uint8_t level) const;
    uint32_t get_num_retained_above_level_zero() const;

    static uint32_t get_serialized_size_bytes(uint8_t num_levels, uint32_t num_retained);
};

}

#endif
