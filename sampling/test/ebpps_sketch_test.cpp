/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <ebpps_sketch.hpp>
#include <var_opt_sketch.hpp>

#include <catch2/catch.hpp>

#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <cmath>
#include <random>
#include <stdexcept>

// TODO: remove when done testing
#include <iomanip>
#include <algorithm>

#ifdef TEST_BINARY_INPUT_PATH
static std::string testBinaryInputPath = TEST_BINARY_INPUT_PATH;
#else
static std::string testBinaryInputPath = "test/";
#endif

namespace datasketches {

static constexpr double EPS = 1e-13;

static ebpps_sketch<int> create_unweighted_sketch(uint32_t k, uint64_t n) {
  ebpps_sketch<int> sk(k);
  for (uint64_t i = 0; i < n; ++i) {
    sk.update(static_cast<int>(i), 1.0);
  }
  return sk;
}

template<typename T, typename A>
static void check_if_equal(ebpps_sketch<T, A>& sk1, ebpps_sketch<T, A>& sk2) {
  REQUIRE(sk1.get_k() == sk2.get_k());
  REQUIRE(sk1.get_n() == sk2.get_n());
  REQUIRE(sk1.get_num_samples() == sk2.get_num_samples());

  auto it1 = sk1.begin();
  auto it2 = sk2.begin();

  while ((it1 != sk1.end()) && (it2 != sk2.end())) {
    auto p1 = *it1;
    auto p2 = *it2;
    REQUIRE(p1.first == p2.first);   // data values
    REQUIRE(p1.second == p2.second); // weights
    ++it1;
    ++it2;
  }

  REQUIRE((it1 == sk1.end() && it2 == sk2.end())); // iterators must end at the same time
}

TEST_CASE("ebpps sketch: invalid k", "[ebpps_sketch]") {
  REQUIRE_THROWS_AS(ebpps_sketch<int>(0), std::invalid_argument);
  REQUIRE_THROWS_AS(ebpps_sketch<int>(ebpps_constants::MAX_K + 1), std::invalid_argument);
}
/*
TEST_CASE("ebpps sketch: insert items", "[ebpps_sketch]") {
  size_t n = 0;
  uint32_t k = 5;
  ebpps_sketch<int> sk = create_unweighted_sketch(k, n);
  REQUIRE(sk.is_empty());

  n = k;
  sk = create_unweighted_sketch(k, n);
  REQUIRE_FALSE(sk.is_empty());
  REQUIRE(sk.get_n() == n);
  REQUIRE(sk.get_cumulative_weight() == static_cast<double>(n));
  for (int val : sk.get_result())
    REQUIRE(val < static_cast<int>(n));

  n = k * 10;
  sk = create_unweighted_sketch(k, n);
  REQUIRE_FALSE(sk.is_empty());
  REQUIRE(sk.get_n() == n);
  REQUIRE(sk.get_cumulative_weight() == static_cast<double>(n));
  
  auto result = sk.get_result();
  REQUIRE(result.size() == sk.get_k()); // uniform weights so should be exactly k
  for (int val : sk.get_result())
    REQUIRE(val < static_cast<int>(n));
}
*/
template<typename T>
double entropy(std::vector<T> x) {
  T sum = 0;
  for (auto val : x) {
    sum += val;
  }
  double normalization = static_cast<double>(sum);
  double H = 0.0;
  for (auto val : x) {
    double p = val / normalization;
    if (p > 0)
      H -= p * std::log2(p);
  }
  return H;
}

template<typename T>
double kl_divergence(std::vector<T> p_arr, std::vector<T> q_arr) {
  REQUIRE(p_arr.size() == q_arr.size());
  
  T sum = 0;
  for (auto val : p_arr) {
    sum += val;
  }
  double p_normalization = static_cast<double>(sum);

  sum = 0;
  for (auto val : q_arr) {
    sum += val;
  }
  double q_normalization = static_cast<double>(sum);

  double D = 0.0;
  for (size_t i = 0; i < p_arr.size(); ++i) {
    double p = p_arr[i] / p_normalization;
    double q = q_arr[i] / q_normalization;

    if (p > 0 && q > 0)
      D -= p * std::log2(q / p);
  }
  
  return D;
}

/*
TEST_CASE("ebpps sketch: entropy", "[ebpps_sketch]") {
  uint32_t k = 6;
  uint32_t n = 30;
  uint32_t num_trials = 1000000;
  //double expected_c = static_cast<double>(k);
  //double expected_c = 4.9999999999999999; // i + 1, k=5
  //double expected_c = 1.5819768068010642; // exp(i) + 1
  //double expected_c = 5.999999999999998; // exp(i/10.0) + 1
  //double expected_c = 3.163974760803654; // exp(i/2) + 1 -- integer division
  double expected_c = 2.541507153714545; // exp(i/2.0) + 1

  // create index and weight vectors
  std::vector<int> idx(n);
  std::vector<double> wt(n);
  double total_wt = 0.0;
  for (size_t i = 0; i < n; ++i) {
    idx[i] = i;
    //wt[i] = 1.0;
    //wt[i] = i + 1.0;
    //wt[i] = std::exp(i) + 1;
    //wt[i] = std::exp(i / 10.0) + 1;
    //wt[i] = std::exp(i / 2) + 1;
    wt[i] = std::exp(i / 2.0) + 1;
    total_wt += wt[i];
  }

  // create target vector
  std::vector<double> tgt(n);
  for (size_t i = 0; i < n; ++i) {
    tgt[i] = num_trials * expected_c * wt[i] / total_wt;
  }

  std::vector<double> result(n); // double even though it'll hold counts
  double c = 0;

  for (uint32_t iter = 0; iter < num_trials; ++iter) {
    ebpps_sketch<int> sk(k);
    //var_opt_sketch<int> sk(k);

    // feed in data
    std::shuffle(idx.begin(), idx.end(), random_utils::rand);
    auto it_start = idx.begin(); auto it_end = idx.end();
    //auto it_start = idx.rbegin(); auto it_end = idx.rend();
    for (auto it = it_start; it != it_end; ++it) {
      sk.update(idx[*it], wt[idx[*it]]);
    }

    //auto r = sk.get_result();

    // increment counts
    //for (uint32_t val : r) {
    for (auto val : sk) {
      ++result[val];
      //++result[val.first];
    }

    c = sk.get_c();
  }

  std::cout << "c: " << std::setprecision(18) << c << std::endl;
  std::cout << "theoretical entropy: " << std::setprecision(12) << entropy(tgt) << std::endl;
  std::cout << "observed entropy: " << std::setprecision(12) << entropy(result) << std::endl;
  std::cout << "KL Divergence: " << std::setw(10) << kl_divergence(result, tgt) << std::endl;
  std::cout << std::endl;
  std::cout << "index\t tgt\t count\t\terror\t\trel error" << std::endl;
  for (uint32_t i = 0; i < n; ++i) {
    std::cout << std::setw(3) << i << "\t"
      << std::setw(10) << std::setprecision(6) << tgt[i] << "\t"
      << std::setw(6) << result[i] << "\t"
      << std::setw(15) << std::setprecision(12) << (result[i] - tgt[i]) << "\t"
      << std::setw(10) << std::setprecision(6) << (100.0 * std::abs(result[i] - tgt[i])/tgt[i])
      << std::endl;
  }
}
*/

TEST_CASE("ebpps sketch: merge", "[ebpps_sketch]") {
  uint32_t k = 6;
  uint32_t n = 30;
  uint32_t num_trials = 100000;
  //double expected_c = static_cast<double>(k);
  //double expected_c = 4.9999999999999999; // i + 1, k=5
  //double expected_c = 1.5819768068010642; // exp(i) + 1
  //double expected_c = 5.999999999999998; // exp(i/10.0) + 1
  //double expected_c = 3.163974760803654; // exp(i/2) + 1 -- integer division
  double expected_c = 2.541507153714545; // exp(i/2.0) + 1

  // create index and weight vectors
  std::vector<int> idx(n);
  std::vector<double> wt(n);
  double total_wt = 0.0;
  for (size_t i = 0; i < n; ++i) {
    idx[i] = i;
    //wt[i] = 1.0;
    //wt[i] = i + 1.0;
    //wt[i] = std::exp(i) + 1;
    //wt[i] = std::exp(i / 10.0) + 1;
    //wt[i] = std::exp(i / 2) + 1;
    wt[i] = std::exp(i / 2.0) + 1;
    total_wt += wt[i];
  }

  // create target vector
  std::vector<double> tgt(n);
  for (size_t i = 0; i < n; ++i) {
    tgt[i] = num_trials * expected_c * wt[i] / total_wt;
  }

  std::vector<double> result(n); // double even though it'll hold counts
  double c = 0;

  for (uint32_t iter = 0; iter < num_trials; ++iter) {
    ebpps_sketch<int> sk1(k);
    ebpps_sketch<int> sk2(k);
    //var_opt_sketch<int> sk(k);

    int offset = n / 2;
    for (unsigned i = 0; i < n / 2; ++i) {
      sk1.update(idx[i], wt[i]);
      sk2.update(idx[offset + i], wt[offset + i]);
    }

    /*
    // feed in data
    std::shuffle(idx.begin(), idx.end(), random_utils::rand);
    auto it_start = idx.begin(); auto it_end = idx.end();
    for (auto it = it_start; it != it_end; ++it) {
      sk1.update(idx[*it], wt[idx[*it]]);
    }

    std::shuffle(idx.begin(), idx.end(), random_utils::rand);
    it_start = idx.begin(); it_end = idx.end();
    for (auto it = it_start; it != it_end; ++it) {
      sk2.update(idx[*it], wt[idx[*it]]);
    }
    */

    sk1.merge(sk2);

    // increment counts
    for (auto val : sk1)
      ++result[val];

    c = sk1.get_c();
  }

  std::cout << "c: " << std::setprecision(18) << c << std::endl;
  std::cout << "theoretical entropy: " << std::setprecision(12) << entropy(tgt) << std::endl;
  std::cout << "observed entropy: " << std::setprecision(12) << entropy(result) << std::endl;
  std::cout << "KL Divergence: " << std::setw(10) << kl_divergence(result, tgt) << std::endl;
  std::cout << std::endl;
  std::cout << "index\t tgt\t count\t\terror\t\trel error" << std::endl;
  for (uint32_t i = 0; i < n; ++i) {
    std::cout << std::setw(3) << i << "\t"
      << std::setw(10) << std::setprecision(6) << tgt[i] << "\t"
      << std::setw(6) << result[i] << "\t"
      << std::setw(15) << std::setprecision(12) << (result[i] - tgt[i]) << "\t"
      << std::setw(10) << std::setprecision(6) << (100.0 * std::abs(result[i] - tgt[i])/tgt[i])
      << std::endl;
  }
}

}
