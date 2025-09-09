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

#include <iostream>
#include <catch2/catch.hpp>

#include "collapsing_highest_dense_store.hpp"
#include "sparse_store.hpp"
#include "store_factory.hpp"

#include "collapsing_lowest_dense_store.hpp"
#include "ddsketch.hpp"
#include "linearly_interpolated_mapping.hpp"
#include "unbounded_size_dense_store.hpp"

namespace datasketches {

static constexpr double eps = 1e-10;
static constexpr int numTests = 30;

using A = std::allocator<uint64_t>;

template<class T>
class bins_transformer_factory {
public:
  static std::unique_ptr<T> new_bins_transformer() {
    return std::make_unique<T>();
  }
};

template<const int max_num_bins>
class collapsing_lowest_bins {
public:
  static std::vector<Bin> collapse(std::vector<Bin>& bins) {
    int max_index = INT_MIN;
    for (const Bin& bin : bins) {
      max_index = std::max(max_index, bin.getIndex());
    }
    if (max_index < INT_MIN + max_num_bins) {
      return bins;
    }
    int min_collapsed_index = max_index - max_num_bins + 1;
    std::vector<Bin> collapsed_bins;
    collapsed_bins.reserve(bins.size());
    for (const Bin& bin : bins) {
      collapsed_bins.emplace_back(std::max(bin.getIndex(), min_collapsed_index), bin.getCount());
    }
    return collapsed_bins;
  }
};

template<const int max_num_bins>
class collapsing_highest_bins {
public:
  static std::vector<Bin> collapse(std::vector<Bin>& bins) {
    int min_index = INT_MAX;
    for (const Bin& bin : bins) {
      min_index = std::min(min_index, bin.getIndex());
    }
    if (min_index > INT_MAX - max_num_bins) {
      return bins;
    }
    int max_collapsed_index = min_index + max_num_bins - 1;
    std::vector<Bin> collapsed_bins;
    collapsed_bins.reserve(bins.size());
    for (const Bin& bin : bins) {
      collapsed_bins.emplace_back(std::min(bin.getIndex(), max_collapsed_index), bin.getCount());
    }
    return collapsed_bins;
  }
};

class noops_collapsing_bins {
public:
  static std::vector<Bin> collapse(std::vector<Bin>& bins) {
    return bins;
  }

};

std::vector<Bin> normalize_bins(const std::vector<Bin>& bins) {
  std::map<int, double> bins_by_index;
  for (const Bin& bin : bins) {
    if (bin.getCount() <= 0) {
      continue;
    }
    bins_by_index[bin.getIndex()] += bin.getCount();
  }

  std::vector<Bin> normalized_bins;
  normalized_bins.reserve(bins_by_index.size());
  for (auto & it : bins_by_index) {
    normalized_bins.emplace_back(it.first, it.second);
  }

  std::sort(normalized_bins.begin(), normalized_bins.end(), [](const Bin& lhs, const Bin& rhs) {
    return lhs.getIndex() < rhs.getIndex();
  });

  return normalized_bins;
}

int random_index() {
  std::random_device rd;
  std::mt19937_64 rng(rd());
  std::uniform_int_distribution<int> distribution(-1000, 1000);
  return distribution(rng);
}

double random_count() {
  double max = 10.;
  std::random_device rd;
  std::mt19937_64 rng(rd());
  std::uniform_real_distribution<double> distribution(0., 1.);
  double count= 0.;
  do {
    count = distribution(rng);
  } while (count < eps * 10);
  return count;
}

template<class StoreType>
void assert_encode_bins(StoreType& store, const std::vector<Bin>& normalized_bins) {
  double expected_total_count = 0;
  for (const Bin& bin : normalized_bins) {
    expected_total_count += bin.getCount();
  }

  if (expected_total_count == 0) {
    REQUIRE(store->is_empty());
    REQUIRE(store->get_total_count() == 0);
    REQUIRE_THROWS_AS(store->get_min_index(), std::runtime_error);
    REQUIRE_THROWS_AS(store->get_max_index(), std::runtime_error);
  } else {
    REQUIRE_FALSE(store->is_empty());
    REQUIRE(store->get_total_count() - expected_total_count < eps);

    REQUIRE(store->get_min_index() == normalized_bins[0].getIndex());
    REQUIRE(store->get_max_index() == normalized_bins[normalized_bins.size() - 1].getIndex());

    std::vector<Bin> bins;
    for (const Bin& bin : *store) {
        bins.push_back(bin);
      }

    std::ranges::sort(bins, [](const Bin& lhs, const Bin& rhs) {
      return lhs.getIndex() < rhs.getIndex();
    });
    REQUIRE(bins.size() == normalized_bins.size());
    for (size_t i = 0; i < bins.size(); ++i) {
      REQUIRE(bins[i].getIndex() == normalized_bins[i].getIndex());
      REQUIRE_THAT(bins[i].getCount(), Catch::Matchers::WithinAbs(normalized_bins[i].getCount(), 1e-3));
    }
  }
}

template<class StoreType>
void test_copy(StoreType& store, const std::vector<Bin>& normalized_bins) {
  auto store_copy = store->copy();
  store->merge(*store_copy);
  assert_encode_bins(store_copy, normalized_bins);
  store->clear();
  assert_encode_bins(store_copy, normalized_bins);
  std::vector<Bin> empty_bins;
  assert_encode_bins(store, empty_bins);

  std::vector<Bin> permutated_bins = normalized_bins;
  std::ranges::shuffle(permutated_bins, std::mt19937(42));

  for (const Bin& bin : permutated_bins) {
    store->add(bin);
  }

  assert_encode_bins(store, normalized_bins);
}

template<class StoreType>
void test_store(StoreType& store, const std::vector<Bin>& normalized_bins) {
  assert_encode_bins(store, normalized_bins);
  test_copy<StoreType>(store, normalized_bins);
}

TEMPLATE_TEST_CASE("store test empty", "[storetest]",
  (store_factory<CollapsingLowestDenseStore<A>, 8>),
  (store_factory<CollapsingLowestDenseStore<A>, 128>),
  (store_factory<CollapsingLowestDenseStore<A>, 1024>),
  (store_factory<CollapsingHighestDenseStore<A>, 8>),
  (store_factory<CollapsingHighestDenseStore<A>, 128>),
  (store_factory<CollapsingHighestDenseStore<A>, 1024>),
  (store_factory<SparseStore<A>>),
  (store_factory<UnboundedSizeDenseStore<A>>)
  ) {
  auto store = TestType::new_store();
  std::vector<Bin> empty_bins{};
  test_store(store, empty_bins);
}

TEMPLATE_TEST_CASE("store test add datasets", "[storetest]",
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 8>, collapsing_lowest_bins<8>>),
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 128>, collapsing_lowest_bins<128>>),
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 1024>, collapsing_lowest_bins<1024>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 8>, collapsing_highest_bins<8>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 128>, collapsing_highest_bins<128>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 1024>, collapsing_highest_bins<1024>>),
  (std::pair<store_factory<SparseStore<A>>, noops_collapsing_bins>),
  (std::pair<store_factory<UnboundedSizeDenseStore<A>>, noops_collapsing_bins>)
  ) {
  std::vector<std::vector<int>> datasets{
    {-1000},
    {-1},
    {0},
    {1},
    {1000},
    {1000, 1000},
    {1000, -1000},
    {-1000, 1000},
    {-1000, -1000},
    {0, 0, 0, 0}
  };
  std::vector<double> counts{0.1, 1, 100};

  for (const std::vector<int>& dataset : datasets) {
    std::vector<Bin> bins;
    bins.reserve(dataset.size());
    auto storeAdd = TestType::first_type::new_store();
    for (const int& index : dataset) {
      Bin bin(index, 1);
      bins.push_back(bin);
      storeAdd->add(index);
    }
    std::vector<Bin> normalized_bins = normalize_bins(TestType::second_type::collapse(bins));
    test_store(storeAdd, normalized_bins);
    for (const double& count : counts) {
      bins.clear();
      auto storeAddBin = TestType::first_type::new_store();
      auto storeAddWithCount = TestType::first_type::new_store();
      for (const int& index : dataset) {
        Bin bin(index, count);
        bins.push_back(bin);
        storeAddBin->add(bin);
        storeAddWithCount->add(index, count);
      }
      normalized_bins = normalize_bins(TestType::second_type::collapse(bins));
      test_store(storeAddBin, normalized_bins);
      test_store(storeAddWithCount, normalized_bins);
    }
  }

}

TEMPLATE_TEST_CASE("store test add constant", "[storetest]",
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 8>, collapsing_lowest_bins<8>>),
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 128>, collapsing_lowest_bins<128>>),
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 1024>, collapsing_lowest_bins<1024>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 8>, collapsing_highest_bins<8>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 128>, collapsing_highest_bins<128>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 1024>, collapsing_highest_bins<1024>>),
  (std::pair<store_factory<SparseStore<A>>, noops_collapsing_bins>),
  (std::pair<store_factory<UnboundedSizeDenseStore<A>>, noops_collapsing_bins>)
  ) {
  std::vector<int> indexes{-1000, -1, 0, 1, 1000};
  std::vector<double> counts{0, 1, 2, 4, 5, 10, 20, 100, 1000, 10000};

  for (int idx: indexes) {
    for (double count: counts) {
      auto storeAdd = TestType::first_type::new_store();
      auto storeAddBin = TestType::first_type::new_store();
      auto storeAddWithCount = TestType::first_type::new_store();
      for (int i = 0; i < count; ++i) {
        storeAdd->add(idx);
        storeAddBin->add(Bin(idx, 1));
        storeAddWithCount->add(idx, 1);
      }
      std::vector<Bin> bins{Bin(idx, count)};
      std::vector<Bin> normalized_bins = normalize_bins(TestType::second_type::collapse(bins));
      test_store<decltype(storeAdd)>(storeAdd, normalized_bins);
      test_store<decltype(storeAddBin)>(storeAddBin, normalized_bins);
      test_store<decltype(storeAddWithCount)>(storeAddWithCount, normalized_bins);
    }
  }
}

TEMPLATE_TEST_CASE("test add monotonous", "[storetest]",
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 8>, collapsing_lowest_bins<8>>),
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 128>, collapsing_lowest_bins<128>>),
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 1024>, collapsing_lowest_bins<1024>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 8>, collapsing_highest_bins<8>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 128>, collapsing_highest_bins<128>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 1024>, collapsing_highest_bins<1024>>),
  (std::pair<store_factory<SparseStore<A>>, noops_collapsing_bins>),
  (std::pair<store_factory<UnboundedSizeDenseStore<A>>, noops_collapsing_bins>)
  ) {
  std::vector<int> increments{2, 10, 100, -2, -10, -100};
  std::vector<int> spreads{2, 10, 10000};

  for (const int& incr: increments) {
    for (const int& spread: spreads) {
      std::vector<Bin> bins;
      auto storeAdd = TestType::first_type::new_store();
      auto storeAddBin = TestType::first_type::new_store();
      auto storeAddWithCount = TestType::first_type::new_store();
      for (int index = 0; std::abs(index) <= spread; index += incr) {
        Bin bin(index, 1);
        bins.push_back(bin);
        storeAdd->add(index);
        storeAddBin->add(bin);
        storeAddWithCount->add(index, 1);
      }
      std::vector<Bin> normalized_bins = normalize_bins(TestType::second_type::collapse(bins));
      test_store<decltype(storeAdd)>(storeAdd, normalized_bins);
      test_store<decltype(storeAddBin)>(storeAddBin, normalized_bins);
      test_store<decltype(storeAddWithCount)>(storeAddWithCount, normalized_bins);
    }
  }
}

TEMPLATE_TEST_CASE("test add fuzzy", "[storetest]",
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 8>, collapsing_lowest_bins<8>>),
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 128>, collapsing_lowest_bins<128>>),
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 1024>, collapsing_lowest_bins<1024>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 8>, collapsing_highest_bins<8>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 128>, collapsing_highest_bins<128>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 1024>, collapsing_highest_bins<1024>>),
  (std::pair<store_factory<SparseStore<A>>, noops_collapsing_bins>),
  (std::pair<store_factory<UnboundedSizeDenseStore<A>>, noops_collapsing_bins>)
  ) {
  const int maxNumValues = 1000;
  std::random_device r;
  std::mt19937_64 rng(r());
  std::uniform_int_distribution<int> dist(0, maxNumValues - 1);

   for (int i = 0; i < numTests; i++) {
     std::vector<Bin> bins;
     auto storeAdd = TestType::first_type::new_store();
     auto storeAddBin = TestType::first_type::new_store();
     auto storeAddWithCount = TestType::first_type::new_store();
     int numValues = dist(rng);
     for (int j = 0; j < numValues; j++) {
       Bin bin(random_index(), random_count());
       bins.push_back(bin);
       storeAddBin->add(bin);
       storeAddWithCount->add(bin.getIndex(), bin.getCount());
     }
     std::vector<Bin> normalized_bins = normalize_bins(TestType::second_type::collapse(bins));
     test_store<decltype(storeAddBin)>(storeAddBin, normalized_bins);
     test_store<decltype(storeAddWithCount)>(storeAddWithCount, normalized_bins);
   }
}

TEMPLATE_TEST_CASE("test merge fuzzy", "[storetest]",
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 8>, collapsing_lowest_bins<8>>),
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 128>, collapsing_lowest_bins<128>>),
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 1024>, collapsing_lowest_bins<1024>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 8>, collapsing_highest_bins<8>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 128>, collapsing_highest_bins<128>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 1024>, collapsing_highest_bins<1024>>),
  (std::pair<store_factory<SparseStore<A>>, noops_collapsing_bins>),
  (std::pair<store_factory<UnboundedSizeDenseStore<A>>, noops_collapsing_bins>)
) {
  const int numMerges = 3;
  const int maxNumAdds = 1000;

  std::random_device r;
  std::mt19937_64 rng(r());
  std::uniform_int_distribution<int> dist(0, maxNumAdds - 1);

  for (int i = 0; i < numTests; i++) {
    std::vector<Bin> bins;
    auto store = TestType::first_type::new_store();
    for (int j = 0; j < numMerges; j++) {
      int numValues = dist(rng);
      auto tmpStore = TestType::first_type::new_store();
      for (int k = 0; k < numValues; k++) {
        Bin bin(random_index(), random_count());
        bins.push_back(bin);
        tmpStore->add(bin);
      }
      store->merge(*tmpStore);
    }
    std::vector<Bin> normalized_bins = normalize_bins(TestType::second_type::collapse(bins));
    test_store<decltype(store)>(store, normalized_bins);
  }
}

TEMPLATE_TEST_CASE("test merge sparse into dense and vice-versa", "[storetest]",
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 8>, collapsing_lowest_bins<8>>),
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 128>, collapsing_lowest_bins<128>>),
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 1024>, collapsing_lowest_bins<1024>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 8>, collapsing_highest_bins<8>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 128>, collapsing_highest_bins<128>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 1024>, collapsing_highest_bins<1024>>),
  (std::pair<store_factory<SparseStore<A>>, noops_collapsing_bins>),
  (std::pair<store_factory<UnboundedSizeDenseStore<A>>, noops_collapsing_bins>)
  ) {
  std::vector<int> indexes{-1000, -1, 0, 1, 1000};
  std::vector<double> counts{0, 1, 2, 4, 5, 10, 20, 100, 1000, 10000};

  auto denseStore = TestType::first_type::new_store();
  auto sparseStore = store_factory<SparseStore<A>>::new_store();
  std::vector<Bin> bins;
  std::map<int, double> sparse_bins_map;
  for (const int& index : indexes) {
    double total_count = 0.;
    for (const double& count : counts) {
      denseStore->add(index, count);
      sparseStore->add(index, count);
      total_count += count;
    }
    bins.emplace_back(index, total_count);
    sparse_bins_map.emplace(index, total_count);
  }
  std::vector<Bin> normalized_bins = normalize_bins(TestType::second_type::collapse(bins));
  test_store<decltype(denseStore)>(denseStore, normalized_bins);
  test_store<decltype(sparseStore)>(sparseStore, bins);

  std::vector<Bin> bins_in_dense(bins);
  bins_in_dense.insert(bins_in_dense.end(), bins.begin(), bins.end());
  normalized_bins = normalize_bins(TestType::second_type::collapse(bins_in_dense));
  denseStore->merge(*sparseStore);
  test_store<decltype(denseStore)>(denseStore, normalized_bins);

  for (const Bin& dense_bin : normalized_bins) {
    sparse_bins_map[dense_bin.getIndex()] += dense_bin.getCount();
  }
  std::vector<Bin> bins_in_sparse;
  bins_in_sparse.reserve(bins_in_sparse.size());
  for (const auto& [index, count] : sparse_bins_map) {
    bins_in_sparse.emplace_back(index, count);
  }
  sparseStore->merge(*denseStore);
  test_store<decltype(sparseStore)>(sparseStore, bins_in_sparse);
}


TEMPLATE_TEST_CASE("test cross merge", "[storetest]",
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 8>, collapsing_lowest_bins<8>>),
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 128>, collapsing_lowest_bins<128>>),
  (std::pair<store_factory<CollapsingLowestDenseStore<A>, 1024>, collapsing_lowest_bins<1024>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 8>, collapsing_highest_bins<8>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 128>, collapsing_highest_bins<128>>),
  (std::pair<store_factory<CollapsingHighestDenseStore<A>, 1024>, collapsing_highest_bins<1024>>),
  (std::pair<store_factory<UnboundedSizeDenseStore<A>>, noops_collapsing_bins>)
  ) {
  using StoreVariant = std::variant<
    std::unique_ptr<CollapsingHighestDenseStore<A>>,
    std::unique_ptr<CollapsingLowestDenseStore<A>>,
    std::unique_ptr<UnboundedSizeDenseStore<A>>
  >;

  std::vector<std::pair<StoreVariant, std::function<std::vector<Bin>(std::vector<Bin>&)>>> dense_stores;
  dense_stores.push_back({store_factory<CollapsingLowestDenseStore<A>, 8>::new_store(), collapsing_lowest_bins<8>::collapse});
  dense_stores.push_back({store_factory<CollapsingLowestDenseStore<A>, 128>::new_store(), collapsing_lowest_bins<128>::collapse});
  dense_stores.push_back({store_factory<CollapsingLowestDenseStore<A>, 1024>::new_store(), collapsing_lowest_bins<1024>::collapse});
  dense_stores.push_back({store_factory<CollapsingHighestDenseStore<A>, 8>::new_store(), collapsing_highest_bins<8>::collapse});
  dense_stores.push_back({store_factory<CollapsingHighestDenseStore<A>, 128>::new_store(), collapsing_highest_bins<128>::collapse});
  dense_stores.push_back({store_factory<CollapsingHighestDenseStore<A>, 1024>::new_store(), collapsing_highest_bins<1024>::collapse});
  dense_stores.push_back({store_factory<UnboundedSizeDenseStore<A>>::new_store(), noops_collapsing_bins::collapse});

  std::vector<int> indexes{-1000, -1, 0, 1, 1000};
  std::vector<double> counts{0, 1, 2, 4, 5, 10, 20, 100, 1000, 10000};
  for (auto& [other_store, transform_bins] : dense_stores) {
    auto store = TestType::first_type::new_store();
    std::vector<Bin> bins;
    for (const int& index : indexes) {
      double total_count = 0.;
      for (const double& count : counts) {
        store->add(index, count);
        std::visit([&](auto& other) {
          other->add(index, count);
        }, other_store);
        total_count += count;
      }
      bins.emplace_back(index, total_count);
    }
    std::vector<Bin> normalized_bins = normalize_bins(TestType::second_type::collapse(bins));
    std::vector<Bin> normalized_other_bins = normalize_bins(transform_bins(bins));
    test_store<decltype(store)>(store, normalized_bins);
    std::visit([&normalized_other_bins](auto& other) {
      test_store<decltype(other)>(other, normalized_other_bins);
    }, other_store);

    std::vector<Bin> merged_bins(normalized_bins);
    merged_bins.insert(merged_bins.end(), normalized_other_bins.begin(), normalized_other_bins.end());
    std::vector<Bin> normalized_merged_bins = normalize_bins(TestType::second_type::collapse(merged_bins));
    std::visit([&store](auto& other) {
      store->merge(*other);
    }, other_store);
    test_store<decltype(store)>(store, normalized_merged_bins);

    std::vector<Bin> merged_other_bins(normalized_other_bins);
    merged_other_bins.insert(merged_other_bins.end(), normalized_merged_bins.begin(), normalized_merged_bins.end());
    std::vector<Bin> normalized_merged_other_bins = normalize_bins(transform_bins(merged_other_bins));
    std::visit([&store](auto& other) {
      other->merge(*store);
    }, other_store);
    std::visit([&normalized_merged_other_bins](auto& other) {
      test_store<decltype(other)>(other, normalized_merged_other_bins);
    }, other_store);
  }
}

TEST_CASE("merge test", "[mergetest]") {
  CollapsingHighestDenseStore<std::allocator<uint8_t>> s1(1024);
  CollapsingHighestDenseStore<std::allocator<uint8_t>> s2(1024);
  CollapsingLowestDenseStore<std::allocator<uint8_t>> s3(1024);

  SparseStore<std::allocator<uint8_t>> ss;
  s1.merge(s2);
  s1.merge(s3);

  ss.merge(s1);
  s1.merge(ss);

  LinearlyInterpolatedMapping mapping(0.01);
  DDSketch sketch(s1, s2, mapping, 1., 1.);

  sketch.update(10.);
  // DDSketch<SparseStore<std::allocator<uint8_t>>, LinearlyInterpolatedMapping> sketch2(ss, ss, mapping);

  //DDSketch<CollapsingHighestDenseStore<A>, LinearlyInterpolatedMapping> sketch4<1024>(0.99);
}
}

