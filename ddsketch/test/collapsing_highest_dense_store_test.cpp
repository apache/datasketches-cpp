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

#include <string>
#include <functional>
#include <iostream>
#include <catch2/catch.hpp>

#include "collapsing_lowest_dense_store.hpp"
#include "collapsing_highest_dense_store.hpp"
#include "dense_store.hpp"
#include "sparse_store.hpp"
#include "unbounded_size_dense_store.hpp"

namespace datasketches {
using alloc = std::allocator<uint64_t>;

std::function<std::vector<Bin>(const std::vector<Bin>&)> collapsing_lowest_bins(const int max_num_bins) {
  return [max_num_bins](const std::vector<Bin>& bins) -> std::vector<Bin> {
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
  };
}

std::function<std::vector<Bin>(const std::vector<Bin>&)> collapsing_highest_bins(const int max_num_bins) {
  return [max_num_bins](const std::vector<Bin>& bins) -> std::vector<Bin> {
    int min_index = INT_MAX;
    for (const Bin& bin : bins) {
      min_index = std::max(min_index, bin.getIndex());
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
  };
}

struct StoreTest {
  std::string name;
  std::function<std::variant<std::shared_ptr<DenseStore<alloc>>, std::shared_ptr<SparseStore<alloc>>>()> new_store;
  std::function<std::vector<Bin>(const std::vector<Bin>&)> transform_bins;
};

std::vector<StoreTest> store_tests = {
  StoreTest{
    .name = "collapsing lowest 8",
    .new_store = []() {
      return std::make_unique<CollapsingLowestDenseStore<alloc>>(8);
    },
    .transform_bins = collapsing_lowest_bins(8)
  },
  StoreTest{
    .name = "collapsing lowest 128",
    .new_store = []() {
      return std::make_unique<CollapsingLowestDenseStore<alloc>>(128);
    },
    .transform_bins = collapsing_lowest_bins(128)
  },
  StoreTest{
    .name = "collapsing lowest 1024",
    .new_store = []() {
      return std::make_unique<CollapsingLowestDenseStore<alloc>>(1024);
    },
    .transform_bins = collapsing_lowest_bins(1024)
  },
  StoreTest{
    .name = "collapsing highest 8",
    .new_store = []() {
      return std::make_unique<CollapsingHighestDenseStore<alloc>>(8);
    },
    .transform_bins = collapsing_highest_bins(8)
  },
  StoreTest{
    .name = "collapsing highest 128",
    .new_store = []() {
      return std::make_unique<CollapsingHighestDenseStore<alloc>>(128);
    },
    .transform_bins = collapsing_highest_bins(128)
  },
  StoreTest{
    .name = "collapsing highest 1024",
    .new_store = []() {
      return std::make_unique<CollapsingHighestDenseStore<alloc>>(1024);
    },
    .transform_bins = collapsing_highest_bins(1024)
  },
  StoreTest{
    .name = "sparse store",
    .new_store = []() {
      return std::make_unique<SparseStore<alloc>>();
    },
    .transform_bins = [](std::vector<Bin> bins) {return bins;}
  },
  StoreTest{
    .name = "unbounded store",
    .new_store = []() {
      return std::make_unique<UnboundedSizeDenseStore<alloc>>();
    },
    .transform_bins = [](std::vector<Bin> bins) {return bins;}
  }
};



std::vector<Bin> normalize_bins(const std::vector<Bin>& bins) {
  std::map<int, uint64_t> bins_by_index;
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

void test(std::variant<std::shared_ptr<DenseStore<alloc>>, std::shared_ptr<SparseStore<alloc>>>& store, std::vector<Bin>& normalized_bins) {
  uint64_t expected_total_count = 0;
  for (const Bin& bin : normalized_bins) {
    expected_total_count += bin.getCount();
  }
  bool is_empty;
  std::visit([&is_empty](auto& store_ptr) {
    is_empty = store_ptr->is_empty();
  }, store);

  uint64_t total_count;
  std::visit([&total_count](auto& store_ptr) {
    total_count = store_ptr->get_total_count();
  }, store);

  if (expected_total_count == 0) {
    REQUIRE(is_empty);
    REQUIRE(total_count == 0);
    int min_index, max_index;
    REQUIRE_THROWS_AS(std::visit([&](auto& store_ptr) {
      min_index = store_ptr->get_min_index();
    }, store), std::runtime_error);
    REQUIRE_THROWS_AS(std::visit([&](auto& store_ptr) {
      max_index = store_ptr->get_max_index();
    }, store), std::runtime_error);
  } else {
    REQUIRE_FALSE(is_empty);
    REQUIRE(total_count == expected_total_count);

    int min_index, max_index;
    std::visit([&](auto& store_ptr) {
      min_index = store_ptr->get_min_index();
    }, store);
    REQUIRE(min_index == normalized_bins[0].getIndex());
    std::visit([&](auto& store_ptr) {
      max_index = store_ptr->get_max_index();
    }, store);
    REQUIRE(max_index == normalized_bins[normalized_bins.size() - 1].getIndex());

    std::vector<Bin> bins;
    std::visit([&](auto& store_ptr) {
      for (Bin bin : *store_ptr) {
        bins.push_back(bin);
      }
    }, store);
    std::ranges::sort(bins, [](const Bin& lhs, const Bin& rhs) {
      return lhs.getIndex() < rhs.getIndex();
    });
    REQUIRE(bins.size() == normalized_bins.size());
    for (size_t i = 0; i < bins.size(); ++i) {
      REQUIRE(bins[i] == normalized_bins[i]);
    }
  }

}

void test_adding(std::variant<std::shared_ptr<DenseStore<alloc>>, std::shared_ptr<SparseStore<alloc>>>& store, std::vector<int> values) {
  std::vector<Bin> bins;
  test(store, bins);
  for (int v : values) {
    std::visit([&](auto& store_ptr) {
      store_ptr->add(v);
    }, store);
    bins.emplace_back(v, 1);
  }
  test(store, bins);
}


TEST_CASE("store test empty", "[storetest]") {
  for (auto&[name, new_store, _] : store_tests) {
    auto store = new_store();
    test_adding(store, {});
  }
}

TEST_CASE("store test add constant", "[storetest]") {
  std::vector<int> indexes{-1000, -1, 0, 1, 1000};
  std::vector<uint64_t> counts{0, 1, 2, 4, 5, 10, 20, 100, 1000, 10000};

  for (auto&[name, new_store, transform_bins] : store_tests) {
    std::cout << name << std::endl;
    for (int idx: indexes) {
      for (uint64_t count: counts) {
        auto storeAdd = new_store();
        auto storeAddBin = new_store();
        auto storeAddWithCount = new_store();
        for (int i = 0; i < count; ++i) {
          std::visit([&](auto& store_ptr) {
            store_ptr->add(idx);
          }, storeAdd);
          std::visit([&](auto& store_ptr) {
            store_ptr->add(Bin(idx, 1));
          }, storeAddBin);
          std::visit([&](auto& store_ptr) {
            store_ptr->add(idx, 1);
          }, storeAddWithCount);
        }
        std::vector<Bin> bins{Bin(idx, count)};
        std::vector<Bin> normalized_bins = normalize_bins(transform_bins(bins));
        //test(storeAdd, normalized_bins);
        std::cout << idx << " " << count << std::endl;
        test(storeAddBin, normalized_bins);
        //test(storeAddWithCount, normalized_bins);
      }
    }
  }
}

}
