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

// #include <iostream>
// #include <sstream>
// #include <catch2/catch.hpp>
// #include <climits>
// #include <map>
// #include <algorithm>
//
// #include "collapsing_highest_dense_store.hpp"
// #include "store_factory.hpp"
//
// #include "collapsing_lowest_dense_store.hpp"
// #include "ddsketch.hpp"
// #include "linearly_interpolated_mapping.hpp"
// #include "unbounded_size_dense_store.hpp"
//
// namespace datasketches {
//
// static constexpr double eps = 1e-10;
// static constexpr int numTests = 30;
//
// using A = std::allocator<uint64_t>;
//
// template<class T>
// class bins_transformer_factory {
// public:
//   static std::unique_ptr<T> new_bins_transformer() {
//     return std::unique_ptr<T>();
//   }
// };
//
// template<const int max_num_bins>
// class collapsing_lowest_bins {
// public:
//   static std::vector<Bin> collapse(std::vector<Bin>& bins) {
//     int max_index = INT_MIN;
//     for (const Bin& bin : bins) {
//       max_index = std::max(max_index, bin.get_index());
//     }
//     if (max_index < INT_MIN + max_num_bins) {
//       return bins;
//     }
//     int min_collapsed_index = max_index - max_num_bins + 1;
//     std::vector<Bin> collapsed_bins;
//     collapsed_bins.reserve(bins.size());
//     for (const Bin& bin : bins) {
//       collapsed_bins.emplace_back(std::max(bin.get_index(), min_collapsed_index), bin.get_count());
//     }
//     return collapsed_bins;
//   }
// };
//
// template<const int max_num_bins>
// class collapsing_highest_bins {
// public:
//   static std::vector<Bin> collapse(std::vector<Bin>& bins) {
//     int min_index = INT_MAX;
//     for (const Bin& bin : bins) {
//       min_index = std::min(min_index, bin.get_index());
//     }
//     if (min_index > INT_MAX - max_num_bins) {
//       return bins;
//     }
//     int max_collapsed_index = min_index + max_num_bins - 1;
//     std::vector<Bin> collapsed_bins;
//     collapsed_bins.reserve(bins.size());
//     for (const Bin& bin : bins) {
//       collapsed_bins.emplace_back(std::min(bin.get_index(), max_collapsed_index), bin.get_count());
//     }
//     return collapsed_bins;
//   }
// };
//
// class noops_collapsing_bins {
// public:
//   static std::vector<Bin> collapse(std::vector<Bin>& bins) {
//     return bins;
//   }
//
// };
//
// std::vector<Bin> normalize_bins(const std::vector<Bin>& bins) {
//   std::map<int, double> bins_by_index;
//   for (const Bin& bin : bins) {
//     if (bin.get_count() <= 0) {
//       continue;
//     }
//     bins_by_index[bin.get_index()] += bin.get_count();
//   }
//
//   std::vector<Bin> normalized_bins;
//   normalized_bins.reserve(bins_by_index.size());
//   for (auto & it : bins_by_index) {
//     normalized_bins.emplace_back(it.first, it.second);
//   }
//
//   std::sort(normalized_bins.begin(), normalized_bins.end(), [](const Bin& lhs, const Bin& rhs) {
//     return lhs.get_index() < rhs.get_index();
//   });
//
//   return normalized_bins;
// }
//
// int random_index() {
//   std::random_device rd;
//   std::mt19937_64 rng(rd());
//   std::uniform_int_distribution<int> distribution(-1000, 1000);
//   return distribution(rng);
// }
//
// double random_count() {
//   std::random_device rd;
//   std::mt19937_64 rng(rd());
//   std::uniform_real_distribution<double> distribution(0., 1.);
//   double count= 0.;
//   do {
//     count = distribution(rng);
//   } while (count < eps * 10);
//   return count;
// }
//
// template<class StoreType>
// void assert_encode_bins(StoreType& store, const std::vector<Bin>& normalized_bins) {
//   double expected_total_count = 0;
//   for (const Bin& bin : normalized_bins) {
//     expected_total_count += bin.get_count();
//   }
//
//   if (expected_total_count == 0) {
//     REQUIRE(store->is_empty());
//     REQUIRE(store->get_total_count() == 0);
//     REQUIRE_THROWS_AS(store->get_min_index(), std::runtime_error);
//     REQUIRE_THROWS_AS(store->get_max_index(), std::runtime_error);
//   } else {
//     REQUIRE_FALSE(store->is_empty());
//     REQUIRE(store->get_total_count() - expected_total_count < eps);
//
//     REQUIRE(store->get_min_index() == normalized_bins[0].get_index());
//     REQUIRE(store->get_max_index() == normalized_bins[normalized_bins.size() - 1].get_index());
//
//     std::vector<Bin> bins;
//     for (const Bin& bin : *store) {
//         bins.push_back(bin);
//       }
//
//     std::sort(bins.begin(), bins.end(), [](const Bin& lhs, const Bin& rhs) {
//       return lhs.get_index() < rhs.get_index();
//     });
//     REQUIRE(bins.size() == normalized_bins.size());
//     for (size_t i = 0; i < bins.size(); ++i) {
//       REQUIRE(bins[i].get_index() == normalized_bins[i].get_index());
//       REQUIRE_THAT(bins[i].get_count(), Catch::Matchers::WithinAbs(normalized_bins[i].get_count(), 1e-3));
//     }
//   }
// }
//
// template<class StoreType>
// void test_copy(StoreType& store, const std::vector<Bin>& normalized_bins) {
//   auto store_copy = store->copy();
//   store->merge(*store_copy);
//   assert_encode_bins(store_copy, normalized_bins);
//   store->clear();
//   assert_encode_bins(store_copy, normalized_bins);
//   std::vector<Bin> empty_bins;
//   assert_encode_bins(store, empty_bins);
//
//   std::vector<Bin> permutated_bins = normalized_bins;
//
//   std::shuffle(permutated_bins.begin(), permutated_bins.end(), std::mt19937(42));
//
//   for (const Bin& bin : permutated_bins) {
//     store->add(bin);
//   }
//
//   assert_encode_bins(store, normalized_bins);
// }
//
// template<class StoreType>
// void test_store(StoreType& store, const std::vector<Bin>& normalized_bins) {
//   assert_encode_bins(store, normalized_bins);
//   test_copy<StoreType>(store, normalized_bins);
// }
//
// template<int N>
// using CollapsingLowestStoreTestCase = store_factory<CollapsingLowestDenseStore<N, A>>;
//
// template<int N>
// using CollapsingHighestStoreTestCase = store_factory<CollapsingHighestDenseStore<N, A>>;
//
// using UnboundedStoreSizeTestCase = store_factory<UnboundedSizeDenseStore<A>>;
//
// TEMPLATE_TEST_CASE("store test empty", "[storetest]",
//   CollapsingLowestStoreTestCase<8>,
//   CollapsingLowestStoreTestCase<128>,
//   CollapsingLowestStoreTestCase<1024>,
//   CollapsingHighestStoreTestCase<8>,
//   CollapsingHighestStoreTestCase<128>,
//   CollapsingHighestStoreTestCase<1024>,
//   UnboundedStoreSizeTestCase
//   ) {
//   auto store = TestType::new_store();
//   std::vector<Bin> empty_bins{};
//   test_store(store, empty_bins);
// }
//
// TEMPLATE_TEST_CASE("store test add datasets", "[storetest]",
//   (std::pair<store_factory<CollapsingLowestDenseStore<8, A>>, collapsing_lowest_bins<8>>),
//   (std::pair<store_factory<CollapsingLowestDenseStore<128, A>>, collapsing_lowest_bins<128>>),
//   (std::pair<store_factory<CollapsingLowestDenseStore<1024, A>>, collapsing_lowest_bins<1024>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<8, A>>, collapsing_highest_bins<8>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<128, A>>, collapsing_highest_bins<128>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<1024, A>>, collapsing_highest_bins<1024>>),
//   (std::pair<store_factory<UnboundedSizeDenseStore<A>>, noops_collapsing_bins>)
//   ) {
//   std::vector<std::vector<int>> datasets{
//     {-1000},
//     {-1},
//     {0},
//     {1},
//     {1000},
//     {1000, 1000},
//     {1000, -1000},
//     {-1000, 1000},
//     {-1000, -1000},
//     {0, 0, 0, 0}
//   };
//   std::vector<double> counts{0.1, 1, 100};
//
//   for (const std::vector<int>& dataset : datasets) {
//     std::vector<Bin> bins;
//     bins.reserve(dataset.size());
//     auto storeAdd = TestType::first_type::new_store();
//     for (const int& index : dataset) {
//       Bin bin(index, 1);
//       bins.push_back(bin);
//       storeAdd->add(index);
//     }
//     std::vector<Bin> normalized_bins = normalize_bins(TestType::second_type::collapse(bins));
//     test_store(storeAdd, normalized_bins);
//     for (const double& count : counts) {
//       bins.clear();
//       auto storeAddBin = TestType::first_type::new_store();
//       auto storeAddWithCount = TestType::first_type::new_store();
//       for (const int& index : dataset) {
//         Bin bin(index, count);
//         bins.push_back(bin);
//         storeAddBin->add(bin);
//         storeAddWithCount->add(index, count);
//       }
//       normalized_bins = normalize_bins(TestType::second_type::collapse(bins));
//       test_store(storeAddBin, normalized_bins);
//       test_store(storeAddWithCount, normalized_bins);
//     }
//   }
//
// }
//
// TEMPLATE_TEST_CASE("store test add constant", "[storetest]",
//   (std::pair<store_factory<CollapsingLowestDenseStore<8, A>>, collapsing_lowest_bins<8>>),
//   (std::pair<store_factory<CollapsingLowestDenseStore<128, A>>, collapsing_lowest_bins<128>>),
//   (std::pair<store_factory<CollapsingLowestDenseStore<1024, A>>, collapsing_lowest_bins<1024>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<8, A>>, collapsing_highest_bins<8>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<128, A>>, collapsing_highest_bins<128>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<1024, A>>, collapsing_highest_bins<1024>>),
//   (std::pair<store_factory<UnboundedSizeDenseStore<A>>, noops_collapsing_bins>)
//   ) {
//   std::vector<int> indexes{-1000, -1, 0, 1, 1000};
//   std::vector<double> counts{0, 1, 2, 4, 5, 10, 20, 100, 1000, 10000};
//
//   for (int idx: indexes) {
//     for (double count: counts) {
//       auto storeAdd = TestType::first_type::new_store();
//       auto storeAddBin = TestType::first_type::new_store();
//       auto storeAddWithCount = TestType::first_type::new_store();
//       for (int i = 0; i < count; ++i) {
//         storeAdd->add(idx);
//         storeAddBin->add(Bin(idx, 1));
//         storeAddWithCount->add(idx, 1);
//       }
//       std::vector<Bin> bins{Bin(idx, count)};
//       std::vector<Bin> normalized_bins = normalize_bins(TestType::second_type::collapse(bins));
//       test_store<decltype(storeAdd)>(storeAdd, normalized_bins);
//       test_store<decltype(storeAddBin)>(storeAddBin, normalized_bins);
//       test_store<decltype(storeAddWithCount)>(storeAddWithCount, normalized_bins);
//     }
//   }
// }
//
// TEMPLATE_TEST_CASE("test add monotonous", "[storetest]",
//   (std::pair<store_factory<CollapsingLowestDenseStore<8, A>>, collapsing_lowest_bins<8>>),
//   (std::pair<store_factory<CollapsingLowestDenseStore<128, A>>, collapsing_lowest_bins<128>>),
//   (std::pair<store_factory<CollapsingLowestDenseStore<1024, A>>, collapsing_lowest_bins<1024>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<8, A>>, collapsing_highest_bins<8>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<128, A>>, collapsing_highest_bins<128>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<1024, A>>, collapsing_highest_bins<1024>>),
//   (std::pair<store_factory<UnboundedSizeDenseStore<A>>, noops_collapsing_bins>)
//   ) {
//   std::vector<int> increments{2, 10, 100, -2, -10, -100};
//   std::vector<int> spreads{2, 10, 10000};
//
//   for (const int& incr: increments) {
//     for (const int& spread: spreads) {
//       std::vector<Bin> bins;
//       auto storeAdd = TestType::first_type::new_store();
//       auto storeAddBin = TestType::first_type::new_store();
//       auto storeAddWithCount = TestType::first_type::new_store();
//       for (int index = 0; std::abs(index) <= spread; index += incr) {
//         Bin bin(index, 1);
//         bins.push_back(bin);
//         storeAdd->add(index);
//         storeAddBin->add(bin);
//         storeAddWithCount->add(index, 1);
//       }
//       std::vector<Bin> normalized_bins = normalize_bins(TestType::second_type::collapse(bins));
//       test_store<decltype(storeAdd)>(storeAdd, normalized_bins);
//       test_store<decltype(storeAddBin)>(storeAddBin, normalized_bins);
//       test_store<decltype(storeAddWithCount)>(storeAddWithCount, normalized_bins);
//     }
//   }
// }
//
// TEMPLATE_TEST_CASE("test add fuzzy", "[storetest]",
//   (std::pair<store_factory<CollapsingLowestDenseStore<8, A>>, collapsing_lowest_bins<8>>),
//   (std::pair<store_factory<CollapsingLowestDenseStore<128, A>>, collapsing_lowest_bins<128>>),
//   (std::pair<store_factory<CollapsingLowestDenseStore<1024, A>>, collapsing_lowest_bins<1024>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<8, A>>, collapsing_highest_bins<8>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<128, A>>, collapsing_highest_bins<128>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<1024, A>>, collapsing_highest_bins<1024>>),
//   (std::pair<store_factory<UnboundedSizeDenseStore<A>>, noops_collapsing_bins>)
//   ) {
//   const int maxNumValues = 1000;
//   std::random_device r;
//   std::mt19937_64 rng(r());
//   std::uniform_int_distribution<int> dist(0, maxNumValues - 1);
//
//    for (int i = 0; i < numTests; i++) {
//      std::vector<Bin> bins;
//      auto storeAdd = TestType::first_type::new_store();
//      auto storeAddBin = TestType::first_type::new_store();
//      auto storeAddWithCount = TestType::first_type::new_store();
//      int numValues = dist(rng);
//      for (int j = 0; j < numValues; j++) {
//        Bin bin(random_index(), random_count());
//        bins.push_back(bin);
//        storeAddBin->add(bin);
//        storeAddWithCount->add(bin.get_index(), bin.get_count());
//      }
//      std::vector<Bin> normalized_bins = normalize_bins(TestType::second_type::collapse(bins));
//      test_store<decltype(storeAddBin)>(storeAddBin, normalized_bins);
//      test_store<decltype(storeAddWithCount)>(storeAddWithCount, normalized_bins);
//    }
// }
//
// TEMPLATE_TEST_CASE("test merge fuzzy", "[storetest]",
//   (std::pair<store_factory<CollapsingLowestDenseStore<8, A>>, collapsing_lowest_bins<8>>),
//   (std::pair<store_factory<CollapsingLowestDenseStore<128, A>>, collapsing_lowest_bins<128>>),
//   (std::pair<store_factory<CollapsingLowestDenseStore<1024, A>>, collapsing_lowest_bins<1024>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<8, A>>, collapsing_highest_bins<8>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<128, A>>, collapsing_highest_bins<128>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<1024, A>>, collapsing_highest_bins<1024>>),
//   (std::pair<store_factory<UnboundedSizeDenseStore<A>>, noops_collapsing_bins>)
// ) {
//   const int numMerges = 3;
//   const int maxNumAdds = 1000;
//
//   std::random_device r;
//   std::mt19937_64 rng(r());
//   std::uniform_int_distribution<int> dist(0, maxNumAdds - 1);
//
//   for (int i = 0; i < numTests; i++) {
//     std::vector<Bin> bins;
//     auto store = TestType::first_type::new_store();
//     for (int j = 0; j < numMerges; j++) {
//       int numValues = dist(rng);
//       auto tmpStore = TestType::first_type::new_store();
//       for (int k = 0; k < numValues; k++) {
//         Bin bin(random_index(), random_count());
//         bins.push_back(bin);
//         tmpStore->add(bin);
//       }
//       store->merge(*tmpStore);
//     }
//     std::vector<Bin> normalized_bins = normalize_bins(TestType::second_type::collapse(bins));
//     test_store<decltype(store)>(store, normalized_bins);
//   }
// }
//
// template<class StoreType, class OtherType>
// void test_cross_merge(StoreType& s, OtherType& o, std::function<std::vector<Bin>(std::vector<Bin>&)> c1, std::function<std::vector<Bin>(std::vector<Bin>&)> c2) {
//   std::vector<int> indexes{-1000, -1, 0, 1, 1000};
//   std::vector<double> counts{0, 1, 2, 4, 5, 10, 20, 100, 1000, 10000};
//
//   std::vector<Bin> bins;
//   for (const int& index: indexes) {
//     double total_count = 0;
//     for (const double& count: counts) {
//       s.add(index, count);
//       o.add(index, count);
//       total_count += count;
//     }
//     bins.emplace_back(index, total_count);
//   }
//   std::vector<Bin> normalized_bins = normalize_bins(c1(bins));
//   std::vector<Bin> normalized_other_bins = normalize_bins(c2(bins));
//   test_store<decltype(s)>(s, normalized_bins);
//   test_store<decltype(o)>(o, normalized_other_bins);
//
//
//   std::vector<Bin> merged_bins(normalized_bins);
//   merged_bins.insert(merged_bins.end(), normalized_other_bins.begin(), normalized_other_bins.end());
//   std::vector<Bin> normalized_merged_bins = normalize_bins(c1(merged_bins));
//   s.merge(o);
//   test_store<decltype(s)>(s, normalized_merged_bins);
//
//   std::vector<Bin> merged_other_bins(normalized_other_bins);
//   merged_other_bins.insert(merged_other_bins.end(), normalized_merged_bins.begin(), normalized_merged_bins.end());
//   std::vector<Bin> normalized_merged_other_bins = normalize_bins(c2(merged_other_bins));
//   o.merge(s);
//   test_store<decltype(o)>(o, normalized_merged_other_bins);
// }
//
// TEMPLATE_TEST_CASE("test cross merge", "[storetest]",
//   (std::pair<store_factory<CollapsingLowestDenseStore<8, A>>, collapsing_lowest_bins<8>>),
//   (std::pair<store_factory<CollapsingLowestDenseStore<128, A>>, collapsing_lowest_bins<128>>),
//   (std::pair<store_factory<CollapsingLowestDenseStore<1024, A>>, collapsing_lowest_bins<1024>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<8, A>>, collapsing_highest_bins<8>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<128, A>>, collapsing_highest_bins<128>>),
//   (std::pair<store_factory<CollapsingHighestDenseStore<1024, A>>, collapsing_highest_bins<1024>>),
//   (std::pair<store_factory<UnboundedSizeDenseStore<A>>, noops_collapsing_bins>)
//   ) {
//   std::vector<int> indexes{-1000, -1, 0, 1, 1000};
//   std::vector<double> counts{0, 1, 2, 4, 5, 10, 20, 100, 1000, 10000};
//
//   auto dense_store_1 = *store_factory<CollapsingLowestDenseStore<1024, A>>::new_store();
//   auto collapse_1 = collapsing_lowest_bins<1024>::collapse;
//   auto store_1 = *TestType::first_type::new_store();
//
//   test_cross_merge(store_1, dense_store_1, collapse_1, collapse_1);
//   auto dense_store_2 = store_factory<CollapsingHighestDenseStore<1024, A>>::new_store();
//   auto collapse_2 = collapsing_highest_bins<1024>::collapse;
//   auto dense_store_3 = store_factory<UnboundedSizeDenseStore<A>>::new_store();
//   auto collapse_3 = noops_collapsing_bins::collapse;
// }
//
// TEMPLATE_TEST_CASE("test store serialize - deserialize", "[serialization]",
//   CollapsingLowestStoreTestCase<8>,
//   CollapsingLowestStoreTestCase<128>,
//   CollapsingLowestStoreTestCase<1024>,
//   CollapsingHighestStoreTestCase<8>,
//   CollapsingHighestStoreTestCase<128>,
//   CollapsingHighestStoreTestCase<1024>,
//   UnboundedStoreSizeTestCase
// ) {
//   // Test empty store serialization
//   auto store = *TestType::new_store();
//   using StoreType = decltype(store);
//   std::stringstream stream;
//
//   store.serialize(stream);
//   StoreType deserialized_empty_store = StoreType::deserialize(stream);
//   REQUIRE(store.is_empty());
//   REQUIRE(deserialized_empty_store.is_empty());
//   REQUIRE(stream.peek() == std::istream::traits_type::eof());
//   REQUIRE(store == deserialized_empty_store);
//   stream.clear();
//
//   std::vector<int> indexes{-1000, -1, 0, 1, 1000};
//   std::vector<double> counts{0, 1, 2, 4, 5, 10, 20, 100, 1000, 10000};
//   for (int idx: indexes) {
//     for (double count: counts) {
//       store.add(idx, count);
//     }
//   }
//
//   store.serialize(stream);
//   auto deserialized_store = StoreType::deserialize(stream);
//   REQUIRE_FALSE(store.is_empty());
//   REQUIRE_FALSE(deserialized_store.is_empty());
//   REQUIRE(stream.peek() == std::istream::traits_type::eof());
//   REQUIRE(store == deserialized_store);
//
// }
// }

