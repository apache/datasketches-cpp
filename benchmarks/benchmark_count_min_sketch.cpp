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

#include <benchmark/benchmark.h>
#include <count_min.hpp>

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace
{

using Sketch = datasketches::count_min_sketch<uint64_t>;

// Roughly 99.9% confidence and 0.1% relative error:
// suggest_num_hashes(0.999) == 7 and suggest_num_buckets(0.001) == 2719.
constexpr uint8_t NUM_HASHES = 7;
constexpr uint32_t NUM_BUCKETS = 2719;

std::vector<uint64_t> makeUInt64Keys(size_t size)
{
    std::vector<uint64_t> keys;
    keys.reserve(size);
    for (size_t i = 0; i < size; ++i)
        keys.push_back(static_cast<uint64_t>(i * 0x9e3779b97f4a7c15ULL));
    return keys;
}

std::vector<std::string> makeStringKeys(size_t size)
{
    std::vector<std::string> keys;
    keys.reserve(size);
    for (size_t i = 0; i < size; ++i)
        keys.push_back("countmin-key-" + std::to_string(i * 2654435761ULL));
    return keys;
}

int64_t totalStringBytes(const std::vector<std::string> & keys)
{
    int64_t bytes = 0;
    for (const auto & key : keys)
        bytes += static_cast<int64_t>(key.size());
    return bytes;
}

void BM_CountMinUpdateUInt64(benchmark::State & state)
{
    const auto keys = makeUInt64Keys(static_cast<size_t>(state.range(0)));
    for (auto _ : state)
    {
        state.PauseTiming();
        Sketch sketch(NUM_HASHES, NUM_BUCKETS);
        state.ResumeTiming();

        for (const auto key : keys)
            sketch.update(&key, sizeof(key), 1);

        benchmark::DoNotOptimize(sketch.get_total_weight());
    }

    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(keys.size()));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(keys.size() * sizeof(uint64_t)));
}

void BM_CountMinUpdateStringBytes(benchmark::State & state)
{
    const auto keys = makeStringKeys(static_cast<size_t>(state.range(0)));
    const auto bytes_per_iteration = totalStringBytes(keys);

    for (auto _ : state)
    {
        state.PauseTiming();
        Sketch sketch(NUM_HASHES, NUM_BUCKETS);
        state.ResumeTiming();

        for (const auto & key : keys)
            sketch.update(key.data(), key.size(), 1);

        benchmark::DoNotOptimize(sketch.get_total_weight());
    }

    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(keys.size()));
    state.SetBytesProcessed(state.iterations() * bytes_per_iteration);
}

void BM_CountMinEstimateUInt64(benchmark::State & state)
{
    const auto keys = makeUInt64Keys(static_cast<size_t>(state.range(0)));
    Sketch sketch(NUM_HASHES, NUM_BUCKETS);
    for (const auto key : keys)
        sketch.update(&key, sizeof(key), 1);

    uint64_t sum = 0;
    for (auto _ : state)
    {
        for (const auto key : keys)
            sum += sketch.get_estimate(&key, sizeof(key));

        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(keys.size()));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(keys.size() * sizeof(uint64_t)));
}

void BM_CountMinEstimateStringBytes(benchmark::State & state)
{
    const auto keys = makeStringKeys(static_cast<size_t>(state.range(0)));
    const auto bytes_per_iteration = totalStringBytes(keys);

    Sketch sketch(NUM_HASHES, NUM_BUCKETS);
    for (const auto & key : keys)
        sketch.update(key.data(), key.size(), 1);

    uint64_t sum = 0;
    for (auto _ : state)
    {
        for (const auto & key : keys)
            sum += sketch.get_estimate(key.data(), key.size());

        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(keys.size()));
    state.SetBytesProcessed(state.iterations() * bytes_per_iteration);
}

}

BENCHMARK(BM_CountMinUpdateUInt64)->RangeMultiplier(8)->Range(1024, 65536);
BENCHMARK(BM_CountMinUpdateStringBytes)->RangeMultiplier(8)->Range(1024, 65536);
BENCHMARK(BM_CountMinEstimateUInt64)->RangeMultiplier(8)->Range(1024, 65536);
BENCHMARK(BM_CountMinEstimateStringBytes)->RangeMultiplier(8)->Range(1024, 65536);
