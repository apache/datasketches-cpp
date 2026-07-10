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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <streambuf>
#include <vector>

namespace
{

using Sketch = datasketches::count_min_sketch<uint64_t>;

// Roughly 99.9% confidence. Serialization cost then scales with the bucket count.
constexpr uint8_t NUM_HASHES = 7;

Sketch makeSketch(uint32_t num_buckets)
{
    Sketch sketch(NUM_HASHES, num_buckets);
    for (uint64_t i = 0; i < NUM_HASHES; ++i)
        sketch.update(i, i + 1);
    return sketch;
}

class fixed_buffer_streambuf final: public std::streambuf
{
public:
    explicit fixed_buffer_streambuf(std::vector<uint8_t>& buffer): buffer_(buffer), position_(0) {}

    void reset()
    {
        position_ = 0;
    }

    size_t bytes_written() const
    {
        return position_;
    }

protected:
    std::streamsize xsputn(const char* data, std::streamsize size) override
    {
        const size_t requested = static_cast<size_t>(size);
        const size_t available = buffer_.size() - position_;
        const size_t bytes_to_write = std::min(requested, available);
        if (bytes_to_write > 0) {
            std::memcpy(buffer_.data() + position_, data, bytes_to_write);
            position_ += bytes_to_write;
        }
        return static_cast<std::streamsize>(bytes_to_write);
    }

    int_type overflow(int_type ch) override
    {
        if (traits_type::eq_int_type(ch, traits_type::eof()))
            return traits_type::not_eof(ch);
        if (position_ == buffer_.size())
            return traits_type::eof();
        buffer_[position_++] = static_cast<uint8_t>(traits_type::to_char_type(ch));
        return ch;
    }

private:
    std::vector<uint8_t>& buffer_;
    size_t position_;
};

void BM_CountMinGetSerializedSizeBytes(benchmark::State& state)
{
    const auto sketch = makeSketch(static_cast<uint32_t>(state.range(0)));

    for (auto _ : state) {
        const auto size = sketch.get_serialized_size_bytes();
        benchmark::DoNotOptimize(size);
    }

    state.SetItemsProcessed(state.iterations());
}

void BM_CountMinSerializeVector(benchmark::State& state)
{
    const auto sketch = makeSketch(static_cast<uint32_t>(state.range(0)));
    const auto serialized_size = sketch.get_serialized_size_bytes();

    for (auto _ : state) {
        auto bytes = sketch.serialize();
        benchmark::DoNotOptimize(bytes.data());
        benchmark::DoNotOptimize(bytes.size());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(serialized_size));
}

void BM_CountMinSerializeOStream(benchmark::State& state)
{
    const auto sketch = makeSketch(static_cast<uint32_t>(state.range(0)));
    const auto serialized_size = sketch.get_serialized_size_bytes();
    std::vector<uint8_t> bytes(serialized_size);
    fixed_buffer_streambuf stream_buffer(bytes);
    std::ostream os(&stream_buffer);

    for (auto _ : state) {
        stream_buffer.reset();
        os.clear();
        sketch.serialize(os);
        benchmark::DoNotOptimize(stream_buffer.bytes_written());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(serialized_size));
}

void BM_CountMinSerializeToSink(benchmark::State& state)
{
    const auto sketch = makeSketch(static_cast<uint32_t>(state.range(0)));
    const auto serialized_size = sketch.get_serialized_size_bytes();
    std::vector<uint8_t> bytes(serialized_size);

    for (auto _ : state) {
        uint8_t* ptr = bytes.data();
        const auto bytes_written = sketch.serialize_to([&ptr](const void* data, size_t size) {
            std::memcpy(ptr, data, size);
            ptr += size;
        });
        benchmark::DoNotOptimize(ptr);
        benchmark::DoNotOptimize(bytes_written);
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(serialized_size));
}

}

BENCHMARK(BM_CountMinGetSerializedSizeBytes)->RangeMultiplier(8)->Range(1024, 65536);
BENCHMARK(BM_CountMinSerializeVector)->RangeMultiplier(8)->Range(1024, 65536);
BENCHMARK(BM_CountMinSerializeOStream)->RangeMultiplier(8)->Range(1024, 65536);
BENCHMARK(BM_CountMinSerializeToSink)->RangeMultiplier(8)->Range(1024, 65536);
