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

#ifndef DDSKETCH_IMPL_H
#define DDSKETCH_IMPL_H

#include <iomanip>
#include <iostream>
#include "ddsketch.hpp"
#include "store_factory.hpp"
namespace datasketches {

template<store_concept Store, class Mapping>
DDSketch<Store, Mapping>::DDSketch(double relative_accuracy): DDSketch(Mapping(relative_accuracy)) {}

template<store_concept Store, class Mapping>
DDSketch<Store, Mapping>::DDSketch(const Mapping& index_mapping):
  DDSketch(*store_factory<Store>::new_store(), *store_factory<Store>::new_store(), index_mapping, 0.0, 0.0) {}


template<store_concept Store, class Mapping>
DDSketch<Store, Mapping>::DDSketch(const Store& positive_store, const Store& negative_store, const Mapping& mapping, const double& zero_count, const double& min_indexed_value):
  positive_store(std::move(positive_store)),
  negative_store(std::move(negative_store)),
  index_mapping(std::move(mapping)),
  zero_count(zero_count),
  min_indexed_value(std::max(min_indexed_value, mapping.min_indexable_value())),
  max_indexed_value(mapping.max_indexable_value()) {}


template<store_concept Store, class Mapping>
void DDSketch<Store, Mapping>::check_value_trackable(const double& value) const {
  if (value < -max_indexed_value || value > max_indexed_value) {
    throw std::invalid_argument("input value is outside the range that is tracked by the sketch.");
  }
}

template<store_concept Store, class Mapping>
template<store_concept OtherStore>
void DDSketch<Store, Mapping>::check_mergeability(const DDSketch<OtherStore, Mapping>& other) const {
  if (index_mapping != other.index_mapping) {
    throw std::invalid_argument("sketches are not mergeable because they do not use the same index mappings.");
  }
}

template<store_concept Store, class Mapping>
void DDSketch<Store, Mapping>::update(const double& value, const double& count) {
  check_value_trackable(value);

  if (count < 0.0) {
    throw std::invalid_argument("count cannot be negative.");
  }

  if (value > min_indexed_value) {
    positive_store.add(index_mapping.index(value), count);
  } else if (value < -min_indexed_value) {
    negative_store.add(index_mapping.index(-value), count);
  } else {
    zero_count += count;
  }
}

template<store_concept Store, class Mapping>
template<store_concept OtherStore>
void DDSketch<Store, Mapping>::merge(const DDSketch<OtherStore, Mapping>& other) {
  check_mergeability<OtherStore>(other);
  negative_store.merge(other.negative_store);
  positive_store.merge(other.positive_store);
  zero_count += other.zero_count;
}

template<store_concept Store, class Mapping>
bool DDSketch<Store, Mapping>::is_empty() const {
  return zero_count == 0.0 && positive_store.is_empty() && negative_store.is_empty();
}

template<store_concept Store, class Mapping>
void DDSketch<Store, Mapping>::clear() {
  negative_store.clear();
  positive_store.clear();
  zero_count = 0.0;
}

template<store_concept Store, class Mapping>
double DDSketch<Store, Mapping>::get_count() const {
  return zero_count + negative_store.get_total_count() + positive_store.get_total_count();
}

template<store_concept Store, class Mapping>
double DDSketch<Store, Mapping>::get_sum() const {
  double sum = 0.0;
  for (const Bin& bin : negative_store) {
    sum -= index_mapping.value(bin.getIndex()) * bin.getCount();
  }
  for (const Bin& bin : positive_store) {
    sum += index_mapping.value(bin.getIndex()) * bin.getCount();
  }
  return sum;
}

template<store_concept Store, class Mapping>
double DDSketch<Store, Mapping>::get_min() const {
  if (!negative_store.is_empty()) {
    return -index_mapping.value(negative_store.get_max_index());
  }
  if (zero_count > 0.0) {
    return 0.0;
  }
  return index_mapping.value(positive_store.get_min_index());
}

template<store_concept Store, class Mapping>
double DDSketch<Store, Mapping>::get_max() const {
  if (!positive_store.is_empty()) {
    return index_mapping.value(positive_store.get_max_index());
  }
  if (zero_count > 0.0) {
    return 0.0;
  }
  return -index_mapping.value(negative_store.get_min_index());
}

template<store_concept Store, class Mapping>
double DDSketch<Store, Mapping>::get_rank(const double &item) const {
  double rank = 0.0;

  if (!negative_store.is_empty()) {
    for (auto it = negative_store.rbegin(); it != negative_store.rend() && index_mapping.value((*it).getIndex()) <= item; ++it) {
      rank += (*it).getCount();
    }
  }
  if (item >= 0) {
    rank += zero_count;
  }
  if (!positive_store.is_empty()) {
    for (auto it = positive_store.begin(); it != positive_store.end() && index_mapping.value((*it).getIndex()) <= item; ++it) {
      rank += (*it).getCount();
    }
  }
  return rank / get_count();
}


template<store_concept Store, class Mapping>
double DDSketch<Store, Mapping>::get_quantile(const double& rank) const {
  return get_quantile(rank, get_count());
}

template<store_concept Store, class Mapping>
double DDSketch<Store, Mapping>::get_quantile(const double& rank, const double& count) const {
  if (rank < 0.0 || rank > 1.0) {
    throw std::invalid_argument("rank must be in [0.0, 1.0]");
  }

  if (count == 0.0) {
    throw std::runtime_error("no such element");
  }

  const double target_rank = rank * (count - 1.0);
  double n = 0.0;

  for (auto it = negative_store.rbegin(); it != negative_store.rend(); ++it) {
    const Bin& bin = *it;
    if ((n += bin.getCount()) > target_rank) {
      return -index_mapping.value(bin.getIndex());
    }
  }

  if ((n += zero_count) > target_rank) {
    return 0.0;
  }

  for (auto it = positive_store.begin(); it != positive_store.end(); ++it) {
    const Bin& bin = *it;
    if ((n += bin.getCount()) > target_rank) {
      return index_mapping.value(bin.getIndex());
    }
  }
  throw std::invalid_argument("no such element");
}

template<store_concept Store, class Mapping>
void DDSketch<Store, Mapping>::serialize(std::ostream& os) const {
  index_mapping.serialize(os);

  write(os, zero_count);


  auto val = positive_store.get_serialized_size_bytes();
  write(os, positive_store.get_serialized_size_bytes());
  positive_store.serialize(os);

  val = negative_store.get_serialized_size_bytes();
  write(os, negative_store.get_serialized_size_bytes());
  negative_store.serialize(os);
}

template<store_concept Store, class Mapping>
DDSketch<Store, Mapping> DDSketch<Store, Mapping>::deserialize(std::istream &is) {
  Mapping deserialized_index_mapping = Mapping::deserialize(is);
  const auto deserialized_zero_count = read<double>(is);

  const auto positive_store_serialized_size  = read<int>(is);

  std::string pos_buf(positive_store_serialized_size, '\0');
  is.read(pos_buf.data(), pos_buf.size());
  std::stringstream pos_stream(pos_buf);
  Store deserialized_positive_store = Store::deserialize(pos_stream);

  const auto negative_store_serialized_size  = read<int>(is);
  std::string neg_buf(negative_store_serialized_size, '\0');
  is.read(neg_buf.data(), neg_buf.size());
  std::stringstream neg_stream(neg_buf);
  Store deserialized_negative_store = Store::deserialize(neg_stream);

  DDSketch<Store, Mapping> ddsketch(deserialized_positive_store, deserialized_negative_store, deserialized_index_mapping);
  ddsketch.zero_count = deserialized_zero_count;
  return ddsketch;
}

template<store_concept Store, class Mapping>
bool DDSketch<Store, Mapping>::operator==(const DDSketch<Store, Mapping>& other) const {
  return positive_store == other.positive_store &&
    negative_store == other.negative_store &&
      index_mapping == other.index_mapping &&
        zero_count == other.zero_count &&
          min_indexed_value == other.min_indexed_value &&
            max_indexed_value == other.max_indexed_value;
}

}

#endif
