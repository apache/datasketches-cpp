//
// Created by Andrea Novellini on 03/09/2025.
//

#ifndef STORE_HPP
#define STORE_HPP

#include <iterator>
#include <concepts>
#include "bin.hpp"


namespace datasketches {
template<class S>
concept store_concept =
  // range of Bin (by value is fine; you already return Bin by value)
  requires(const S& s) {
  { s.begin() } -> std::input_iterator;
  { s.end()   };
  { *s.begin() } -> std::same_as<Bin>;
  } &&
  // core operations ddsketch needs
  requires(S& s, const S& cs, int i, double c, const Bin& b) {
  { s.add(i) } -> std::same_as<void>;
  { s.add(i, c) } -> std::same_as<void>;
  { s.add(b) } -> std::same_as<void>;
  { s.clear() } -> std::same_as<void>;
  { cs.is_empty() } -> std::same_as<bool>;
  { cs.get_min_index() } -> std::same_as<int>;
  { cs.get_max_index() } -> std::same_as<int>;
  { cs.get_total_count() } -> std::same_as<double>;
  { s.merge(cs) } -> std::same_as<void>;
  };
}

#endif //STORE_HPP
