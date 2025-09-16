//
// Created by Andrea Novellini on 03/09/2025.
//

#ifndef STORE_HPP
#define STORE_HPP

#include <iterator>
#include <concepts>
#include "bin.hpp"


namespace datasketches {

/**
 * @concept store_concept
 * @tparam S Candidate store type.
 * @brief Minimal interface a bin-count store must satisfy to work with {@link DDSketch}.
 *
 * **Iteration (read-only):**
 * - `s.begin()` / `s.end()` form an input range.
 * - `*s.begin()` yields `Bin` **by value** (index, count).
 *
 * **Core operations:**
 * - `s.add(int index) -> void`
 * - `s.add(int index, double count) -> void`
 * - `s.add(const Bin&) -> void`
 * - `s.clear() -> void`
 * - `s.merge(const S&) -> void`
 *
 * **Queries (const):**
 * - `s.is_empty() -> bool`
 * - `s.get_min_index() -> int`  (lowest non-empty bin)
 * - `s.get_max_index() -> int`  (highest non-empty bin)
 * - `s.get_total_count() -> double`
 *
 * **Semantics (brief):**
 * - Indices are integer bin IDs from the index mapping.
 * - `merge` accumulates counts; total_count is additive.
 * - Iteration visits non-empty bins in ascending index order.
 */
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
