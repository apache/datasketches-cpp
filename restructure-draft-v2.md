<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# datasketches-cpp restructure — draft v2

I have created this restructure document and its associated restructure-plan-detail.md with the help of Claude.  I have personally reviewed this material and it makes sense to me. But, of course, it might contain some mistakes. Please review these two documents and give me your feedback.
-- leerho@apache.org

**Status: 2026-09-22** 
* This is a draft for community discussion.
* All sequencing — the PR order, the per-area breakdown and the reviewer-load techniques — is in the companion `restructure-plan-detail.md`.
* Every header in the current tree is accounted for below: kept, moved, renamed, split, or deleted. 

**Goal:** When finished, I expect this to ship as a **major version** (6.0.0).

---

## 0. Precedent

This layout — a single include root, one subdirectory per component with a matching nested namespace, and an `internal/` subdirectory for everything outside the supported API — is the mainstream C++ library convention rather than anything novel.

* **Boost** — `boost/asio/*.hpp` and `boost/asio/detail/` in `boost::asio` and `boost::asio::detail`. Boost is also the precedent for our one deviation from path ≡ namespace: `boost/core/*.hpp` declares into plain `boost::`, exactly as our `common/` declares into `datasketches::` (convention 7).
* **Apache Arrow (C++)** — `arrow/compute/*.h` in `arrow::compute`, with private code in `arrow::compute::internal`. The closest sibling to what we are doing, and a fellow ASF project.
* **Apache Thrift (C++)** — `thrift/protocol/` in `apache::thrift::protocol`.
* **POCO** — `Poco/Net/`, `Poco/Util/`, `Poco/XML/` in `Poco::Net`, `Poco::Util`, `Poco::XML`. Textbook path ≡ namespace.
* **AWS SDK for C++** — one directory and one namespace per service: `aws/s3/` in `Aws::S3`. It also accepts the same stutter we do, in `Aws::S3::S3Client`, for the same reason: the component name has to survive a `using namespace`.
* **Protocol Buffers** — `google/protobuf/` in `google::protobuf`, with `google::protobuf::internal` for the private half.

Three more use the single include root and the `internal/` (or `detail/`) split but keep their namespaces deliberately flat, so they are precedent for the directory layout only: 

* **Abseil** (`absl/container/internal/`, namespace `absl::container_internal`) 
* **Folly** (`folly/container/detail/`, namespace `folly::detail`)
* **LLVM** (`include/llvm/ADT/`, namespace `llvm::`). 

Both choices are respectable; we take the nested side because it matches the Java package layout.

**Note:** `internal/` is a contract, not a wall: the library is header-only, so internal headers still ship and are still `#include`d by public headers. `internal` means "not for library users", not "private to one sketch" — tuple derives from `theta::internal` bases.


## 1. Definitions
**\<area>** - a place holder for a sketch family, grouping of similar sketches, or collection

**User API** — dedicated to one sketch family. Public, user-facing, supported: stable across minor releases, changed only at a major release. Tested. Includes (a) anything a user is meant to call directly, and (b) any type that appears in a User API signature as a return type or parameter, even if users never spell it.

* Location `include/datasketches/<area>/`, namespace `datasketches::<area>`.

**Internal API** — dedicated to one sketch family, not **User API**.

* Location `include/datasketches/<area>/internal/`, namespace `datasketches::<area>::internal`.

**Shared User API** — User API used by more than one family.

* Location `include/datasketches/common/`, namespace **`datasketches`** (not `datasketches::common` — see convention 7).

**Shared Internal API** — shared across families, not User API.

* Location `include/datasketches/internal/`, namespace `datasketches::internal`.

**RPD** abbreviation for the file restructure-plan-detail.md

**Shorthand** `X.hpp / _impl` means both `X.hpp` and `X_impl.hpp`; `←` marks a rename or move from today's location.



## 2. Conventions

1. **`_impl.hpp` files stay beside the header whose members they define.** They hold public-namespace member definitions; they are not internal.
2. **Constants namespaces stay intact.** `<area>_constants` namespaces and files are kept whole and re-parented (`datasketches::theta::theta_constants::DEFAULT_LG_K`), not folded or split. A constants file goes to `internal/` whole if nothing public references it (`hll_constants`, `req_constants`). If a file mixes public constants with internal code — only `cpc_common.hpp` — the constants namespace moves whole into the public sketch header. The same applies to other existing sub-namespaces (`random_utils`, `bit_array_ops`, `fdlibm`): re-parented, not dissolved.
3. **Tests mirror the include tree** under `test/datasketches/`. Cross-language `.sk` fixtures and `*_serialize_for_java.cpp` move with their sketch.
4. **Naming.** A User API header's prefix is the sketch name (`theta_sketch.hpp`, `var_opt_union.hpp`, `array_of_doubles_sketch.hpp`). The directory and namespace are the sketch name or an established abbreviation of it (`hll`, `cpc`, `kll`, `req`, `var_opt`, `aod`, `aos`). Internal headers have no naming requirement; if one already carries a prefix, leave it.
5. **Categories nest by category, not by dependency layer.** An area that is a category of sketches (`sampling`, `filters`, `tuple`) gets one subdirectory and sub-namespace per sketch or summary family, all siblings. Inheritance chains are not reflected in the tree (tuple depends on theta and is its sibling).
6. **A namespace never shares its name with a type inside it.** Hence `filters/bloom/` (type `bloom_filter`), `tuple/array_tuple/` (type `array<T>`).
7. **Path ≡ namespace, with one exception:** `include/datasketches/common/` declares into `datasketches::`, the way `boost/core/` declares into `boost::`. This keeps the top-level directory clean without forcing `common::` onto `serde`, `resize_factor`, `DEFAULT_SEED` and `string<A>` at their ~300 use sites.
8. **HLL** is brought to snake_case, `-internal.hpp` → `_impl.hpp`, and one public API per file, in its own PR before the move.

## 3. Repo root

```
include/datasketches/…     the library — the only thing that is installed
test/datasketches/…        mirrors include/datasketches/
benchmarks/                performance benchmarks; not installed
cmake/                     CMake package config templates
tools/                     rat-check.sh, Doxyfile, .rat-excludes, migration scripts
.github/                   CI workflows
build/                     tracked only for its .gitignore placeholder

CMakeLists.txt
version.cfg.in             base version; 5.3. → 6.0. at PR 1
LICENSE                    required by ASF release policy
NOTICE                     required by ASF release policy
README.md
CODE_OF_CONDUCT.md
CONTRIBUTING.md
.asf.yaml
.clang-tidy                stays at the root; clang-tidy and clangd search upward for it
.gitattributes
.gitignore
.pre-commit-config.yaml
```

## 4. Target tree

```
include/datasketches/
├── version.hpp                                   (generated)                        datasketches
│
├── common/                                                                          datasketches
│   ├── common_defs.hpp                           DEFAULT_SEED, resize_factor, string<A> — public half of today's file
│   ├── serde.hpp
│   ├── quantiles_sorted_view.hpp / _impl         exported by kll, quantiles, req
│   └── kolmogorov_smirnov.hpp / _impl            user-called; compares two quantiles_sorted_views
│
├── internal/                                                                        datasketches::internal
│   ├── common_utils.hpp                          ← common_defs.hpp: random_utils, read/write, log2, return_value_holder, unused
│   ├── memory_operations.hpp
│   ├── ceiling_power_of_2.hpp
│   ├── count_zeros.hpp
│   ├── inv_pow2_table.hpp
│   ├── fdlibm_log.hpp
│   ├── conditional_back_inserter.hpp
│   ├── conditional_forward.hpp
│   ├── murmur_hash3.hpp                          ← MurmurHash3.h
│   ├── xxhash64.hpp                              ← xxhash64.h
│   ├── bounds_binomial_proportions.hpp           ← common/
│   └── bounds_on_ratios_in_sampled_sets.hpp      ← theta/
│   ✗ optional.hpp                                deleted — std::optional
│
├── theta/                                                                           datasketches::theta
│   ├── theta_sketch.hpp / _impl
│   ├── theta_union.hpp / _impl
│   ├── theta_intersection.hpp / _impl
│   ├── theta_a_not_b.hpp / _impl
│   ├── theta_jaccard_similarity.hpp
│   ├── theta_constants.hpp                       namespace theta_constants kept whole
│   └── internal/                                                                    datasketches::theta::internal
│       ├── theta_update_sketch_base.hpp / _impl
│       ├── theta_union_base.hpp / _impl
│       ├── theta_intersection_base.hpp / _impl
│       ├── theta_set_difference_base.hpp / _impl
│       ├── theta_jaccard_similarity_base.hpp
│       ├── compact_theta_sketch_parser.hpp / _impl
│       ├── theta_helpers.hpp
│       ├── theta_comparators.hpp
│       ├── bit_packing.hpp
│       ├── binomial_bounds.hpp                   ← common/  (used only by theta and tuple)
│       └── bounds_on_ratios_in_theta_sketched_sets.hpp
│
├── tuple/                                                                           datasketches::tuple
│   ├── tuple_sketch.hpp / _impl                  tuple_sketch, update_tuple_sketch, compact_tuple_sketch
│   ├── tuple_union.hpp / _impl
│   ├── tuple_intersection.hpp / _impl
│   ├── tuple_a_not_b.hpp / _impl
│   ├── tuple_jaccard_similarity.hpp
│   ├── array_tuple/                                                                 datasketches::tuple::array_tuple
│   │   ├── array_tuple_sketch.hpp / _impl        array<T>, update/compact_array_tuple_sketch, default policy
│   │   ├── array_tuple_union.hpp / _impl
│   │   ├── array_tuple_intersection.hpp / _impl
│   │   └── array_tuple_a_not_b.hpp / _impl
│   ├── aod/                                                                         datasketches::tuple::aod
│   │   └── array_of_doubles_sketch.hpp           aliases over array_tuple<array<double>>
│   └── aos/                                                                         datasketches::tuple::aos
│       └── array_of_strings_sketch.hpp / _impl   aliases + string policy + string-serde compact class
│
├── hll/                                          (snake_cased; -internal.hpp → _impl.hpp)   datasketches::hll
│   ├── hll_sketch.hpp                            ← hll.hpp (hll_sketch_alloc, target_hll_type)
│   ├── hll_sketch_impl.hpp                       ← HllSketch-internal.hpp
│   ├── hll_union.hpp                             ← hll.hpp (hll_union_alloc, split out)
│   ├── hll_union_impl.hpp                        ← HllUnion-internal.hpp
│   └── internal/                                                                    datasketches::hll::internal
│       ├── hll_util.hpp                          ← HllUtil.hpp (hll_mode, hll_constants)
│       ├── hll_sketch_state.hpp / _impl          ← HllSketchImpl.hpp / HllSketchImpl-internal.hpp
│       ├── hll_sketch_state_factory.hpp          ← HllSketchImplFactory.hpp
│       ├── hll_array.hpp / _impl                 ← HllArray
│       ├── hll4_array.hpp / _impl                ← Hll4Array
│       ├── hll6_array.hpp / _impl                ← Hll6Array
│       ├── hll8_array.hpp / _impl                ← Hll8Array
│       ├── aux_hash_map.hpp / _impl              ← AuxHashMap
│       ├── coupon_list.hpp / _impl               ← CouponList
│       ├── coupon_hash_set.hpp / _impl           ← CouponHashSet
│       ├── coupon_iterator.hpp / _impl
│       ├── composite_interpolation_x_table.hpp / _impl
│       ├── cubic_interpolation.hpp / _impl
│       ├── harmonic_numbers.hpp / _impl
│       └── relative_error_tables.hpp / _impl
│       ✗ hll.private.hpp                         dissolved — include list moves to the bottom of hll_sketch.hpp / hll_union.hpp
│
├── cpc/                                                                             datasketches::cpc
│   ├── cpc_sketch.hpp / _impl                    + namespace cpc_constants, moved whole from cpc_common.hpp
│   ├── cpc_union.hpp / _impl
│   └── internal/                                                                    datasketches::cpc::internal
│       ├── cpc_common.hpp                        compressed_state, uncompressed_state
│       ├── cpc_compressor.hpp / _impl
│       ├── compression_data.hpp
│       ├── cpc_confidence.hpp
│       ├── cpc_util.hpp
│       ├── icon_estimator.hpp
│       ├── kxp_byte_lookup.hpp
│       └── u32_table.hpp / _impl
│
├── kll/                                                                             datasketches::kll
│   ├── kll_sketch.hpp / _impl                    (kll_constants inside, as today)
│   └── internal/
│       └── kll_helper.hpp / _impl
│
├── quantiles/                                                                       datasketches::quantiles
│   └── quantiles_sketch.hpp / _impl              (quantiles_constants inside, as today)
│
├── req/                                                                             datasketches::req
│   ├── req_sketch.hpp / _impl
│   └── internal/
│       ├── req_common.hpp                        req_constants — no public signature uses it
│       └── req_compactor.hpp / _impl
│
├── tdigest/                                                                         datasketches::tdigest
│   └── tdigest.hpp / _impl
│
├── density/                                                                         datasketches::density
│   └── density_sketch.hpp / _impl
│
├── frequencies/                                  ← fi/                              datasketches::frequencies
│   ├── frequent_items_sketch.hpp / _impl
│   └── internal/
│       └── reverse_purge_hash_map.hpp / _impl
│
├── sampling/                                                                        (category — no headers)
│   ├── var_opt/                                                                     datasketches::sampling::var_opt
│   │   ├── var_opt_sketch.hpp / _impl            (var_opt_constants inside, as today)
│   │   └── var_opt_union.hpp / _impl
│   └── ebpps/                                                                       datasketches::sampling::ebpps
│       ├── ebpps_sketch.hpp / _impl              (ebpps_constants inside, as today)
│       └── ebpps_sample.hpp / _impl              public — its const_iterator is in ebpps_sketch's signature
│
├── count/                                                                           datasketches::count
│   └── count_min_sketch.hpp / _impl              ← count_min.hpp (match the class name)
│
└── filters/                                                                         (category — no headers)
    └── bloom/                                                                       datasketches::filters::bloom
        ├── bloom_filter.hpp / _impl
        ├── bloom_filter_builder_impl.hpp
        └── internal/                                                                datasketches::filters::bloom::internal
            └── bit_array_ops.hpp
```

## 5. Test tree

```
test/datasketches/
├── common/                      test_allocator.hpp, test_type.hpp, catch_runner.cpp,
│                                integration_test.cpp, deserialize_hardening_test.cpp, quantiles_sorted_view_test.cpp
│                                ✗ optional_test.cpp (deleted with optional.hpp)
├── theta/                       theta_*_test.cpp, theta_sketch_serialize_for_java.cpp, *.sk
│   └── internal/                bit_packing_test.cpp, binomial_bounds_test.cpp ← common/test
├── tuple/                       tuple_*_test.cpp, tuple_sketch_serialize_for_java.cpp, engagement_test.cpp
│   ├── aod/                     array_of_doubles_sketch_test.cpp, aod_sketch_serialize_for_java.cpp, aod_sketch_deserialize_from_java_test.cpp
│   └── aos/                     array_of_strings_sketch_test.cpp, aos_sketch_serialize_for_java.cpp, aos_sketch_deserialize_from_java_test.cpp
├── kll/                         kll_*_test.cpp, kolmogorov_smirnov_test.cpp
├── quantiles/                   quantiles_*_test.cpp, kolmogorov_smirnov_test.cpp
├── sampling/
│   ├── var_opt/                 var_opt_*_test.cpp, var_opt_*_serialize_for_java.cpp
│   └── ebpps/                   ebpps_*_test.cpp
├── filters/
│   └── bloom/                   bloom_filter_*_test.cpp, bloom_filter_serialize_for_java.cpp
│       └── internal/            bit_array_ops_test.cpp
└── hll/, cpc/, req/, tdigest/, density/, frequencies/, count/   — one directory per area, same shape
```

`test/datasketches/` is on the test include path, so shared support is `#include <common/test_allocator.hpp>`.

## 6. Namespace hierarchy

```
datasketches                  serde, quantiles_sorted_view, kolmogorov_smirnov, DEFAULT_SEED, resize_factor, string<A>
├── internal                  murmur_hash3, xxhash64, memory ops, ceiling_power_of_2, count_zeros, bounds_binomial_proportions,
│   │                         bounds_on_ratios_in_sampled_sets, random_utils (sub-ns), …
│   └── fdlibm                vendored; stays nested
├── theta                     theta_sketch, update_theta_sketch, compact_theta_sketch, theta_union, theta_intersection,
│   │                         theta_a_not_b, theta_jaccard_similarity, theta_constants (sub-ns)
│   └── internal              *_base classes, compact_theta_sketch_parser, theta_helpers, comparators, bit_packing,
│                             binomial_bounds, bounds_on_ratios_in_theta_sketched_sets
├── tuple                     tuple_sketch, update_tuple_sketch, compact_tuple_sketch, tuple_union, tuple_intersection,
│   │                         tuple_a_not_b, tuple_jaccard_similarity       (derives from theta::internal bases)
│   ├── array_tuple           array<T>, update/compact_array_tuple_sketch, array_tuple_union/intersection/a_not_b
│   ├── aod                   update/compact_array_of_doubles_sketch, array_of_doubles_union/intersection/a_not_b
│   └── aos                   array_of_strings, update/compact_array_of_strings_tuple_sketch
├── hll                       hll_sketch, hll_union, target_hll_type
│   └── internal              hll_sketch_state, hll_array, hll4/6/8_array, coupon_list, coupon_hash_set, aux_hash_map,
│                             hll_mode, hll_constants (sub-ns)
├── cpc                       cpc_sketch, cpc_union, cpc_constants (sub-ns)
│   └── internal              cpc_compressor, u32_table, icon_estimator, compressed_state, uncompressed_state, …
├── kll                       kll_sketch, kll_constants (sub-ns)
│   └── internal              kll_helper
├── quantiles                 quantiles_sketch, quantiles_constants (sub-ns)
├── req                       req_sketch
│   └── internal              req_compactor, req_constants (sub-ns)
├── tdigest                   tdigest
├── density                   density_sketch
├── frequencies               frequent_items_sketch, frequent_items_error_type
│   └── internal              reverse_purge_hash_map
├── sampling                  (empty — category)
│   ├── var_opt               var_opt_sketch, var_opt_union, var_opt_constants (sub-ns)
│   └── ebpps                 ebpps_sketch, ebpps_sample, ebpps_constants (sub-ns)
├── count                     count_min_sketch
└── filters                   (empty — category)
    └── bloom                 bloom_filter, bloom_filter_builder
        └── internal          bit_array_ops (sub-ns)
```

Tests: no `::test` namespace. Each test file opens the namespace of the code under test plus an anonymous namespace for file-local helpers:

```cpp
namespace datasketches::theta {
namespace {
  theta_sketch make_sketch(...) { ... }
}
TEST_CASE("theta sketch: empty", "[theta_sketch]") { ... }
}
```

## 7. Decisions made so far

1. `fi/` → `frequencies/`, `datasketches::frequencies`. Same as ds-java and ds-go.
2. HLL: snake_case, `-internal.hpp` → `_impl.hpp`, `hll.hpp` split into `hll_sketch.hpp` + `hll_union.hpp`, `hll.private.hpp` dissolved, `HllSketchImpl` → `hll_sketch_state`. Separate PR, before the move. Consistent with all other sketches.
3. Constants namespaces are kept intact and re-parented; not folded.
4. `kolmogorov_smirnov` and `quantiles_sorted_view` → `common/`, namespace `datasketches`.
5. `binomial_bounds` → `theta/internal/` (verified: only `theta_sketch_impl.hpp` and `tuple_sketch_impl.hpp` include it).
6. `common_defs.hpp` splits into public `common/common_defs.hpp` (`DEFAULT_SEED`, `resize_factor`, `string<A>`) and `internal/common_utils.hpp`.
7. `ebpps_sample` is User API (`sampling::ebpps`) because its iterator appears in a public signature.
8. `count_min.hpp` → `count_min_sketch.hpp`.
9. `test/datasketches/common/` is kept together; `binomial_bounds_test.cpp` follows its header.
10. `common/` directory, `datasketches::` namespace (Boost model).
11. `cpc_constants` moves whole into `cpc_sketch.hpp`; the rest of `cpc_common.hpp` goes internal.
12. `sampling/` and `filters/` are categories: `sampling/var_opt/`, `sampling/ebpps/`, `filters/bloom/` with parallel namespaces. Stutter (`var_opt::var_opt_sketch`) is accepted house style.
13. `tuple/` is a category, nested by summary family, siblings not layers: `array_tuple/`, `aod/`, `aos/`. Directory and namespace abbreviated (`aod`, `aos` — already the prefix of the Java-interop tests); file and type names spelled out.
14. No `::test` namespace.
15. `__rat_negative_test.hpp` is not in the repo (untracked, ignored); nothing to do.
16. The three `bounds_*` headers (`bounds_binomial_proportions`, `bounds_on_ratios_in_sampled_sets`, `bounds_on_ratios_in_theta_sketched_sets`) are internal: probability math functions called from jaccard and var_opt, not meaningful to users on their own. Java marks them `public` only so its tests can reach them across packages — a visibility artifact of not using JPMS, not a statement of API intent — so `internal/` is the C++ expression of what Java meant. One line in the 6.0 release notes ("moved to internal") is enough.

17. ds-cpp adopts the TCK pattern (RPD step 7b) and the full three-language matrix (RPD step 7c), including **testing ds-cpp against `cpp` snapshots**. The self-regression leg is deliberate, not incidental: it is why ds-java added it. The hub model also removes the n² foreign-toolchain problem — no repo needs another language's build to run compatibility tests.

18. **Branching model** Standard project model applies: feature branch → PR → master for every RPD step; no intermediate integration branch. When RPD step 7 is complete, master merges into a new `6.0.x` release branch, `6.0.0-RC1` is tagged, and the Apache vote runs. `5.2.x` already serves as the 5.x maintenance line. Master's `version.cfg.in` moves from `5.3.` to `6.0.` at PR 1 (the timestamp components are the dev-build marker; CMake's `project(VERSION)` cannot take a `-SNAPSHOT` suffix).

## 8. Open items

**A. Downstream.** datasketches-python, datasketches-postgresql and datasketches-bigquery depend on a *released* ds-cpp, not on this repo, so they cannot be updated before 6.0 releases — the release is what unblocks them, not a prerequisite for it. `6.0.0-RC1` is a real artifact, and the Apache vote period is when downstream maintainers can build against it and report breakage; a genuine problem found there is grounds to respin the RC. After `6.0.0` is final, each project adopts on its own schedule and stays on 5.x until it does. 

What this project owes them: (a) an upgrade note in the release notes mapping old → new include paths and namespaces, (b) advance notice on dev@ before PR 4 lands, and (c) the migration script from PRs 4–5 kept in `tools/`, so each downstream can run the same rewrite over its own sources.

**B. Open PRs** #520, #509, #466 will conflict with the move. Land or close #520 and #509 first; the DDSketch author should target the new layout (`include/datasketches/ddsketch/`, `datasketches::ddsketch`).

**C. TCK coordination.** `apache/datasketches-tck` pins ds-cpp at `fe0261a` in its `config.toml` — 16 commits behind master as of 2026-09-22, predating #526's compact-theta serialization change. `6.0.0` is the natural moment to push a fresh pin (`mise run tck -- snapshots update cpp v6.0.0` in a TCK PR). The TCK builds ds-cpp from source via cmake+ctest, so it is insensitive to the include/namespace restructure, but it *is* sensitive to where the serialize tests write their output — see RPD step 7b.

## 9. Sequencing

The PR sequence, its rules, the per-area breakdown and the reviewer-load techniques are in `restructure-plan-detail.md`.
