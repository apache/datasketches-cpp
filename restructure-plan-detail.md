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

# datasketches-cpp restructure — sequencing detail (DRAFT)

Companion to `restructure-draft-v2.md`, which holds the definitions, target tree, namespace hierarchy, current decisions and open items. **All sequencing lives here:** the PR order, the per-area breakdown for steps 4 and 5, and the techniques for keeping each PR reviewable.

---

## Sequencing: C++17 flag first, restructure second, feature adoption last

Hold the C++17 syntactic sugar until the end — with exactly two exceptions, both because they touch the same lines the restructure touches anyway.

### Why the flag flip goes first

It's a one-line change (`CMAKE_CXX_STANDARD 11` → `17`, `cxx_std_11` → `cxx_std_17`) and it's already de-risked:

- Nothing C++17 removed is in use (`auto_ptr`, `random_shuffle`, `bind1st`, `register`, `throw()` specs — the only `register` hits are comments).
- Every CI compiler (GCC 9–15, Clang 18, MSVC 2025, macOS Clang) fully supports 17.
- One CI job already builds at `-DCMAKE_CXX_STANDARD=17` with libc++ hardening.

So it's a tiny PR whose only job is to prove the platform question on every target before you invest in the move. If some downstream (datasketches-python's build matrix, say) has a problem, you learn that from a one-line PR, not a 200-file one.

Doing it the other way round has no upside: you'd write `namespace datasketches { namespace theta { namespace internal {` and `}}}` into every file, then rewrite every one of those lines again for 17.

### The two exceptions

1. **Nested namespace syntax** — `namespace datasketches::theta::internal {`. The restructure rewrites the namespace open/close lines in every file. Write them once, in the C++17 form.
2. **`std::optional`** — deleting `optional.hpp` is part of the tree cleanup. At 17 the file is already a shim (`common/include/optional.hpp:25-27` does `#include <optional>; using std::optional;`), so deletion means `optional<T>` → `std::optional<T>` in its five users (kll, quantiles, req, ebpps ×2) and dropping `common/test/optional_test.cpp`. Do it right after the flag flip so the move doesn't carry a dead file.

Everything else — `if constexpr`, structured bindings, `string_view`, `[[nodiscard]]`, `[[maybe_unused]]` replacing `unused()`, `inline constexpr` for the constants — waits. The constants one is the tempting case, since the restructure moves those exact declarations out of `theta_constants` into `theta`; resist anyway. `const` at namespace scope already has internal linkage, so nothing is wrong today, and mixing it in makes the move diff non-trivial to verify.

## PR sequence overview

**Rules** 

* create a PR with complete tests after each step
* ask permission before creating each PR and before moving to the next step.

| # | PR | Notes |
|---|---|---|
| 0 | Rename default branch `master` → `main` | Independent of the restructure; do it first so every PR below targets `main`. Touch points: `.asf.yaml` protected branch, six workflows triggering on `master`, committers' clones. ASF repos rename via INFRA, not GitHub settings. |
| 1 | Flip to C++17; `version.cfg.in` → `6.0.` | One line each; CI already covers 17 on one job; nothing removed-in-17 is in use. |
| 2 | Delete `optional.hpp` → `std::optional` | Five users + one test. |
| 3 | HLL naming fixup: snake_case, `_impl`, **and** the `hll.hpp` split | Split here so step 4 moves final names; a 1→2 split defeats rename detection anyway. |
| 4 | Move/rename into target directories, fix `#include` paths, **CMake: add `include/` root** | Per area: common → the eleven leaf areas (any order) → theta → tuple. Pure move; `git diff -M` shows only `#include` lines. Also move the two root config files that belong with the tooling: `.rat-excludes` → `tools/.rat-excludes` (update the `cp` line in `tools/rat-check.sh`; RAT matches basenames, so no exclusion pattern changes) and `Doxyfile` → `tools/Doxyfile` (update `.github/workflows/doxygen.yml` to `doxygen tools/Doxyfile`; doxygen resolves `INPUT`/`OUTPUT_DIRECTORY` against the CWD, so the file itself needs no path edits). `.clang-tidy` stays at the root — clang-tidy and clangd find it by walking up from the file being checked. Rewrite `Doxyfile`'s `INPUT` from the twelve per-area include dirs to `include/datasketches` — which also fixes a live documentation bug: that list omits `tdigest/include` and `filters/include` today, so those two sketches are absent from the published docs. |
| 5 | Namespace rewrite, C++17 nested form, temporary `using` compat blocks | Per area, same order. Biggest risk step. No CMake work needed. |
| 6 | Cleanup: delete compat blocks, drop per-area include roots, one target, one `install(DIRECTORY)` | **Preserve the `GENERATE` and `SERDE_COMPAT` cmake options and the per-area test CMakeLists that gate on them.** `apache/datasketches-tck` generates the C++ corpus with `cmake -DGENERATE=true … && ctest`, so dropping or renaming those options breaks it. |
| 7 | Downstream checkpoint: python, postgresql, bigquery | Before 6.0 tags. |
| 7b | Adopt the ds-java cross-language fixture pattern | Rename `java/` → `serialization_test_data/`, matching ds-java and ds-go. Port ds-java's `tools/download_serialization_test_data.sh`: it downloads a pinned `apache/datasketches-tck` tarball and extracts `serialization/<lang>/snapshots/*.sk` into `<lang>_generated_files/` — no ds-java checkout, no JDK, no Maven. `serde_compat.yml` becomes checkout → run script → cmake `-DSERDE_COMPAT=true` → build → test, and the same command populates a developer's tree (`java` and `go` both available). Touch points: 14 `*_deserialize_from_java_test.cpp` input paths, `.gitignore:45`, `.github/workflows/serde_compat.yml`. Hoist the input path into a shared constant in `test/datasketches/common/`. **Leave the 14 `*_serialize_for_java.cpp` tests writing `*_cpp.sk` to the build CWD:** the TCK's `internal/snapshots/cpp.go` runs cmake+ctest and then `collectSnapshots(build, …)` filtering `*_cpp.sk`, so moving that output would make the TCK find zero C++ snapshots. Copy them into `serialization_test_data/cpp_generated_files/` from the script instead, if a local copy is wanted. |
| 7c | Consume `go` and `cpp` snapshots, not just `java` | The TCK uses identical case names across languages, varying only the directory and the `_<lang>` suffix (`aod_1_n0_java.sk` / `_go.sk` / `_cpp.sk`), so the 14 `*_deserialize_from_java_test.cpp` files can be parameterized by source language and run once per language — matching ds-java's `check_{cpp,go,java}_files` matrix. Testing against `cpp` snapshots is the regression check ds-cpp lacks entirely today: can 6.0 still read what 5.x wrote? Most valuable across a major version. Rename the files `*_deserialize_from_<lang>_test.cpp` or drop the producer from the name. |
| 8+ | C++17 feature adoption, one feature class per PR | `if constexpr`, structured bindings, `string_view`, `[[nodiscard]]`, `[[maybe_unused]]`, `inline constexpr`, … |

Steps 4 and 5 are broken down per area below.

### Why separate the move from the namespace rewrite

A reviewer can verify a pure move in minutes with `git diff -M`, while a move-plus-edit forces a full re-read of every file and defeats git's rename tracking (`git log --follow`, `git blame`) for the rest of the repo's life. Keep the move (v2 step 4) as content-pure as possible and leave every namespace line to step 5.

### Open PRs will collide

See v2 open item C. Land or close #520 and #509 before step 4 starts; #466 (DDSketch) should target the new layout rather than rebase across it.

---

## Q1. Why delete `optional.hpp` and `__rat_negative_test.hpp`? 

**`optional.hpp`**: `std::optional` *is* the C++17 alternative — that's the whole reason to delete the file. It's a hand-written substitute for `std::optional` from when the repo had to work on C++11; its own comment (`common/include/optional.hpp:23`) says "simplistic substitute for std::optional until we require C++17". At C++17 the file already turns itself into `#include <optional>; using std::optional;` and the hand-rolled class below is dead code. Deleting it is: five headers (kll, quantiles, req, ebpps ×2) change `optional<T>` → `std::optional<T>`, and `common/test/optional_test.cpp` goes away since you don't test the standard library.

**`__rat_negative_test.hpp`**: strike it from the plan — **it isn't in the repo**. `git ls-files` doesn't know it and `git status --ignored` shows it as ignored, so it's a stray local file in the checkout (almost certainly a leftover from testing Apache RAT license-header checks: an empty header with no license, to confirm RAT flags it). Nothing to do. (`restructure-draft.md` lists it as deleted; ignore that line.)

---

## Q2. Can the move/rename and namespace rewrite be done one sketch at a time to reduce reviewer load?

Yes, and the dependency graph makes it easy. The cross-area include graph is nearly empty: **tuple → theta is the only edge**; every other area depends solely on `common`. So eleven of the fourteen areas can migrate independently, in any order, with nobody downstream to break.

Recommended sequence, one PR per row. Each row is a *pair* of PRs — the move (v2 step 4) then the namespace rewrite (v2 step 5) for that area — or a single PR doing both for the smaller areas:

| PR | Area | Headers | Tests | Notes |
|---|---|---|---|---|
| A | scaffolding | — | — | Add `include/` as a second include root alongside the existing per-area ones. Both resolve during the migration. |
| B | common | 20 | 7 | Root of the graph, so first. Public headers stay in `datasketches::`; private ones go to `datasketches::internal`, plus a temporary block `namespace datasketches { using internal::copy_from_mem; … }` so unmigrated areas keep compiling. |
| C–M | kll, quantiles, req, cpc, frequencies, sampling, count, density, tdigest, filters, hll | 2–32 | 1–14 | Any order, any parallelism. Each PR qualifies its own `internal::` uses and stops relying on B's compat block. |
| N | theta | 26 | 9 | Adds a temporary `namespace datasketches { using theta::internal::theta_update_sketch_base; … }` for the six base classes tuple uses. |
| O | tuple | 20 | 15 | Qualifies to `theta::internal::`, removes N's compat block. |
| P | cleanup | — | — | Delete B's compat block, drop the per-area include dirs, collapse the CMake targets, one `install(DIRECTORY)`. |

Per-area sizes today:

| area | headers | test files |
|---|---|---|
| common | 20 | 7 |
| hll | 32 | 14 |
| cpc | 14 | 6 |
| kll | 4 | 6 |
| fi → frequencies | 4 | 5 |
| theta | 26 | 9 |
| sampling | 8 | 10 |
| tuple | 20 | 15 |
| req | 5 | 4 |
| quantiles | 2 | 5 |
| count | 2 | 2 |
| density | 2 | 1 |
| tdigest | 2 | 5 |
| filters | 4 | 5 |

Largest PR is HLL at 32 headers + 14 tests, but its snake_case rename and `hll.hpp` split happen earlier, in v2 step 3, so by the time this table applies HLL is already in final file names.

Note that `sampling`, `filters` and `tuple` gain subdirectories (v2 conventions 5–6), so their move PRs also create `var_opt/`, `ebpps/`, `bloom/`, `array_tuple/`, `aod/`, `aos/` and the matching sub-namespaces.

Don't reach for `inline namespace` as the transition mechanism — with `theta` and `hll` both inline, `datasketches::internal` becomes ambiguous between `datasketches::internal` and `datasketches::theta::internal`. Using-declarations are explicit and can't do that.

### Reducing reviewer load within each PR (matters more than PR count)

- **Three commits per PR, in this order**: (1) `git mv` only — content byte-identical, GitHub collapses these as "renamed without changes"; (2) `#include` line fixes only; (3) namespace lines and `internal::` qualifications only. The reviewer reads a rename list, then a diff that's nothing but `#include` lines, then a diff that's nothing but namespace lines.
- **Commit the script.** Steps 2 and 3 are `sed`-shaped. Put the script in `tools/` and cite it in the commit message. The reviewer reviews the script once and spot-checks output, instead of reading every hunk. This is how LLVM and Chromium handle mass mechanical changes.
- **The real reviewer is the serialization fixtures.** The `.sk` files and the cross-language byte-identity tests already in place prove that nothing semantic moved. If those pass after a pure move, the move is correct — a human reading namespace lines adds little.

---
