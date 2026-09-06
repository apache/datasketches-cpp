#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Runs the Apache Release Audit Tool over the tracked files of this repository
# and fails if any file lacks an approved license header.
#
# Only tracked files are examined: the tree is exported with `git archive`, so a
# local build directory or other untracked scratch cannot affect the result.
#
# Usage:  tools/rat-check.sh [git-ref]      (default: HEAD)

set -euo pipefail

RAT_VERSION="${RAT_VERSION:-0.16.1}"
RAT_JAR="${RAT_JAR:-}"
REF="${1:-HEAD}"

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

if [[ -z "$RAT_JAR" ]]; then
  RAT_JAR="$work/apache-rat.jar"
  url="https://repo1.maven.org/maven2/org/apache/rat/apache-rat/${RAT_VERSION}/apache-rat-${RAT_VERSION}.jar"
  echo "Downloading Apache RAT ${RAT_VERSION}"
  curl -fsSL -o "$RAT_JAR" "$url"
fi

mkdir -p "$work/src"
git archive "$REF" | tar -x -C "$work/src"
cp .rat-excludes "$work/src/.rat-excludes"

report="$work/rat-report.txt"
java -jar "$RAT_JAR" -E "$work/src/.rat-excludes" -d "$work/src" > "$report" 2>/dev/null

unknown="$(sed -n 's/^\([0-9][0-9]*\) Unknown Licenses$/\1/p' "$report" | head -1)"
unknown="${unknown:-0}"

if [[ "$unknown" -ne 0 ]]; then
  echo
  echo "Apache RAT found ${unknown} file(s) without an approved license header:"
  awk '/^Files with unapproved licenses:/{f=1;next} f&&/^\*\*\*\*/{exit} f&&NF{print "  " $0}' "$report" \
    | sed "s|$work/src/||"
  echo
  echo "Add the ASF header to each file. If a file is third-party source that must"
  echo "keep its own notice, add it to .rat-excludes AND record it in LICENSE."
  exit 1
fi

echo "Apache RAT: all tracked files carry an approved license header."
