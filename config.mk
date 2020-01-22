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

# Common flags and other useful variables

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  CC := clang++ -arch x86_64
else
  CC := g++
endif

COMMON_FLAGS := -O3 -fpic -Wall -pedantic -g0

ifeq ($(UNAME_S),Linux)
    CFLAGS += -std=gnu++11 -fPIC
    CPPFLAGS += -std=gnu++11 -fPIC
else
  CFLAGS += -x c $(COMMON_FLAGS)
  CPPFLAGS += -std=c++11 $(COMMON_FLAGS)

  ifeq (clang,$(findstring clang,$(CC)))
    CPPFLAGS += -stdlib=libc++
  endif
endif

#TSTLNKFLAGS := -Wl,-rpath=/usr/local/lib

ifeq ($(COVERAGE),1)
  #ifeq (clang,$(findstring clang,$(CC)))
    CFLAGS += --coverage
    CPPFLAGS += --coverage
    LINKFLAGS += --coverage
    TSTLNKFLAGS += --coverage
  #else
  #  CFLAGS += --coverage -ftest-covearge -fprofile-arcs
  #  CPPFLAGS += --coverage -ftest-covearge -fprofile-arcs
  #endif
endif