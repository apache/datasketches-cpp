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

KLL_INCDIR := kll/include
KLL_BUILDDIR := build/kll

KLL_TSTDIR := kll/test
KLL_TSTBUILD := kll/build

KLL_TEST_BIN := kll_test
KLL_TARGET := kll/$(KLL_TEST_BIN)

KLL_TSTSOURCES := $(shell find $(KLL_TSTDIR) -type f -name "*.cpp")
KLL_TSTOBJS := $(patsubst $(KLL_TSTDIR)/%,$(KLL_TSTBUILD)/%,$(KLL_TSTSOURCES:.cpp=.o))

KLL_INCLIST := $(COM_INCLIST) -I $(KLL_INCDIR) -I common/test

$(KLL_TSTBUILD)/%.o: $(KLL_TSTDIR)/%.cpp
	@mkdir -p $(KLL_TSTBUILD)
	@echo "Compiling $<...";
	@$(CC) $(CPPFLAGS) $(INC) $(KLL_INCLIST) -c -o $@ $<

.PHONY: kll_test kll_clean
kll_exec: $(COM_TSTOBJS) $(KLL_TSTOBJS)
	@echo "Linking $(KLL_TARGET)..."
	@$(CC) $^ -o $(KLL_TARGET) $(TSTLNKFLAGS) $(LIB)

kll_test: kll_exec
	@cd kll; ./$(KLL_TEST_BIN)

kll_clean:
	@echo "Cleaning kll...";
	@$(RM) -r $(KLL_BUILDDIR) $(KLL_TSTBUILD) $(KLL_TARGET)
