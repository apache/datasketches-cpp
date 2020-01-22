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

HLL_INCDIR := hll/include
HLL_BUILDDIR := build/hll

HLL_TSTDIR := hll/test
HLL_TSTBUILD := hll/build

HLL_TEST_BIN := hll_test
HLL_TARGET := hll/$(HLL_TEST_BIN)

HLL_TSTSOURCES := $(shell find $(HLL_TSTDIR) -type f -name "*.cpp")
HLL_TSTOBJS := $(patsubst $(HLL_TSTDIR)/%,$(HLL_TSTBUILD)/%,$(HLL_TSTSOURCES:.cpp=.o))

HLL_INCLIST := $(COM_INCLIST) -I $(HLL_INCDIR)

$(HLL_TSTBUILD)/%.o: $(HLL_TSTDIR)/%.cpp
	@mkdir -p $(HLL_TSTBUILD)
	@echo "Compiling $<...";
	@$(CC) $(CPPFLAGS) $(INC) $(HLL_INCLIST) -c -o $@ $<

.PHONY: hll_test hll_clean
hll_exec: $(COM_TSTOBJS) $(HLL_TSTOBJS)
	@echo "Linking $(HLL_TARGET)..."
	@$(CC) $^ -o $(HLL_TARGET) $(TSTLNKFLAGS) $(LIB)

hll_test: hll_exec
	@cd hll; ./$(HLL_TEST_BIN)

hll_clean:
	@echo "Cleaning hll...";
	@$(RM) -r $(HLL_BUILDDIR) $(HLL_TSTBUILD) $(HLL_TARGET)
