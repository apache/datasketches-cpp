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

THETA_INCDIR := theta/include
THETA_BUILDDIR := build/theta

THETA_TSTDIR := theta/test
THETA_TSTBUILD := theta/build

THETA_TEST_BIN := theta_test
THETA_TARGET := theta/$(THETA_TEST_BIN)

THETA_TSTSOURCES := $(shell find $(THETA_TSTDIR) -type f -name "*.cpp")
THETA_TSTOBJS := $(patsubst $(THETA_TSTDIR)/%,$(THETA_TSTBUILD)/%,$(THETA_TSTSOURCES:.cpp=.o))

THETA_INCLIST := $(COM_INCLIST) -I $(THETA_INCDIR)

$(THETA_TSTBUILD)/%.o: $(THETA_TSTDIR)/%.cpp
	@mkdir -p $(THETA_TSTBUILD)
	@echo "Compiling $<...";
	@$(CC) $(CPPFLAGS) $(INC) $(THETA_INCLIST) -c -o $@ $<

.PHONY: theta_test theta_clean
theta_exec: $(COM_TSTOBJS) $(THETA_TSTOBJS)
	@echo "Linking $(THETA_TARGET)..."
	@$(CC) $^ -o $(THETA_TARGET) $(TSTLNKFLAGS) $(LIB)

theta_test: theta_exec
	@cd theta; LD_LIBRARY_PATH=../$(TARGETDIR) ./$(THETA_TEST_BIN)

.PHONY: theta_clean
theta_clean:
	@echo "Cleaning theta...";
	@$(RM) -r $(THETA_TSTBUILD) $(THETA_TARGET) $(THETA_TARGET)
