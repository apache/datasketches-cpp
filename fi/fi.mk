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

FI_INCDIR := fi/include
FI_BUILDDIR := build/fi

FI_TSTDIR := fi/test
FI_TSTBUILD := fi/build

FI_TEST_BIN := fi_test
FI_TARGET := fi/$(FI_TEST_BIN)

FI_TSTSOURCES := $(shell find $(FI_TSTDIR) -type f -name "*.cpp")
FI_TSTOBJS := $(patsubst $(FI_TSTDIR)/%,$(FI_TSTBUILD)/%,$(FI_TSTSOURCES:.cpp=.o))

FI_INCLIST := $(COM_INCLIST) -I $(FI_INCDIR) -I common/test

$(FI_TSTBUILD)/%.o: $(FI_TSTDIR)/%.cpp
	@mkdir -p $(FI_TSTBUILD)
	@echo "Compiling $<...";
	@$(CC) $(CPPFLAGS) $(INC) $(FI_INCLIST) -c -o $@ $<

.PHONY: fi_test fi_clean
fi_exec: $(COM_TSTOBJS) $(FI_TSTOBJS)
	@echo "Linking $(FI_TARGET)..."
	@$(CC) $^ -o $(FI_TARGET) $(TSTLNKFLAGS) $(LIB)

fi_test: fi_exec
	@cd fi; ./$(FI_TEST_BIN)

fi_clean:
	@echo "Cleaning fi...";
	@$(RM) -r $(FI_BUILDDIR) $(FI_TSTBUILD) $(FI_TARGET)
