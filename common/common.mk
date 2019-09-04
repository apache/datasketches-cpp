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

#COM_SRCDIR := common/src
COM_INCDIR := common/include
COM_BUILDDIR := build/common

COM_TSTDIR := common/test
COM_TSTBUILD := common/build

#COM_CPPSOURCES := $(shell find $(COM_SRCDIR) -type f -name "*.cpp")
#COM_SOURCES := $(COM_CPPSOURCES)
#COM_OBJECTS := $(patsubst $(COM_SRCDIR)/%,$(COM_BUILDDIR)/%,$(COM_CPPSOURCES:.cpp=.o))

COM_TSTSOURCES := $(shell find $(COM_TSTDIR) -type f -name "*.cpp")
COM_TSTOBJS := $(patsubst $(COM_TSTDIR)/%,$(COM_TSTBUILD)/%,$(COM_TSTSOURCES:.cpp=.o))

#COM_INCLIST := $(patsubst $(COM_SRCDIR)/%,-I $(COM_SRCDIR)/%,$(COM_INCDIR))
COM_INCLIST := -I $(COM_INCDIR)

$(COM_TSTBUILD)/%.o: $(COM_TSTDIR)/%.cpp
	@mkdir -p $(COM_TSTBUILD)
	@echo "Compiling $<...";
	@$(CC) $(CPPFLAGS) $(INC) $(COM_INCLIST) -c -o $@ $<

.PHONY: common_test
common_test: $(COM_TSTOBJS)

.PHONY: common_clean
common_clean:
	@echo "Cleaning common...";
	@$(RM) -r $(COM_TSTBUILD)
