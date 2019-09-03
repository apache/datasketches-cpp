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

include config.mk

TEST_BINARY_INPUT_PATH = test

BUILDDIR := build
TARGETDIR := lib

LIB_BASE_NAME := datasketches
LIBRARY := lib$(LIB_BASE_NAME).$(LIB_SUFFIX)
TARGET := $(TARGETDIR)/$(LIBRARY)

INC := -I /usr/local/include
LIB := -L /usr/local/lib -lcppunit -L lib -l$(LIB_BASE_NAME)

MODULES := cpc kll fi theta hll

.PHONY: all
all: $(MODULES) $(LIBRARY)

include common/common.mk

INCLIST := $(COM_INCLIST)
OBJECTS := $(COM_OBJECTS)

# pull in configs for each of the specified modules
define MODULETASKS
include $(1)/$(1).mk
endef
$(foreach MODULE,$(MODULES),$(eval $(call MODULETASKS,$(MODULE))))

INCLIST += $(foreach mod, $(MODULES), $($(shell echo $(mod) | tr a-z A-Z)_INCLIST))
OBJECTS += $(foreach mod, $(MODULES), $($(shell echo $(mod) | tr a-z A-Z)_OBJECTS))

TEST_MODULES := common_test $(addsuffix _test, $(MODULES))
CLEAN_MODULES := common_clean $(addsuffix _clean, $(MODULES))
EXEC_MODULES := $(addsuffix _exec, $(MODULES))

$(LIBRARY): $(OBJECTS) 
	@mkdir -p $(TARGETDIR)
	@echo "Linking $(LIBRARY)"
	@$(CC) $^ $(LINKFLAGS) $(CPPFLAGS) -o $@
	@mv $(LIBRARY) $(TARGETDIR)

.PHONY: test clean

test: $(LIBRARY) $(TEST_MODULES) $(EXEC_MODULES)

clean: $(CLEAN_MODULES)
	@echo "Cleaning $(TARGET)..."
	@$(RM) -r $(BUILDDIR) $(TARGET) coverage.info

ifeq ($(COVERAGE),1)
coverage: test
	lcov --o coverage.info -c -d . --no-external
	genhtml --legend coverage.info --output-directory coverage_report
endif

# TODO: also copy headers to /usr/local/include
#install:
#	@echo "Installing $(LIBRARY)..."; cp $(TARGET) $(INSTALLLIBDIR)

#distclean:
#	@echo "Removing $(LIBRARY)"; rm $(INSTALLLIBDIR)/$(LIBRARY)
