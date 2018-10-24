UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  CC := clang++ -arch x86_64
else
  CC := g++
endif

BUILDDIR := build
TARGETDIR := lib

LIBRARY := libdatasketches.dylib
TARGET := $(TARGETDIR)/$(LIBRARY)

INSTALLLIBDIR := /usr/local/lib

#OBJECTS := $(shell find $(BUILDDIR) -type f -name *.o)

CFLAGS := -c -g -Wall
INC := -I $(INCLIST) -I /usr/local/include

ifeq ($(UNAME_S),Linux)
    #CFLAGS += -std=gnu++11 -O2 # -fPIC

    # PostgreSQL Special
    #PG_VER := 9.3
    #INC += -I /usr/pgsql-$(PG_VER)/include
    #LIB += -L /usr/pgsql-$(PG_VER)/lib
else
  CFLAGS += -std=c++11 -stdlib=libc++ -O0 -g
endif

# need to explicitly add command to run tests, too
MODULES := cpc hll kll
TEST_MODULES := $(addsuffix _test, $(MODULES))
CLEAN_MODULES := $(addsuffix _clean, $(MODULES))

all: $(MODULES)

# build directory doesn't exist until after make all,
# so this is a separate command
library: $(TARGET)

# TODO: can we use $(MODULES) to build a list of *.o files?
$(TARGET): $(wildcard build/**/*) # doesn't complain if nothing matches
	@mkdir -p $(TARGETDIR)
	@echo "Linking..."
	@echo "  Linking $(TARGET)";
	@$(CC) $^ -dynamiclib -o $(TARGET) 

.PHONY: clean

common:
	@$(MAKE) -C common

common_test:
	@$(MAKE) -C common test

common_clean:
	@$(MAKE) -C common clean


define MODULETASKS
$(1): common
	@$(MAKE) -C $(1)

$(1)_test: $(1) common_test
	@$(MAKE) -C $(1) test

$(1)_clean:
	@$(MAKE) -C $(1) clean
endef

# expand the targets for each module
$(foreach MODULE,$(MODULES),$(eval $(call MODULETASKS,$(MODULE))))

test: $(TEST_MODULES)
	@echo "CPC tests:"
	@cd cpc; ./cpc_test
	@echo "HLL tests:"
	@cd hll; ./hll_test
	@echo "KLL tests:"
	@cd kll; ./kll_test


clean: common_clean $(CLEAN_MODULES)
	@echo "Cleaning $(TARGET)..."
	@$(RM) -r $(BUILDDIR) $(TARGET)

# TODO: also copy headers to /usr/local/include
#install:
#	@echo "Installing $(LIBRARY)..."; cp $(TARGET) $(INSTALLLIBDIR)

#distclean:
#	@echo "Removing $(LIBRARY)"; rm $(INSTALLLIBDIR)/$(LIBRARY)
