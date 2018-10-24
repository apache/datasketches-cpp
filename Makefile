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

all: cpc

# build directory doesn't exist until after make all,
# so this is a separate command
library: $(TARGET)


$(TARGET): $(wildcard build/**/*) # doesn't complain if nothing matches
	@mkdir -p $(TARGETDIR)
	@echo "Linking..."
	@echo "  Linking $(TARGET)";
	@$(CC) $^ -dynamiclib -o $(TARGET) 

.PHONY: common cpc common_test cpc_test kll_test clean

common:
	@$(MAKE) -C common

cpc: common
	@$(MAKE) -C cpc


common_test:
	@$(MAKE) -C common test

cpc_test: cpc common_test
	@$(MAKE) -C cpc test

kll_test: common_test
	@$(MAKE) -C kll test


test: cpc_test kll_test
	@echo "CPC tests:"
	@cd cpc; ./cpc_test
	@echo "KLL tests:"
	@cd kll; ./kll_test


clean:
	@echo "Cleaning $(TARGET)..."; $(RM) -r $(BUILDDIR) $(TARGET)
	@$(MAKE) -C common clean
	@$(MAKE) -C cpc clean
	@$(MAKE) -C kll clean

# TODO: also copy headers to /usr/local/include
#install:
#	@echo "Installing $(LIBRARY)..."; cp $(TARGET) $(INSTALLLIBDIR)
 
#distclean:
#	@echo "Removing $(LIBRARY)"; rm $(INSTALLLIBDIR)/$(LIBRARY)
