UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  CC := clang++ -arch x86_64
else
  CC := g++
endif

MODULE := common

SRCDIR := src
INCDIR := include
BUILDDIR := ../build/$(MODULE)

TSTDIR := test
TSTBUILD := build

#CSOURCES := $(shell find $(SRCDIR) -type f -name "*.c")
CPPSOURCES := $(shell find $(SRCDIR) -type f -name "*.cpp")
SOURCES := $(CSOURCES) $(CPPSOURCES)
OBJECTS := $(patsubst $(SRCDIR)/%,$(BUILDDIR)/%,$(CSOURCES:.c=.o) $(CPPSOURCES:.cpp=.o))

TSTSOURCES := $(shell find $(TSTDIR) -type f -name "*.cpp")
TSTOBJS := $(patsubst $(TSTDIR)/%,$(TSTBUILD)/%,$(TSTSOURCES:.cpp=.o))

INCLIST := $(patsubst $(SRCDIR)/%,-I $(SRCDIR)/%,$(INCDIR))

COMMON_FLAGS := -g3 -O0 -Wall -pedantic
INC := -I $(INCLIST) -I /usr/local/include
LIB := -L /usr/local/lib -lcppunit

ifeq ($(UNAME_S),Linux)
    #CFLAGS += -std=gnu++11 -O2 # -fPIC

    # PostgreSQL Special
    #PG_VER := 9.3
    #INC += -I /usr/pgsql-$(PG_VER)/include
    #LIB += -L /usr/pgsql-$(PG_VER)/lib
else
  CFLAGS += -x c $(COMMON_FLAGS)
  CPPFLAGS += -std=c++11 -stdlib=libc++ $(COMMON_FLAGS)
endif

$(BUILDDIR)/%.o: $(SRCDIR)/%.cpp
	@mkdir -p $(BUILDDIR)
	@echo "Compiling $<...";
	@$(CC) $(CPPFLAGS) $(INC) -c -o $@ $<

all: $(OBJECTS)


$(TSTBUILD)/%.o: $(TSTDIR)/%.cpp
	@mkdir -p $(TSTBUILD)
	@echo "Compiling $<...";
	@$(CC) $(CPPFLAGS) $(INC) -c -o $@ $<

test: all $(TSTOBJS)


.PHONY: clean
clean:
	@echo "Cleaning $(MODULE)...";
	@$(RM) -r $(BUILDDIR) $(TSTBUILD)
