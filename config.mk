# Common flags and other useful variables

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  CC := clang++ -arch x86_64
  #CC := g++-8
  LIB_SUFFIX := dylib
else
  CC := g++
  LIB_SUFFIX := so
endif

COMMON_FLAGS := -O3 -fpic -Wall -pedantic -g0

ifeq ($(UNAME_S),Linux)
    CFLAGS += -std=gnu++11 -fPIC
    CPPFLAGS += -std=gnu++11 -fPIC
    LINKFLAGS := -shared

    # PostgreSQL Special
    #PG_VER := 9.3
    #INC += -I /usr/pgsql-$(PG_VER)/include
    #LIB += -L /usr/pgsql-$(PG_VER)/lib
else
  CFLAGS += -x c $(COMMON_FLAGS)
  CPPFLAGS += -std=c++11 $(COMMON_FLAGS)

  ifeq (clang,$(findstring clang,$(CC)))
    LINKFLAGS := -dynamiclib
    CPPFLAGS += -stdlib=libc++
  else
    LINKFLAGS := -shared
  endif
endif

TSTLNKFLAGS := -Wl,-rpath=/usr/local/lib

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