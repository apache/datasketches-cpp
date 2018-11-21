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

COMMON_FLAGS := -O0 -fpic -Wall -pedantic -g3

ifeq ($(UNAME_S),Linux)
    #CFLAGS += -std=gnu++11 -O2 # -fPIC

    # PostgreSQL Special
    #PG_VER := 9.3
    #INC += -I /usr/pgsql-$(PG_VER)/include
    #LIB += -L /usr/pgsql-$(PG_VER)/lib
else
  CFLAGS += -x c $(COMMON_FLAGS)
  CPPFLAGS += -std=c++17 -stdlib=libc++ $(COMMON_FLAGS)
  #CPPFLAGS += -std=c++17 $(COMMON_FLAGS)
  LINKFLAGS := -dynamiclib
  #LINKFLAGS := -shared
endif
