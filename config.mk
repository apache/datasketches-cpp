# Common flags and other useful variables

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  CC := clang++ -arch x86_64
  LIB_SUFFIX := dylib
else
  CC := g++
  LIB_SUFFIX := so
endif

COMMON_FLAGS := -O3 -fpic -Wall -pedantic -flto -fpic

ifeq ($(UNAME_S),Linux)
    #CFLAGS += -std=gnu++11 -O2 # -fPIC

    # PostgreSQL Special
    #PG_VER := 9.3
    #INC += -I /usr/pgsql-$(PG_VER)/include
    #LIB += -L /usr/pgsql-$(PG_VER)/lib
else
  CFLAGS += -x c $(COMMON_FLAGS)
  CPPFLAGS += -std=c++17 -stdlib=libc++ $(COMMON_FLAGS) -fwhole-program-vtables
endif
