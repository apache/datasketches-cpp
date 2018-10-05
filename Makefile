UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  CC := clang++ -arch x86_64
else
  CC := g++
endif

SRCDIR := src
TSTDIR := test
BUILDDIR := build
#TARGETDIR := bin
TARGETDIR := lib

#EXECUTABLE := ds
LIBRARY := libdatasketches.dylib
#TARGET := $(TARGETDIR)/$(EXECUTABLE)
TARGET := $(TARGETDIR)/$(LIBRARY)

INSTALLLIBDIR := /usr/local/lib

SRCEXT := cpp
SOURCES := $(shell find $(SRCDIR) -type f -name *.$(SRCEXT))
OBJECTS := $(patsubst $(SRCDIR)/%,$(BUILDDIR)/%,$(SOURCES:.$(SRCEXT)=.o))

#INCDIRS := $(shell find include/*/* -name '*.h' -exec dirname {} \; | sort | uniq)
INCDIRS := $(shell find $(SRCDIR) -type d)
INCLIST := $(patsubst $(SRCDIR)/%,-I $(SRCDIR)/%,$(INCDIRS))
BUILDLIST := $(patsubst src/%,$(BUILDDIR)/%,$(INCDIRS))

CFLAGS := -c -g -Wall
INC := -I $(INCLIST) -I /usr/local/include
#LIB := -L /usr/local/lib

ifeq ($(UNAME_S),Linux)
    #CFLAGS += -std=gnu++11 -O2 # -fPIC

    # PostgreSQL Special
    #PG_VER := 9.3
    #INC += -I /usr/pgsql-$(PG_VER)/include
    #LIB += -L /usr/pgsql-$(PG_VER)/lib
else
  CFLAGS += -std=c++11 -stdlib=libc++ -O0 -g
endif

	#@echo "  Linking $(TARGET)"; $(CC) $^ -o $(TARGET) $(LIB)
$(TARGET): $(OBJECTS)
	@mkdir -p $(TARGETDIR)
	@echo "Linking..."
	@echo "  Linking $(TARGET)"; $(CC) $^ -dynamiclib -o $(TARGET) 

$(BUILDDIR)/%.o: $(SRCDIR)/%.$(SRCEXT)
	@mkdir -p $(BUILDLIST)
	@echo "Compiling $<..."; $(CC) $(CFLAGS) $(INC) -c -o $@ $<

clean:
	@echo "Cleaning $(TARGET)..."; $(RM) -r $(BUILDDIR) $(TARGET)

# TODO: also copy headers to /usr/local/include
install:
	@echo "Installing $(LIBRARY)..."; cp $(TARGET) $(INSTALLLIBDIR)
  
distclean:
	@echo "Removing $(LIBRARY)"; rm $(INSTALLLIBDIR)/$(LIBRARY)

.PHONY: clean