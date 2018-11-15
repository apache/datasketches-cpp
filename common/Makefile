include ../common.mk

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

INC := -I $(INCLIST) -I /usr/local/include
LIB := -L /usr/local/lib -lcppunit

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
