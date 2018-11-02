include ../common.mk

MODULE := cpc

SRCDIR := src
INCDIR := include
BUILDDIR := ../build/$(MODULE)

TSTDIR := test
TSTBUILD := build

COMMONOBJ := ../build/common
COMMONINC := ../common/include
COMMONTSTOBJ := ../common/build

TARGET := $(MODULE)_test

CSOURCES := $(shell find $(SRCDIR) -type f -name "*.c")
CPPSOURCES := $(shell find $(SRCDIR) -type f -name "*.cpp")
SOURCES := $(CSOURCES) $(CPPSOURCES)
OBJECTS := $(patsubst $(SRCDIR)/%,$(BUILDDIR)/%,$(CSOURCES:.c=.o) $(CPPSOURCES:.cpp=.o))

TSTSOURCES := $(shell find $(TSTDIR) -type f -name "*.cpp")
TSTOBJS := $(patsubst $(TSTDIR)/%,$(TSTBUILD)/%,$(TSTSOURCES:.cpp=.o))

INCDIRS = $(INCDIR)
INCLIST := $(patsubst $(SRCDIR)/%,-I $(SRCDIR)/%,$(INCDIR))
BUILDLIST := $(patsubst src/%,$(BUILDDIR)/%,$(INCDIRS))

INC := -I $(INCLIST) -I $(COMMONINC) -I /usr/local/include
LIB := -L /usr/local/lib -lcppunit


$(BUILDDIR)/%.o: $(SRCDIR)/%.c
	@mkdir -p $(BUILDDIR)
	@echo "Compiling $<...";
	@$(CC) $(CFLAGS) $(INC) -c -o $@ $<

$(BUILDDIR)/%.o: $(SRCDIR)/%.cpp
	@mkdir -p $(BUILDDIR)
	@echo "Compiling $<...";
	@$(CC) $(CPPFLAGS) $(INC) -c -o $@ $<

all: $(OBJECTS)

test: $(TARGET)

$(TSTBUILD)/%.o: $(TSTDIR)/%.cpp
	@mkdir -p $(TSTBUILD)
	@echo "Compiling $<...";
	@$(CC) $(CPPFLAGS) $(INC) -c -o $@ $<

$(TARGET): $(OBJECTS) $(TSTOBJS)
	@echo "Linking $(TARGET)..."
	@$(CC) $^ -o $(TARGET) $(LIB) $(COMMONOBJ)/*.o $(COMMONTSTOBJ)/*.o


.PHONY: clean
clean:
	@echo "Cleaning $(MODULE)...";
	@$(RM) -r $(BUILDDIR) $(TSTBUILD) $(MODULE)_test *.bin
