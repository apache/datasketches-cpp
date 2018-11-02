include ../common.mk

MODULE := kll

SRCDIR := src
INCDIR := include
BUILDDIR := ../build/$(MODULE)

TSTDIR := test
TSTBUILD := build

COMMONTSTOBJ := ../common/build

TARGET := $(MODULE)_test

TSTSOURCES := $(shell find $(TSTDIR) -type f -name "*.cpp")
TSTOBJS := $(patsubst $(TSTDIR)/%,$(TSTBUILD)/%,$(TSTSOURCES:.cpp=.o))

INCLIST := $(INCDIR)
BUILDLIST := $(patsubst src/%,$(BUILDDIR)/%,$(INCDIRS))

INC := -I $(INCLIST) -I /usr/local/include
LIB := -L /usr/local/lib -lcppunit

.PHONY: all clean

all:
	@ # no-op

test: $(TARGET)

$(TSTBUILD)/%.o: $(TSTDIR)/%.cpp
	@mkdir -p $(TSTBUILD)
	@echo "Compiling $<...";
	@$(CC) $(CPPFLAGS) $(INC) -c -o $@ $<

$(TARGET): $(TSTOBJS)
	@echo "Linking $(TARGET)..."
	@$(CC) $^ -o $(TARGET) $(LIB) $(COMMONTSTOBJ)/*.o


clean:
	@echo "Cleaning $(MODULE)...";
	@$(RM) -r $(BUILDDIR) $(TSTBUILD) $(MODULE)_test *.bin
