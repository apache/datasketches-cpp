HLL_SRCDIR := hll/src
HLL_INCDIR := hll/include
HLL_BUILDDIR := build/hll

HLL_TSTDIR := hll/test
HLL_TSTBUILD := hll/build

HLL_TEST_BIN := hll_test
HLL_TARGET := hll/$(HLL_TEST_BIN)

HLL_CSOURCES := $(shell find $(HLL_SRCDIR) -type f -name "*.c")
HLL_CPPSOURCES := $(shell find $(HLL_SRCDIR) -type f -name "*.cpp")
HLL_SOURCES := $(HLL_CSOURCES) $(HLL_CPPSOURCES)
HLL_OBJECTS := $(patsubst $(HLL_SRCDIR)/%,$(HLL_BUILDDIR)/%,$(HLL_CPPSOURCES:.cpp=.o))

HLL_TSTSOURCES := $(shell find $(HLL_TSTDIR) -type f -name "*.cpp")
HLL_TSTOBJS := $(patsubst $(HLL_TSTDIR)/%,$(HLL_TSTBUILD)/%,$(HLL_TSTSOURCES:.cpp=.o))

HLL_INCLIST := $(COM_INCLIST) -I $(HLL_INCDIR)
HLL_BUILDLIST := $(patsubst src/%,$(HLL_BUILDDIR)/%,$(HLL_INCDIR))

hll: $(HLL_OBJECTS)

$(HLL_BUILDDIR)/%.o: $(HLL_SRCDIR)/%.cpp
	@mkdir -p $(HLL_BUILDDIR)
	@echo "Compiling $<...";
	@$(CC) $(CPPFLAGS) $(INC) $(HLL_INCLIST) -c -o $@ $<

$(HLL_TSTBUILD)/%.o: $(HLL_TSTDIR)/%.cpp
	@mkdir -p $(HLL_TSTBUILD)
	@echo "Compiling $<...";
	@$(CC) $(CPPFLAGS) $(INC) $(HLL_INCLIST) -c -o $@ $<

.PHONY: hll_test hll_clean
hll_test: $(COM_TSTOBJS) $(HLL_TSTOBJS)
	@echo "Linking $(HLL_TARGET)..."
	@$(CC) $^ -o $(HLL_TARGET) $(LIB)

hll_exec: hll_test
	cd hll; DYLD_LIBRARY_PATH=../$(TARGETDIR) ./$(HLL_TEST_BIN)

hll_clean:
	@echo "Cleaning hll...";
	@$(RM) -r $(HLL_BUILDDIR) $(HLL_TSTBUILD) $(HLL_TARGET)
