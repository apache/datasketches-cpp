HLL_INCDIR := hll/include
HLL_BUILDDIR := build/hll

HLL_TSTDIR := hll/test
HLL_TSTBUILD := hll/build

HLL_TEST_BIN := hll_test
HLL_TARGET := hll/$(HLL_TEST_BIN)

HLL_TSTSOURCES := $(shell find $(HLL_TSTDIR) -type f -name "*.cpp")
HLL_TSTOBJS := $(patsubst $(HLL_TSTDIR)/%,$(HLL_TSTBUILD)/%,$(HLL_TSTSOURCES:.cpp=.o))

HLL_INCLIST := $(COM_INCLIST) -I $(HLL_INCDIR)

$(HLL_TSTBUILD)/%.o: $(HLL_TSTDIR)/%.cpp
	@mkdir -p $(HLL_TSTBUILD)
	@echo "Compiling $<...";
	@$(CC) $(CPPFLAGS) $(INC) $(HLL_INCLIST) -c -o $@ $<

.PHONY: hll_test hll_clean
hll_exec: $(COM_TSTOBJS) $(HLL_TSTOBJS)
	@echo "Linking $(HLL_TARGET)..."
	@$(CC) $^ -o $(HLL_TARGET) $(TSTLNKFLAGS) $(LIB)

hll_test: hll_exec
	@cd hll; LD_LIBRARY_PATH=../$(TARGETDIR) ./$(HLL_TEST_BIN)

.PHONY: hll_clean
hll_clean:
	@echo "Cleaning hll...";
	@$(RM) -r $(HLL_BUILDDIR) $(HLL_TSTBUILD) $(HLL_TARGET)
