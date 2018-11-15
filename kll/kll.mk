KLL_INCDIR := kll/include
KLL_BUILDDIR := build/kll

KLL_TSTDIR := kll/test
KLL_TSTBUILD := kll/build

KLL_TEST_BIN := kll_test
KLL_TARGET := kll/$(KLL_TEST_BIN)

KLL_TSTSOURCES := $(shell find $(KLL_TSTDIR) -type f -name "*.cpp")
KLL_TSTOBJS := $(patsubst $(KLL_TSTDIR)/%,$(KLL_TSTBUILD)/%,$(KLL_TSTSOURCES:.cpp=.o))

KLL_INCLIST := -I $(KLL_INCDIR)

$(KLL_TSTBUILD)/%.o: $(KLL_TSTDIR)/%.cpp
	@mkdir -p $(KLL_TSTBUILD)
	@echo "Compiling $<...";
	@$(CC) $(CPPFLAGS) $(INC) $(KLL_INCLIST) -c -o $@ $<

.PHONY: kll_test kll_clean
kll_test: $(COM_TSTOBJS) $(KLL_TSTOBJS)
	@echo "Linking $(KLL_TARGET)..."
	@$(CC) $^ -o $(KLL_TARGET) $(LIB)

kll_exec: kll_test
	cd kll; ./$(KLL_TEST_BIN)

.PHONY: kll_clean
kll_clean:
	@echo "Cleaning kll...";
	@$(RM) -r $(KLL_TSTBUILD) $(KLL_TARGET) $(KLL_TARGET)
