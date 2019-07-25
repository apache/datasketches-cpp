KLL_INCDIR := kll/include
KLL_BUILDDIR := build/kll

KLL_TSTDIR := kll/test
KLL_TSTBUILD := kll/build

KLL_TEST_BIN := kll_test
KLL_TARGET := kll/$(KLL_TEST_BIN)

KLL_TSTSOURCES := $(shell find $(KLL_TSTDIR) -type f -name "*.cpp")
KLL_TSTOBJS := $(patsubst $(KLL_TSTDIR)/%,$(KLL_TSTBUILD)/%,$(KLL_TSTSOURCES:.cpp=.o))

KLL_INCLIST := $(COM_INCLIST) -I $(KLL_INCDIR) -I common/test

$(KLL_TSTBUILD)/%.o: $(KLL_TSTDIR)/%.cpp
	@mkdir -p $(KLL_TSTBUILD)
	@echo "Compiling $<...";
	@$(CC) $(CPPFLAGS) $(INC) $(KLL_INCLIST) -c -o $@ $<

.PHONY: kll_test kll_clean
kll_exec: $(COM_TSTOBJS) $(KLL_TSTOBJS)
	@echo "Linking $(KLL_TARGET)..."
	@$(CC) $^ -o $(KLL_TARGET) $(TSTLNKFLAGS) $(LIB)

kll_test: kll_exec
	@cd kll; LD_LIBRARY_PATH=../$(TARGETDIR) ./$(KLL_TEST_BIN)

.PHONY: kll_clean
kll_clean:
	@echo "Cleaning kll...";
	@$(RM) -r $(KLL_TSTBUILD) $(KLL_TARGET) $(KLL_TARGET)
