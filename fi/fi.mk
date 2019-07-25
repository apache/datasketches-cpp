FI_INCDIR := fi/include
FI_BUILDDIR := build/fi

FI_TSTDIR := fi/test
FI_TSTBUILD := fi/build

FI_TEST_BIN := fi_test
FI_TARGET := fi/$(FI_TEST_BIN)

FI_TSTSOURCES := $(shell find $(FI_TSTDIR) -type f -name "*.cpp")
FI_TSTOBJS := $(patsubst $(FI_TSTDIR)/%,$(FI_TSTBUILD)/%,$(FI_TSTSOURCES:.cpp=.o))

FI_INCLIST := $(COM_INCLIST) -I $(FI_INCDIR) -I common/test

$(FI_TSTBUILD)/%.o: $(FI_TSTDIR)/%.cpp
	@mkdir -p $(FI_TSTBUILD)
	@echo "Compiling $<...";
	@$(CC) $(CPPFLAGS) $(INC) $(FI_INCLIST) -c -o $@ $<

.PHONY: fi_test fi_clean
fi_exec: $(COM_TSTOBJS) $(FI_TSTOBJS)
	@echo "Linking $(FI_TARGET)..."
	@$(CC) $^ -o $(FI_TARGET) $(TSTLNKFLAGS) $(LIB)

fi_test: fi_exec
	@cd fi; LD_LIBRARY_PATH=../$(TARGETDIR) ./$(FI_TEST_BIN)

.PHONY: fi_clean
fi_clean:
	@echo "Cleaning fi...";
	@$(RM) -r $(FI_TSTBUILD) $(FI_TARGET) $(FI_TARGET)
