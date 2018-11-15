#COM_SRCDIR := common/src
COM_INCDIR := common/include
COM_BUILDDIR := build/common

COM_TSTDIR := common/test
COM_TSTBUILD := common/build

#COM_CPPSOURCES := $(shell find $(COM_SRCDIR) -type f -name "*.cpp")
#COM_SOURCES := $(COM_CPPSOURCES)
#COM_OBJECTS := $(patsubst $(COM_SRCDIR)/%,$(COM_BUILDDIR)/%,$(COM_CPPSOURCES:.cpp=.o))

COM_TSTSOURCES := $(shell find $(COM_TSTDIR) -type f -name "*.cpp")
COM_TSTOBJS := $(patsubst $(COM_TSTDIR)/%,$(COM_TSTBUILD)/%,$(COM_TSTSOURCES:.cpp=.o))

#COM_INCLIST := $(patsubst $(COM_SRCDIR)/%,-I $(COM_SRCDIR)/%,$(COM_INCDIR))
COM_INCLIST := -I $(COM_INCDIR)

$(COM_TSTBUILD)/%.o: $(COM_TSTDIR)/%.cpp
	@mkdir -p $(COM_TSTBUILD)
	@echo "Compiling $<...";
	@$(CC) $(CPPFLAGS) $(INC) $(COM_INCLIST) -c -o $@ $<

.PHONY: common_test
common_test: $(COM_TSTOBJS)

.PHONY: common_clean
common_clean:
	@echo "Cleaning common...";
	@$(RM) -r $(COM_TSTBUILD)
