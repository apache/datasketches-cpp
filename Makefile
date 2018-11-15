include config.mk

BUILDDIR := build
TARGETDIR := lib

LIBRARY := libdatasketches.$(LIB_SUFFIX)
TARGET := $(TARGETDIR)/$(LIBRARY)

INC := -I /usr/local/include
LIB := -L /usr/local/lib -lcppunit

MODULES := hll cpc kll

.PHONY: all
all: $(MODULES) $(LIBRARY)

include common/common.mk

INCLIST := $(COM_INCLIST)
OBJECTS := $(COM_OBJECTS)

# pull in configs for each of the specified modules
define MODULETASKS
include $(1)/$(1).mk
endef
$(foreach MODULE,$(MODULES),$(eval $(call MODULETASKS,$(MODULE))))

INCLIST += $(foreach mod, $(MODULES), $($(shell echo $(mod) | tr a-z A-Z)_INCLIST))
OBJECTS += $(foreach mod, $(MODULES), $($(shell echo $(mod) | tr a-z A-Z)_OBJECTS))

TEST_MODULES := common_test $(addsuffix _test, $(MODULES))
CLEAN_MODULES := common_clean $(addsuffix _clean, $(MODULES))
EXEC_MODULES := $(addsuffix _exec, $(MODULES))

$(LIBRARY): $(OBJECTS) 
	@mkdir -p $(TARGETDIR)
	@echo "Linking $(LIBRARY)"
	@$(CC) $^ -dynamiclib $(CPPFLAGS) -o $@
	@mv $(LIBRARY) $(TARGETDIR)

.PHONY: test clean

test: $(TEST_MODULES) $(EXEC_MODULES)

clean: $(CLEAN_MODULES)
	@echo "Cleaning $(TARGET)..."
	@$(RM) -r $(BUILDDIR) $(TARGET)

# TODO: also copy headers to /usr/local/include
#install:
#	@echo "Installing $(LIBRARY)..."; cp $(TARGET) $(INSTALLLIBDIR)

#distclean:
#	@echo "Removing $(LIBRARY)"; rm $(INSTALLLIBDIR)/$(LIBRARY)
