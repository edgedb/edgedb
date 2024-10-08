PYTHON ?= python3
EXT_NAME := $(shell $(PYTHON) -c 'import tomllib; f = tomllib.load(open("MANIFEST.toml", "rb")); print(f["name"])')
EXT_VERSION := $(shell $(PYTHON) -c 'import tomllib; f = tomllib.load(open("MANIFEST.toml", "rb")); print(f["version"])')
EDGEQL_SRCS := $(shell $(PYTHON) -c 'import tomllib; f = tomllib.load(open("MANIFEST.toml", "rb")); print(" ".join(f["files"]))')
SQL_DIR := $(shell $(PYTHON) -c 'import tomllib; f = tomllib.load(open("MANIFEST.toml", "rb")); print(f["postgres_files"])')
EXT_FNAME := $(EXT_NAME)--$(EXT_VERSION)

.DEFAULT_GOAL := $(EXT_FNAME).zip
.PHONY: clean install

#

WITH_SQL ?= yes
WITH_EDGEQL ?= yes

rwildcard=$(foreach d,$(wildcard $(1:=/*)),$(call rwildcard,$d,$2) $(filter $(subst *,%,$2),$d))
SQL_DEPS := $(call rwildcard,$(SQL_MODULE),*.c *.h *.sql *.control Makefile)

SQL_BUILD_STAMP := build/.sql_build_stamp
SQL_INSTALL_STAMP := build/.sql_install_stamp
EDGEQL_INSTALL_STAMP := build/.edgeql_install_stamp

ifeq ($(strip $(WITH_SQL)),yes)

EDB := $(PYTHON) -m edb.tools $(EDBFLAGS)
PG_CONFIG := $(shell $(EDB) config --pg-config)

ifneq ($(strip $(CUSTOM_SQL_BUILD)),1)

$(SQL_BUILD_STAMP): MANIFEST.toml $(SQL_DEPS) $(EXTRA_DEPS) Makefile
	$(MAKE) -C $(SQL_MODULE) DESTDIR=$(PWD)/build/out PG_CONFIG=$(PG_CONFIG) install
	touch $(SQL_BUILD_STAMP)
endif

$(SQL_INSTALL_STAMP): $(SQL_BUILD_STAMP)
	mkdir -p build/$(SQL_DIR)
	cp -r $(PWD)/build/out/$(shell $(PG_CONFIG) --pkglibdir) build/$(SQL_DIR)/lib
	cp -r $(PWD)/build/out/$(shell $(PG_CONFIG) --sharedir) build/$(SQL_DIR)/share
	cp -r build/$(SQL_DIR) build/$(EXT_FNAME)/$(SQL_DIR)
	touch $(SQL_INSTALL_STAMP)
else
$(SQL_INSTALL_STAMP):
	mkdir -p build
	touch $(SQL_INSTALL_STAMP)
endif
ifeq ($(strip $(WITH_EDGEQL)),yes)
$(EDGEQL_INSTALL_STAMP): MANIFEST.toml $(EDGEQL_SRCS) $(EXTRA_FILES) Makefile
	mkdir -p build/$(EXT_FNAME)
	cp $(EDGEQL_SRCS) build/$(EXT_FNAME)
	if [ -n "$(EXTRA_FILES)" ]; then cp $(EXTRA_FILES) build/$(EXT_FNAME); fi
	cp MANIFEST.toml build/$(EXT_FNAME)
	touch $(EDGEQL_INSTALL_STAMP)
else
EDGEQL:
	mkdir -p build
	touch $(EDGEQL_INSTALL_STAMP)
endif

$(EXT_FNAME).zip: $(EDGEQL_INSTALL_STAMP) $(SQL_INSTALL_STAMP)
	rm -f $(EXT_FNAME).zip
	cd build/$(EXT_FNAME)/ && zip -r ../../$(EXT_FNAME).zip *

install: $(EXT_FNAME).zip
	if [ -z "$(DESTDIR)" ]; then echo "DESTDIR must be set" >&2; exit 1; fi
	mkdir -p "$(DESTDIR)"
	cp $(EXT_FNAME).zip "$(DESTDIR)/"

clean:
	rm -rf build $(EXT_FNAME).zip
