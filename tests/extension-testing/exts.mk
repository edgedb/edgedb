PYTHON ?= python3
EXT_NAME := $(shell $(PYTHON) -c 'import tomllib; f = tomllib.load(open("MANIFEST.toml", "rb")); print(f["name"])')
EXT_VERSION := $(shell $(PYTHON) -c 'import tomllib; f = tomllib.load(open("MANIFEST.toml", "rb")); print(f["version"])')
EDGEQL_SRCS := $(shell $(PYTHON) -c 'import tomllib; f = tomllib.load(open("MANIFEST.toml", "rb")); print(" ".join(f["files"]))')
EXT_FNAME := $(EXT_NAME)--$(EXT_VERSION)

.DEFAULT_GOAL := build
.PHONY: build clean install zip

#

WITH_SQL ?= yes
WITH_EDGEQL ?= yes

rwildcard=$(foreach d,$(wildcard $(1:=/*)),$(call rwildcard,$d,$2) $(filter $(subst *,%,$2),$d))
SQL_DEPS := $(call rwildcard,$(SQL_MODULE),*.c *.h *.sql *.control Makefile)

SQL_BUILD_STAMP := build/.sql_build_stamp
SQL_INSTALL_STAMP := build/.sql_install_stamp
EDGEQL_INSTALL_STAMP := build/.edgeql_install_stamp
INSTALLABLES := MANIFEST.toml

ifeq ($(strip $(WITH_SQL)),yes)

INSTALLABLES += lib/ share/

ifeq ($(origin PG_CONFIG), undefined)
EDB := $(PYTHON) -m edb.tools $(EDBFLAGS)
PG_CONFIG := $(shell $(EDB) config --pg-config)

ifeq ($(PG_CONFIG),)
$(error cannot find pg_config, please set PG_CONFIG explicitly)
endif
endif

ifneq ($(strip $(CUSTOM_SQL_BUILD)),1)

$(SQL_BUILD_STAMP): MANIFEST.toml $(SQL_DEPS) $(EXTRA_DEPS) Makefile
	env PG_CONFIG=$(PG_CONFIG) $(MAKE) -C $(SQL_MODULE) DESTDIR=$(PWD)/build/out PG_CONFIG=$(PG_CONFIG) install
	touch $(SQL_BUILD_STAMP)
endif

$(SQL_INSTALL_STAMP): $(SQL_BUILD_STAMP)
	rm -rf build/$(EXT_FNAME)/lib
	mkdir -p build/$(EXT_FNAME)/lib
	rm -rf build/$(EXT_FNAME)/share/postgresql
	mkdir -p build/$(EXT_FNAME)/share/postgresql
	mkdir -p $(PWD)/build/out/$(shell $(PG_CONFIG) --pkglibdir)
	cp -r $(PWD)/build/out/$(shell $(PG_CONFIG) --pkglibdir) build/$(EXT_FNAME)/lib/postgresql
	mkdir -p $(PWD)/build/out/$(shell $(PG_CONFIG) --sharedir)/contrib
	mkdir -p $(PWD)/build/out/$(shell $(PG_CONFIG) --sharedir)/extension
	cp -r $(PWD)/build/out/$(shell $(PG_CONFIG) --sharedir)/contrib build/$(EXT_FNAME)/share/postgresql/contrib
	cp -r $(PWD)/build/out/$(shell $(PG_CONFIG) --sharedir)/extension build/$(EXT_FNAME)/share/postgresql/extension
	touch $(SQL_INSTALL_STAMP)
else
$(SQL_INSTALL_STAMP):
	mkdir -p build
	touch $(SQL_INSTALL_STAMP)
endif
ifeq ($(strip $(WITH_EDGEQL)),yes)
INSTALLABLES += $(EDGEQL_SRCS) $(EXTRA_FILES)

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

build: $(EDGEQL_INSTALL_STAMP) $(SQL_INSTALL_STAMP)

install: build
	if [ -z "$(DESTDIR)" ]; then echo "DESTDIR must be set" >&2; exit 1; fi
	mkdir -p "$(DESTDIR)"
	cd "build/$(EXT_FNAME)" && cp -r $(INSTALLABLES) "${DESTDIR}/"

$(EXT_FNAME).zip: build
	rm -f $(EXT_FNAME).zip
	cd build/ && zip -r ../$(EXT_FNAME).zip $(EXT_FNAME)/

zip: $(EXT_FNAME).zip

clean:
	rm -rf build $(EXT_FNAME).zip
