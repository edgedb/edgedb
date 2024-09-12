EXT_NAME := $(shell python3 -c 'import tomllib; f = tomllib.load(open("MANIFEST.toml", "rb")); print(f["name"])')
EXT_VERSION := $(shell python3 -c 'import tomllib; f = tomllib.load(open("MANIFEST.toml", "rb")); print(f["version"])')
EDGEQL_SRCS := $(shell python3 -c 'import tomllib; f = tomllib.load(open("MANIFEST.toml", "rb")); print(" ".join(f["files"]))')
SQL_DIR := $(shell python3 -c 'import tomllib; f = tomllib.load(open("MANIFEST.toml", "rb")); print(f["postgres_files"])')

#

EXT_FNAME := $(EXT_NAME)--$(EXT_VERSION)

PG_CONFIG := $(shell edb config --pg-config)
PG_DIR := $(shell dirname $(shell dirname $(PG_CONFIG)))

rwildcard=$(foreach d,$(wildcard $(1:=/*)),$(call rwildcard,$d,$2) $(filter $(subst *,%,$2),$d))
SQL_DEPS := $(call rwildcard,$(SQL_MODULE),*.c *.h *.sql *.control Makefile)

SQL_STAMP := build/out/.sql_stamp

ifneq ($(strip $(CUSTOM_SQL_BUILD)),1)

$(SQL_STAMP): MANIFEST.toml $(SQL_DEPS) $(EXTRA_DEPS) Makefile
	$(MAKE) -C $(SQL_MODULE) DESTDIR=$(PWD)/build/out PG_CONFIG=$(PG_DIR)/bin/pg_config install
	touch $(SQL_STAMP)
endif


$(EXT_FNAME).zip: MANIFEST.toml $(EDGEQL_SRCS) $(EXTRA_FILES) $(SQL_STAMP) Makefile
	rm -rf build/$(EXT_FNAME)
	mkdir build/$(EXT_FNAME)

	rm -rf build/$(SQL_DIR)
	cp -r $(PWD)/build/out/$(PG_DIR) build/$(SQL_DIR)
	cp -r build/$(SQL_DIR) build/$(EXT_FNAME)/$(SQL_DIR)
	cp $(EDGEQL_SRCS) build/$(EXT_FNAME)
	if [ -n "$(EXTRA_FILES)" ]; then cp $(EXTRA_FILES) build/$(EXT_FNAME); fi
	cp MANIFEST.toml build/$(EXT_FNAME)

	rm -f $(EXT_FNAME).zip
	cd build/$(EXT_FNAME)/ && zip -r ../../$(EXT_FNAME).zip *

clean:
	rm -rf build $(EXT_FNAME).zip

.DEFAULT_GOAL := $(EXT_FNAME).zip
