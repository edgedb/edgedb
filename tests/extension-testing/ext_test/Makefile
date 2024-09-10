# Configurable parts
SQL_MODULE := sql

# ==== All the rest of this is default stuff

EXT_NAME := $(shell python3 -c 'import tomllib; f = tomllib.load(open("MANIFEST.toml", "rb")); print(f["name"])')
EXT_VERSION := $(shell python3 -c 'import tomllib; f = tomllib.load(open("MANIFEST.toml", "rb")); print(f["version"])')
EDGEQL_SRCS := $(shell python3 -c 'import tomllib; f = tomllib.load(open("MANIFEST.toml", "rb")); print(" ".join(f["files"]))')

#

EXT_FNAME := $(EXT_NAME)--$(EXT_VERSION)

ROOT_DIR := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
EDGEDB_DIR:= $(shell dirname $(shell dirname $(shell dirname $(ROOT_DIR))))
PG_DIR = $(EDGEDB_DIR)/build/postgres/install/

rwildcard=$(foreach d,$(wildcard $(1:=/*)),$(call rwildcard,$d,$2) $(filter $(subst *,%,$2),$d))
SQL_DEPS := $(call rwildcard,$(SQL_MODULE),*.c *.h *.sql *.control Makefile)

$(EXT_FNAME).zip: MANIFEST.toml $(EDGEQL_SRCS) $(SQL_DEPS) Makefile
	echo $(EDGEDB_DIR)
	echo $(PG_DIR)
	make -C sql DESTDIR=$(PWD)/build/out PG_CONFIG=$(PG_DIR)/bin/pg_config install

	rm -rf build/$(EXT_FNAME)
	mkdir build/$(EXT_FNAME)

	cp -r $(PWD)/build/out/$(PG_DIR) build/$(EXT_FNAME)/install
	cp $(EDGEQL_SRCS) build/$(EXT_FNAME)
	cp MANIFEST.toml build/$(EXT_FNAME)

	rm -f $(EXT_FNAME).zip
	cd build/$(EXT_FNAME)/ && zip -r ../../$(EXT_FNAME).zip *

clean:
	rm -rf build $(EXT_FNAME).zip
