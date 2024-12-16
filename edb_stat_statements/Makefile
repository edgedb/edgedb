MODULE_big = edb_stat_statements
OBJS = \
	$(WIN32RES) \
	edb_stat_statements.o

EXTENSION = edb_stat_statements
DATA = edb_stat_statements--1.0.sql
PGFILEDESC = "edb_stat_statements - execution statistics of EdgeDB queries"

LDFLAGS_SL += $(filter -lm, $(LIBS))

REGRESS = select dml cursors utility level_tracking planning \
	user_activity wal entry_timestamp privileges \
	parallel cleanup oldextversions

PG_CONFIG ?= pg_config

TAP_TESTS = 1
PG_MAJOR = $(shell $(PG_CONFIG) --version | grep -oE '[0-9]+' | head -1)
PG_MAJOR_SPLIT_17 = $(shell test $(PG_MAJOR) -ge 17 && echo 17 || echo 16)
PG_MAJOR_SPLIT_18 = $(shell test $(PG_MAJOR) -ge 18 && echo 18 || echo 17)

ifeq ($(PG_MAJOR_SPLIT_18), 18)
	REGRESS += extended
endif

all:

expected/dml.out: expected/dml.out.$(PG_MAJOR_SPLIT_18)
	cp $< $@

expected/level_tracking.out: expected/level_tracking.out.$(PG_MAJOR_SPLIT_18)
	cp $< $@

expected/parallel.out: expected/parallel.out.$(PG_MAJOR_SPLIT_18)
	cp $< $@

expected/utility.out: expected/utility.out.$(PG_MAJOR_SPLIT_17)
	cp $< $@

expected/wal.out: expected/wal.out.$(PG_MAJOR_SPLIT_18)
	cp $< $@

installcheck: \
	expected/dml.out \
	expected/level_tracking.out \
	expected/parallel.out \
	expected/utility.out \
	expected/wal.out

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
