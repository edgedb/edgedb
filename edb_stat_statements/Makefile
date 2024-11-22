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

TAP_TESTS = 1
PG_MAJOR = $(shell $(PG_CONFIG) --version | grep -oE '[0-9]+' | head -1)

ifeq ($(shell test $(PG_MAJOR) -ge 18 && echo true), true)
	REGRESS += extended
endif

all:

expected/dml.out:
	if [ $(PG_MAJOR) -ge 18 ]; then \
		cp expected/dml.out.18 expected/dml.out; \
	else \
		cp expected/dml.out.17 expected/dml.out; \
	fi

expected/level_tracking.out:
	if [ $(PG_MAJOR) -ge 18 ]; then \
		cp expected/level_tracking.out.18 expected/level_tracking.out; \
	else \
		cp expected/level_tracking.out.17 expected/level_tracking.out; \
	fi

expected/parallel.out:
	if [ $(PG_MAJOR) -ge 18 ]; then \
		cp expected/parallel.out.18 expected/parallel.out; \
	else \
		cp expected/parallel.out.17 expected/parallel.out; \
	fi

expected/utility.out:
	if [ $(PG_MAJOR) -ge 17 ]; then \
		cp expected/utility.out.17 expected/utility.out; \
	else \
		cp expected/utility.out.16 expected/utility.out; \
	fi

expected/wal.out:
	if [ $(PG_MAJOR) -ge 18 ]; then \
		cp expected/wal.out.18 expected/wal.out; \
	else \
		cp expected/wal.out.17 expected/wal.out; \
	fi

installcheck: \
	expected/dml.out \
	expected/level_tracking.out \
	expected/parallel.out \
	expected/utility.out \
	expected/wal.out

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
