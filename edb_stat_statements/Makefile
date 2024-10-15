MODULE_big = edb_stat_statements
OBJS = \
	$(WIN32RES) \
	edb_stat_statements.o

EXTENSION = edb_stat_statements
DATA = edb_stat_statements--1.0.sql
PGFILEDESC = "edb_stat_statements - execution statistics of EdgeDB queries"

LDFLAGS_SL += $(filter -lm, $(LIBS))

REGRESS = select dml cursors utility level_tracking planning \
	user_activity wal entry_timestamp privileges extended \
	parallel cleanup oldextversions

TAP_TESTS = 1

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
