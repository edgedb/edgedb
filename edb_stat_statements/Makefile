# contrib/pg_stat_statements/Makefile

MODULE_big = pg_stat_statements
OBJS = \
	$(WIN32RES) \
	pg_stat_statements.o

EXTENSION = pg_stat_statements
DATA = pg_stat_statements--1.0.sql
PGFILEDESC = "pg_stat_statements - execution statistics of SQL statements"

LDFLAGS_SL += $(filter -lm, $(LIBS))

REGRESS_OPTS = --temp-config $(top_srcdir)/contrib/pg_stat_statements/pg_stat_statements.conf
REGRESS = select dml cursors utility level_tracking planning \
	user_activity wal entry_timestamp privileges extended \
	parallel cleanup oldextversions
# Disabled because these tests require "shared_preload_libraries=pg_stat_statements",
# which typical installcheck users do not have (e.g. buildfarm clients).
NO_INSTALLCHECK = 1

TAP_TESTS = 1

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
