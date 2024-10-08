# Configurable parts
SQL_MODULE := sql

### Boilerplate
PYTHON := python3
EDB := $(PYTHON) -m edb.tools $(EDBFLAGS)
MKS := $(shell $(EDB) config --make-include)
include $(MKS)
### End Boilerplate
