root=$(dir $(abspath $(lastword $(MAKEFILE_LIST))))
tests=$(t)
tests?="all"
debug=$(d)

all:

test:
	@PYTHONPATH="$$PYTHONPATH:$(abspath $(root))" python3.1 $(root)/semantix/tests/run.py --tests=$(tests) --debug=$(debug)
