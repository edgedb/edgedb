root=$(dir $(abspath $(lastword $(MAKEFILE_LIST))))
tests:=$(t)
debug:=$(d)

ifdef tests
    tests:=--tests=$(tests) -k testmask
endif

ifdef debug
    debug:=--semantix-debug=$(debug) --capture=no
endif

all:

test:
	@PYTHONPATH="$$PYTHONPATH:$(abspath $(root))" EPYTHON=python3.1 /usr/bin/py.test $(tests) $(debug) -x --colorize
