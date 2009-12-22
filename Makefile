root=$(dir $(abspath $(lastword $(MAKEFILE_LIST))))
tests:=$(t)
debug:=$(d)

ifdef tests
    tests:=--tests=$(tests) -k testmask
endif

ifdef debug
    debug:=--semantix-debug=$(debug)
endif

all:

test:
	@PYTHONPATH="$$PYTHONPATH:$(abspath $(root))" python3.1 /usr/bin/py.test $(tests) $(debug) -x
