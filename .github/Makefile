.PHONY: all

ROOT = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))

all: workflows/nightly.yml workflows/release.yml workflows/tests.yml \
    workflows/tests-managed-pg.yml workflows/tests-ha.yml

workflows/%.yml: workflows.src/%.tpl.yml workflows.src/%.targets.yml
	$(ROOT)/workflows.src/render.py $* $*.targets.yml

workflows.src/tests.tpl.yml: workflows.src/tests.inc.yml
	touch $(ROOT)/workflows.src/tests.tpl.yml

workflows.src/tests-managed-pg.tpl.yml: workflows.src/tests.inc.yml
	touch $(ROOT)/workflows.src/tests-managed-pg.tpl.yml

workflows.src/tests-ha.tpl.yml: workflows.src/tests.inc.yml
	touch $(ROOT)/workflows.src/tests-ha.tpl.yml
