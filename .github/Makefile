.PHONY: all

ROOT = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))

all: workflows/nightly.yml

workflows/nightly.yml: workflows.src/nightly.tpl.yml workflows.src/nightly.targets.yml
	$(ROOT)/workflows.src/render.py nightly nightly.targets.yml
