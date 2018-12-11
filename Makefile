.PHONY: docs

SPHINXOPTS:="-W -n"

docs:
	find docs -name '*.rst' | xargs touch
	$(MAKE) -C docs html SPHINXOPTS=$(SPHINXOPTS) BUILDDIR="../build"
