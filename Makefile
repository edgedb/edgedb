.PHONY: docs cython

SPHINXOPTS:="-W -n"

docs:
	find docs -name '*.rst' | xargs touch
	$(MAKE) -C docs html SPHINXOPTS=$(SPHINXOPTS) BUILDDIR="../build"

cython:
	find edb -name '*.pyx' | xargs touch
	python setup.py build_ext --inplace
