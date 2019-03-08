.PHONY: docs cython postgres

SPHINXOPTS:="-W -n"


cython:
	find edb -name '*.pyx' | xargs touch
	python setup.py build_ext --inplace


docs:
	find docs -name '*.rst' | xargs touch
	$(MAKE) -C docs html SPHINXOPTS=$(SPHINXOPTS) BUILDDIR="../build"


postgres:
	python setup.py build_postgres
