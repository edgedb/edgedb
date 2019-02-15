.PHONY: docs cython postgres pygments

SPHINXOPTS:="-W -n"


cython:
	find edb -name '*.pyx' | xargs touch
	python setup.py build_ext --inplace


docs:
	find docs -name '*.rst' | xargs touch
	$(MAKE) -C docs html SPHINXOPTS=$(SPHINXOPTS) BUILDDIR="../build"


postgres:
	python setup.py build_postgres


pygments:
	edb gen-meta-grammars edgeql > edb/edgeql/pygments/meta.py
	edb gen-meta-grammars edgeql eschema > edb/eschema/pygments/meta.py
