.PHONY: docs cython postgres postgres-ext pygments

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
	out=$$(edb gen-meta-grammars edgeql) && \
		echo "$$out" > edb/edgeql/pygments/meta.py
