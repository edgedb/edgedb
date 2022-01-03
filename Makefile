.PHONY: build docs cython postgres postgres-ext pygments
.DEFAULT_GOAL := build

SPHINXOPTS:="-W -n"


cython:
	find edb -name '*.pyx' | xargs touch
	BUILD_EXT_MODE=py-only python setup.py build_ext --inplace


docs:
	find docs -name '*.rst' | xargs touch
	$(MAKE) -C docs html SPHINXOPTS=$(SPHINXOPTS) BUILDDIR="../build"


postgres:
	python setup.py build_postgres


pygments:
	out=$$(edb gen-meta-grammars edgeql) && \
		echo "$$out" > edb/edgeql/pygments/meta.py


casts:
	out=$$(edb gen-cast-table) && \
		echo "$$out" > docs/reference/edgeql/casts.csv


build:
	pip install -Ue .[docs,test]


clean:
	git clean -Xfd -e "!/*.code-workspace" -e "!/*.vscode"
