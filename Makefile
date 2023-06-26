.PHONY: build docs cython postgres postgres-ext pygments build-reqs
.DEFAULT_GOAL := build

SPHINXOPTS:="-W -n"

BUILD_REQS_SCRIPT='print("\x00".join(__import__("build").ProjectBuilder(".").build_system_requires))'

build-reqs:
	python -m pip install --no-build-isolation build
	python -c $(BUILD_REQS_SCRIPT) | xargs -0 python -m pip install --no-build-isolation


cython: build-reqs
	find edb -name '*.pyx' | xargs touch
	BUILD_EXT_MODE=py-only python setup.py build_ext --inplace


rust: build-reqs
	BUILD_EXT_MODE=rust-only python setup.py build_ext --inplace


docs: build-reqs
	find docs -name '*.rst' | xargs touch
	$(MAKE) -C docs html SPHINXOPTS=$(SPHINXOPTS) BUILDDIR="../build"


postgres: build-reqs
	python setup.py build_postgres

pg-ext: build-reqs
	python setup.py build_postgres_extensions

ui: build-reqs
	python setup.py build_ui


pygments: build-reqs
	out=$$(edb gen-meta-grammars edgeql) && \
		echo "$$out" > edb/tools/pygments/edgeql/meta.py


casts: build-reqs
	out=$$(edb gen-cast-table) && \
		echo "$$out" > docs/reference/edgeql/casts.csv


build: build-reqs
	find edb -name '*.pyx' | xargs touch
	pip install -Ue .[docs,test]


clean:
	git clean -Xfd -e "!/*.code-workspace" -e "!/*.vscode"
