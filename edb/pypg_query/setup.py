from setuptools import setup, Extension
from Cython.Build import cythonize

import os.path

libpg_query = os.path.join('.', 'libpg_query')

extensions = cythonize([
    Extension('pypg_query.parser',
              ['pypg_query/parser.pyx'],
              libraries=['pg_query'],
              include_dirs=[libpg_query],
              library_dirs=[libpg_query])
])

setup(name='pypg_query',
      packages=['pypg_query'],
      ext_modules=extensions)
