#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import os.path

from distutils.command import build


class build(build.build):
    def _compile_parsers(self):
        import parsing

        import edgedb.lang.edgeql.parser.grammar.single as edgeql_spec
        import edgedb.lang.edgeql.parser.grammar.block as edgeql_spec2
        import edgedb.server.pgsql.parser.pgsql as pgsql_spec
        import edgedb.lang.schema.parser.grammar.declarations as schema_spec
        import edgedb.lang.graphql.parser.grammar.document as graphql_spec

        base_path = os.path.dirname(
            os.path.dirname(os.path.dirname(__file__)))

        for spec in (edgeql_spec, edgeql_spec2, pgsql_spec,
                     schema_spec, graphql_spec):
            subpath = os.path.dirname(spec.__file__)[len(base_path) + 1:]
            cache_dir = os.path.join(self.build_lib, subpath)
            os.makedirs(cache_dir, exist_ok=True)
            cache = os.path.join(
                cache_dir, spec.__name__.rpartition('.')[2] + '.pickle')
            parsing.Spec(spec, pickleFile=cache, verbose=True)

    def run(self, *args, **kwargs):
        super().run(*args, **kwargs)
        self._compile_parsers()
