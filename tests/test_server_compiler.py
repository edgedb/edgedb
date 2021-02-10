#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


from edb.testbase import lang as tb
from edb.server import compiler as edbcompiler


class TestServerCompiler(tb.BaseSchemaLoadTest):

    SCHEMA = '''
        type Foo {
            property bar -> str;
        }
    '''

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._std_schema = tb._load_std_schema()

    def test_server_compiler_compile_edgeql_script(self):
        compiler = tb.new_compiler()
        context = edbcompiler.new_compiler_context(
            user_schema=self.schema,
            modaliases={None: 'test'},
        )

        edbcompiler.compile_edgeql_script(
            compiler=compiler,
            ctx=context,
            eql='''
                SELECT Foo {
                    bar
                }
            ''',
        )
