#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2011-present MagicStack Inc. and the EdgeDB authors.
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


import textwrap
import unittest

from edb.common import xdedent

X = xdedent.escape


EXPECTED_1 = '''
call_something()
foo = 10
while True:
    if bar:
        do_something(
            foo,
            bar,
            [1, 2, 3,
             4, 5, 6],
            reify(
                spam
            ),
            reify(
                eggs
            ),
            reify(
                ham
            )
        )
        another
more
'''.strip('\n')

EXPECTED_2 = '''
call_something()
foo = 10
while True:
    if bar:
        another
more
'''.strip('\n')


class XDedentTests(unittest.TestCase):
    def _test1(self, do_something=True):
        foo = 'foo'
        bar = 'bar'

        left = '''
            [1, 2, 3,
             4, 5, 6]
        '''

        things = []
        for thing in ['spam', 'eggs', 'ham']:
            things.append(f'''
                reify(
                    {thing}
                )
            ''')

        sep = ",\n"
        if do_something:
            orig = f'''
                do_something(
                    {foo},
                    {bar},
                    {X(left)},
                    {X(sep.join(X(x) for x in things))}
                )
            '''
        else:
            orig = xdedent.LINE_BLANK

        return xdedent.xdedent(f'''
            call_something()
            {X(foo)} = 10
            while True:
                if {bar}:
                    {X(orig)}
                    another
            more
        ''')

    def test_xdedent_1(self):
        self.assertEqual(self._test1(True), EXPECTED_1)
        self.assertEqual(self._test1(False), EXPECTED_2)

    def test_xdedent_2(self):
        EXPECTED = textwrap.dedent('''\
        LATERAL (SELECT
                    ARRAY[(q0.val)->>'name'] AS key
                ) AS k0,
        LATERAL (SELECT
                    ARRAY[(q0.val)->>'name'] AS key
                ) AS k0''')

        q = '''
        (SELECT
            ARRAY[(q0.val)->>'name'] AS key
        ) AS k0
        '''
        sources = [q, q]
        fromlist = ',\n'.join(f'LATERAL {X(s)}' for s in sources)
        res = xdedent.xdedent(fromlist)
        self.assertEqual(res, EXPECTED)
