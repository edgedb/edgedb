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


import unittest

from edgedb.lang.common.term import Style16, Style256


class TermStyleTests(unittest.TestCase):

    def test_common_term_style16(self):
        s = Style16(color='red', bgcolor='green', bold=True)

        assert s.color == 'red'
        assert s.bgcolor == 'green'

        assert s.bold
        s.bold = False
        assert not s.bold
        s.underline = True
        assert s.underline

        s.color = 'yellow'
        assert s.color == 'yellow'

        with self.assertRaisesRegex(ValueError, 'unknown color'):
            s.color = '#FFF'

        assert not s.empty
        assert Style16().empty

    def test_common_term_style256(self):
        assert Style256(color='red')._color == 196
        assert Style256(color='#FF0000')._color == 196
        assert Style256(color='#FE0000')._color == 196
        assert Style256(color='darkmagenta')._color == 90

        with self.assertRaisesRegex(ValueError, 'Unknown color'):
            Style256(color='foooocolor')
