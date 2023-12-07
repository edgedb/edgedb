#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2012-present MagicStack Inc. and the EdgeDB authors.
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

from edb.testbase import lang as tb
from edb.ir import pathid
from edb.ir import typeutils as irtyputils
from edb.schema import name as s_name
from edb.schema import pointers as s_pointers


class TestEdgeQLIRPathID(tb.BaseEdgeQLCompilerTest):
    """Unit tests for path id logic."""

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.esdl')

    def extend(self, pid, step, ns=frozenset(), dir='>'):
        nschema, typ = irtyputils.ir_typeref_to_type(self.schema, pid.target)
        assert nschema == self.schema
        ptr = None
        if rptr := pid.rptr():
            nschema, ptr = irtyputils.ptrcls_from_ptrref(
                rptr, schema=self.schema)
            assert nschema == self.schema

        dir = s_pointers.PointerDirection(dir)
        ns = frozenset(ns)

        if step[0] == '@':
            src, step = ptr, step[1:]
            pid = pid.ptr_path()
        else:
            src = typ

        ptr = src.getptr(self.schema, s_name.UnqualName(step))
        ptr_ref = irtyputils.ptrref_from_ptrcls(
            schema=self.schema,
            ptrcls=ptr,
            cache=None,
            typeref_cache=None,
        )

        return pid.extend(ptrref=ptr_ref, ns=ns, direction=dir)

    def extend_many(self, pid, *path):
        for step in path:
            if not isinstance(step, tuple):
                ns, dir = frozenset(), '>'
            elif len(step) == 2:
                step, ns = step
                dir = '>'
            else:
                step, ns, dir = step

            pid = self.extend(pid, step, ns=ns, dir=dir)

        return pid

    def mk_path(self, *path, ns=frozenset()):
        start = path[0]

        typ = self.schema.get(f'default::{start}')
        pid = pathid.PathId.from_type(self.schema, typ, namespace=ns, env=None)

        return self.extend_many(pid, *path[1:])

    def test_edgeql_ir_pathid_basic(self):
        User = self.schema.get('default::User')
        deck_ptr = User.getptr(self.schema, s_name.UnqualName('deck'))

        pid_1 = self.mk_path('User')
        self.assertEqual(
            str(pid_1),
            '(default::User)')

        self.assertTrue(pid_1.is_objtype_path())
        self.assertFalse(pid_1.is_scalar_path())

        self.assertIsNone(pid_1.rptr())
        self.assertIsNone(pid_1.rptr_dir())
        self.assertIsNone(pid_1.rptr_name())
        self.assertIsNone(pid_1.src_path())

        pid_2 = self.extend(pid_1, 'deck')

        self.assertEqual(
            str(pid_2),
            '(default::User).>deck[IS default::Card]')

        self.assertEqual(pid_2.rptr().name, deck_ptr.get_name(self.schema))
        self.assertEqual(pid_2.rptr_dir(),
                         s_pointers.PointerDirection.Outbound)
        self.assertEqual(pid_2.rptr_name().name, 'deck')
        self.assertEqual(pid_2.src_path(), pid_1)

        ptr_pid = pid_2.ptr_path()
        self.assertEqual(
            str(ptr_pid),
            '(default::User).>deck[IS default::Card]@')

        self.assertTrue(ptr_pid.is_ptr_path())
        self.assertFalse(ptr_pid.is_objtype_path())
        self.assertFalse(ptr_pid.is_scalar_path())

        self.assertEqual(ptr_pid.tgt_path(), pid_2)

        prop_pid = self.extend(pid_2, '@count')
        self.assertEqual(
            str(prop_pid),
            '(default::User).>deck[IS default::Card]@count[IS std::int64]')

        self.assertFalse(prop_pid.is_ptr_path())
        self.assertFalse(prop_pid.is_objtype_path())
        self.assertTrue(prop_pid.is_scalar_path())
        self.assertTrue(prop_pid.is_linkprop_path())
        self.assertEqual(prop_pid.src_path(), ptr_pid)

    def test_edgeql_ir_pathid_startswith(self):
        pid_1 = self.mk_path('User')
        pid_2 = self.extend(pid_1, 'deck')
        ptr_pid = pid_2.ptr_path()
        prop_pid = self.extend(pid_2, '@count')

        self.assertTrue(pid_2.startswith(pid_1))
        self.assertFalse(pid_1.startswith(pid_2))

        self.assertTrue(ptr_pid.startswith(pid_1))
        self.assertTrue(prop_pid.startswith(pid_1))

        self.assertFalse(ptr_pid.startswith(pid_2))
        self.assertFalse(prop_pid.startswith(pid_2))

        self.assertTrue(prop_pid.startswith(ptr_pid))

    def test_edgeql_ir_pathid_namespace_01(self):
        ns = frozenset(('foo',))
        pid_1 = self.mk_path('User', ns=ns)
        pid_2 = self.extend(pid_1, 'deck')
        ptr_pid = pid_2.ptr_path()
        prop_pid = self.extend(pid_2, '@count')

        self.assertEqual(pid_1.namespace, ns)
        self.assertEqual(pid_2.namespace, ns)
        self.assertEqual(ptr_pid.namespace, ns)
        self.assertEqual(prop_pid.namespace, ns)

        pid_1_no_ns = self.mk_path('User')
        self.assertNotEqual(pid_1, pid_1_no_ns)

    def test_edgeql_ir_pathid_namespace_02(self):
        # Test cases where the prefix is in a different namespace

        ns_1 = frozenset(('foo',))
        ns_2 = frozenset(('bar',))

        pid_1 = self.mk_path('Card')
        pid_2 = self.extend(pid_1, 'owners', ns=ns_1)
        pid_2_no_ns = self.extend(pid_1, 'owners')

        self.assertNotEqual(pid_2, pid_2_no_ns)
        self.assertEqual(pid_2.src_path(), pid_1)

        pid_3 = self.extend(pid_2, 'deck', ns=ns_2)
        prop_pid = self.extend(pid_3, '@count')

        self.assertEqual(prop_pid.src_path().namespace, ns_1 | ns_2)
        self.assertEqual(prop_pid.src_path().src_path().namespace, ns_1)
        self.assertFalse(prop_pid.src_path().src_path().src_path().namespace)

        prefixes = [str(p) for p in pid_3.iter_prefixes()]

        self.assertEqual(
            prefixes,
            [
                '(default::Card)',
                'foo@@(default::Card).>owners[IS default::User]',
                'bar@foo@@(default::Card).>owners[IS default::User]'
                '.>deck[IS default::Card]',
            ]
        )

    def test_edgeql_ir_pathid_replace_01(self):
        base_1 = self.mk_path('Card')
        base_2 = self.mk_path('SpecialCard')

        ns = {'ns'}
        ptr_1 = self.extend(base_1, 'name', ns=ns)
        ptr_2 = self.extend(base_2, 'name', ns=ns)

        ptr_1b = irtyputils.replace_pathid_prefix(ptr_1, base_1, base_2)

        self.assertEqual(repr(ptr_2), repr(ptr_1b))

    def test_edgeql_ir_pathid_replace_02(self):
        base_1 = self.mk_path('Card', ns={'ns1'})
        base_2 = self.mk_path('SpecialCard', ns={'ns2'})

        ptr_1 = self.extend(base_1, 'name')
        ptr_2 = self.extend(base_2, 'name')

        ptr_1b = irtyputils.replace_pathid_prefix(ptr_1, base_1, base_2)

        self.assertEqual(repr(ptr_2), repr(ptr_1b))

    def test_edgeql_ir_pathid_replace_03a(self):
        base_1 = self.mk_path('User', 'deck')
        base_2 = self.mk_path('Bot', 'deck')

        ptr_1 = self.extend(base_1, '@count')
        ptr_2 = self.extend(base_2, '@count')

        ptr_1b = irtyputils.replace_pathid_prefix(
            ptr_1, base_1, base_2, permissive_ptr_path=True)

        self.assertEqual(repr(ptr_2), repr(ptr_1b))

        ptr_1b = irtyputils.replace_pathid_prefix(
            ptr_1, base_1.ptr_path(), base_2.ptr_path())

        self.assertEqual(repr(ptr_2), repr(ptr_1b))

    def test_edgeql_ir_pathid_replace_03b(self):
        base_1 = self.mk_path('User', 'deck')
        base_2 = self.mk_path('Bot', 'deck')

        ptr_1 = self.extend(base_1, '@count')
        ptr_2 = self.extend(base_2, '@count')

        ptr_1b = irtyputils.replace_pathid_prefix(
            ptr_1, base_1, base_2, permissive_ptr_path=True)

        self.assertEqual(repr(ptr_2), repr(ptr_1b))
