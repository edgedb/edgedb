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
from edb.schema import pointers as s_pointers


class TestEdgeQLIRPathID(tb.BaseEdgeQLCompilerTest):
    """Unit tests for path id logic."""

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.esdl')

    def test_edgeql_ir_pathid_basic(self):
        User = self.schema.get('test::User')
        deck_ptr = User.getptr(self.schema, 'deck')
        count_prop = deck_ptr.getptr(self.schema, 'count')

        pid_1 = pathid.PathId.from_type(self.schema, User)
        self.assertEqual(
            str(pid_1),
            '(test::User)')

        self.assertTrue(pid_1.is_objtype_path())
        self.assertFalse(pid_1.is_scalar_path())

        self.assertIsNone(pid_1.rptr())
        self.assertIsNone(pid_1.rptr_dir())
        self.assertIsNone(pid_1.rptr_name())
        self.assertIsNone(pid_1.src_path())

        deck_ptr_ref = irtyputils.ptrref_from_ptrcls(
            schema=self.schema,
            ptrcls=deck_ptr,
        )
        pid_2 = pid_1.extend(ptrref=deck_ptr_ref, schema=self.schema)
        self.assertEqual(
            str(pid_2),
            '(test::User).>deck[IS test::Card]')

        self.assertEqual(pid_2.rptr().name, deck_ptr.get_name(self.schema))
        self.assertEqual(pid_2.rptr_dir(),
                         s_pointers.PointerDirection.Outbound)
        self.assertEqual(pid_2.rptr_name().name, 'deck')
        self.assertEqual(pid_2.src_path(), pid_1)

        ptr_pid = pid_2.ptr_path()
        self.assertEqual(
            str(ptr_pid),
            '(test::User).>deck[IS test::Card]@')

        self.assertTrue(ptr_pid.is_ptr_path())
        self.assertFalse(ptr_pid.is_objtype_path())
        self.assertFalse(ptr_pid.is_scalar_path())

        self.assertEqual(ptr_pid.tgt_path(), pid_2)

        count_prop_ref = irtyputils.ptrref_from_ptrcls(
            schema=self.schema,
            ptrcls=count_prop,
        )
        prop_pid = ptr_pid.extend(ptrref=count_prop_ref, schema=self.schema)
        self.assertEqual(
            str(prop_pid),
            '(test::User).>deck[IS test::Card]@count[IS std::int64]')

        self.assertFalse(prop_pid.is_ptr_path())
        self.assertFalse(prop_pid.is_objtype_path())
        self.assertTrue(prop_pid.is_scalar_path())
        self.assertTrue(prop_pid.is_linkprop_path())
        self.assertEqual(prop_pid.src_path(), ptr_pid)

    def test_edgeql_ir_pathid_startswith(self):
        User = self.schema.get('test::User')
        deck_ptr = User.getptr(self.schema, 'deck')
        deck_ptr_ref = irtyputils.ptrref_from_ptrcls(
            schema=self.schema,
            ptrcls=deck_ptr,
        )
        count_prop = deck_ptr.getptr(self.schema, 'count')
        count_prop_ref = irtyputils.ptrref_from_ptrcls(
            schema=self.schema,
            ptrcls=count_prop,
        )

        pid_1 = pathid.PathId.from_type(self.schema, User)
        pid_2 = pid_1.extend(ptrref=deck_ptr_ref, schema=self.schema)
        ptr_pid = pid_2.ptr_path()
        prop_pid = ptr_pid.extend(ptrref=count_prop_ref, schema=self.schema)

        self.assertTrue(pid_2.startswith(pid_1))
        self.assertFalse(pid_1.startswith(pid_2))

        self.assertTrue(ptr_pid.startswith(pid_1))
        self.assertTrue(prop_pid.startswith(pid_1))

        self.assertFalse(ptr_pid.startswith(pid_2))
        self.assertFalse(prop_pid.startswith(pid_2))

        self.assertTrue(prop_pid.startswith(ptr_pid))

    def test_edgeql_ir_pathid_namespace_01(self):
        User = self.schema.get('test::User')
        deck_ptr = User.getptr(self.schema, 'deck')
        deck_ptr_ref = irtyputils.ptrref_from_ptrcls(
            schema=self.schema,
            ptrcls=deck_ptr,
        )
        count_prop = deck_ptr.getptr(self.schema, 'count')
        count_prop_ref = irtyputils.ptrref_from_ptrcls(
            schema=self.schema,
            ptrcls=count_prop,
        )

        ns = frozenset(('foo',))
        pid_1 = pathid.PathId.from_type(self.schema, User, namespace=ns)
        pid_2 = pid_1.extend(ptrref=deck_ptr_ref, schema=self.schema)
        ptr_pid = pid_2.ptr_path()
        prop_pid = ptr_pid.extend(ptrref=count_prop_ref, schema=self.schema)

        self.assertEqual(pid_1.namespace, ns)
        self.assertEqual(pid_2.namespace, ns)
        self.assertEqual(ptr_pid.namespace, ns)
        self.assertEqual(prop_pid.namespace, ns)

        pid_1_no_ns = pathid.PathId.from_type(self.schema, User)
        self.assertNotEqual(pid_1, pid_1_no_ns)

    def test_edgeql_ir_pathid_namespace_02(self):
        # Test cases where the prefix is in a different namespace

        Card = self.schema.get('test::Card')
        User = self.schema.get('test::User')
        owners_ptr = Card.getptr(self.schema, 'owners')
        owners_ptr_ref = irtyputils.ptrref_from_ptrcls(
            schema=self.schema,
            ptrcls=owners_ptr,
        )
        deck_ptr = User.getptr(self.schema, 'deck')
        deck_ptr_ref = irtyputils.ptrref_from_ptrcls(
            schema=self.schema,
            ptrcls=deck_ptr,
        )
        count_prop = deck_ptr.getptr(self.schema, 'count')
        count_prop_ref = irtyputils.ptrref_from_ptrcls(
            schema=self.schema,
            ptrcls=count_prop,
        )

        ns_1 = frozenset(('foo',))
        ns_2 = frozenset(('bar',))

        pid_1 = pathid.PathId.from_type(self.schema, Card)
        pid_2 = pid_1.extend(ptrref=owners_ptr_ref, ns=ns_1,
                             schema=self.schema)
        pid_2_no_ns = pid_1.extend(ptrref=owners_ptr_ref, schema=self.schema)

        self.assertNotEqual(pid_2, pid_2_no_ns)
        self.assertEqual(pid_2.src_path(), pid_1)

        pid_3 = pid_2.extend(ptrref=deck_ptr_ref, ns=ns_2, schema=self.schema)
        ptr_pid = pid_3.ptr_path()
        prop_pid = ptr_pid.extend(ptrref=count_prop_ref, schema=self.schema)

        self.assertEqual(prop_pid.src_path().namespace, ns_1 | ns_2)
        self.assertEqual(prop_pid.src_path().src_path().namespace, ns_1)
        self.assertFalse(prop_pid.src_path().src_path().src_path().namespace)

        prefixes = [str(p) for p in pid_3.iter_prefixes()]

        self.assertEqual(
            prefixes,
            [
                '(test::Card)',
                'foo@@(test::Card).>owners[IS test::User]',
                'bar@foo@@(test::Card).>owners[IS test::User]'
                '.>deck[IS test::Card]',
            ]
        )
