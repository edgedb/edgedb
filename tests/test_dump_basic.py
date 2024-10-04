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

import hashlib
import os
import random
import tempfile

from edb.testbase import server as tb


class TestDumpBasics(tb.DatabaseTestCase, tb.CLITestCaseMixin):
    DEFAULT_MODULE = 'test'

    TRANSACTION_ISOLATION = False

    SETUP = '''
        CREATE TYPE test::Tmp {
            CREATE REQUIRED PROPERTY idx -> std::int64;
            CREATE REQUIRED PROPERTY data -> std::bytes;
        };
    '''

    TEARDOWN = '''
        DROP TYPE test::Tmp;
    '''

    RANDBYTES = os.urandom(1024) * 1024
    DBSIZE = 1024 * 1024 * 50

    def some_bytes(self, nbytes):
        buf = b''
        while len(buf) < nbytes:
            buf += self.RANDBYTES[:nbytes - len(buf)]
        return buf

    async def test_dump_fuzz_01(self):
        if not self.has_create_database:
            self.skipTest('create database is not supported by the backend')

        # This test creates a simple `DBSIZE` DB filled with semi-random
        # byte strings. While the DB is populated a hash of all byte
        # strings is computed. The DB is then dumped and restored.
        # A new hash computed of all byte strings in the new DB.
        # The former and latter hashes must be the same.
        #
        # This test is not designed to test how well the schema is
        # preserved or compatibility between different edgedb or
        # dump versions. Its only purpose is to make sure that
        # the basic dump I/O and network protocol functions correctly.

        hasher = hashlib.sha1()

        idx = 0
        total_len = 0
        while total_len < self.DBSIZE:
            data = self.some_bytes(random.randint(100_000, 10_000_000))
            hasher.update(data)
            total_len += len(data)

            await self.con.query_single('''
                INSERT test::Tmp {
                    idx := <int64>$idx,
                    data := <bytes>$data,
                }
            ''', idx=idx, data=data)

            idx += 1

        expected_hash = hasher.digest()
        nrows = idx
        dbname = self.get_database_name()
        restored_dbname = f'{dbname}_restored'

        try:
            await self.con.execute(f'CREATE DATABASE {restored_dbname}')
            with tempfile.TemporaryDirectory() as f:
                fname = os.path.join(f, 'dump')
                self.run_cli('-d', f"{dbname}", 'dump', fname)
                self.run_cli('-d', restored_dbname, 'restore', fname)
            con2 = await self.connect(database=restored_dbname)
        except Exception:
            await tb.drop_db(self.con, restored_dbname)
            raise

        try:
            hasher = hashlib.sha1()
            for idx in range(nrows):
                # We don't have cursors yet and we also don't want to fetch
                # a huge data set in one hop; so we fetch row by row.
                # Not ideal, but isn't too bad either.
                r = await con2.query_single('''
                    WITH
                        MODULE test,
                        A := (SELECT Tmp FILTER Tmp.idx = <int64>$idx)
                    SELECT A.data
                    LIMIT 1
                ''', idx=idx)

                hasher.update(r)

            self.assertEqual(hasher.digest(), expected_hash)
        finally:
            await con2.aclose()
            await tb.drop_db(self.con, restored_dbname)
