#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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


from edb.testbase import server as tb


class TestEdgeQLExtPgCrypto(tb.QueryTestCase):
    EXTENSIONS = ['pgcrypto']
    BACKEND_SUPERUSER = True

    async def test_edgeql_ext_pgcrypto_digest(self):
        CASES = [
            ("md5", "rL0Y20zC+Fzt72VPzMSk2A=="),
            ("sha1", "C+7Hteo/D9vJXQ3UfzxbwnXaijM="),
            ("sha224", "CAj2TmDViXn8tnbJbsk4Jw3qQkRa7vzTpOb42w=="),
            ("sha256", "LCa0a2j/xo/5m0U8HTBBNBNCLXBkg7+g+YpeiGJm564="),
            ("sha384", "mMEf/f3VQGdrGhN8saIrKnA1DJpEFx1rEYDGvly7LuP3n"
                       + "VMsih3Z7y6OCOdSo7q7"),
            ("sha512", "9/u6bgY2+JDlb7vzKD5STG+jIErimDgtYkdB0NxmODJuK"
                       + "CxBvl5CVNiCB3LFUYosWowMf37aGVlK\nfrU5RT4e1w=="),
        ]

        for hash_type, result in CASES:
            with self.subTest(hash_type):
                await self.assert_query_result(
                    """
                        select ext::pgcrypto::digest(<str>$data, <str>$type)
                    """,
                    [result],
                    variables={
                        "data": "foo",
                        "type": hash_type,
                    },
                    json_only=True,
                )

                await self.assert_query_result(
                    """
                        select ext::pgcrypto::digest(<bytes>$data, <str>$type)
                    """,
                    [result],
                    variables={
                        "data": b"foo",
                        "type": hash_type,
                    },
                    json_only=True,
                )

    async def test_edgeql_ext_pgcrypto_hmac(self):
        CASES = [
            ("md5", "Mbbbnl60rdtC8abKBzZ63A=="),
            ("sha1", "hdFVxV7ShqMAvRzxJN4I2H6RTzo="),
            ("sha224", "1/UIN19LWxwjbS3xuFDeJHSpE2RIdnBeYr14zA=="),
            ("sha256", "FHkzIYqqvAuLEKKzpcNGhMjZQ0G88QpHNtxycPd0GFE="),
            ("sha384", "HZBw0Hy3dG4GZMzMbOwfqZbcf0Y2iYKs+iCV7o1z/iW1"
                       + "tuMieZAM2w/TcqNlTkHF"),
            ("sha512", "JCV9chBYKmXHMexVFZyBhMwkwCSJRT5YWH9x9EwjotYb"
                       + "S3IVSonRey1JRIqEUuoGb0/FaivOrUXA\niFcv/M2z2A=="),
        ]

        for hash_type, result in CASES:
            with self.subTest(hash_type):
                await self.assert_query_result(
                    """
                        select ext::pgcrypto::hmac(
                            <str>$data, <str>$key, <str>$type)
                    """,
                    [result],
                    variables={
                        "data": "foo",
                        "key": "bar",
                        "type": hash_type,
                    },
                    json_only=True,
                )

                await self.assert_query_result(
                    """
                        select ext::pgcrypto::hmac(
                            <bytes>$data, <bytes>$key, <str>$type)
                    """,
                    [result],
                    variables={
                        "data": b"foo",
                        "key": b"bar",
                        "type": hash_type,
                    },
                    json_only=True,
                )

    async def test_edgeql_ext_pgcrypto_gen_salt(self):
        CASES = [
            ("bf", 5),
            ("xdes", 801),
            ("md5", None),
            ("des", None),
        ]

        await self.assert_query_result(
            """
                select {
                    salt := ext::pgcrypto::gen_salt()
                }
            """,
            [{}],
            json_only=True,
        )

        for hash_type, iter_count in CASES:
            with self.subTest(hash_type):
                await self.assert_query_result(
                    """
                        select {
                            salt := ext::pgcrypto::gen_salt(<str>$type)
                        }
                    """,
                    [{}],
                    variables={
                        "type": hash_type,
                    },
                    json_only=True,
                )

                if iter_count is not None:
                    await self.assert_query_result(
                        """
                            select {
                                salt := ext::pgcrypto::gen_salt(
                                    <str>$type,
                                    <int64>$iter_count,
                                )
                            }
                        """,
                        [{}],
                        variables={
                            "type": hash_type,
                            "iter_count": iter_count,
                        },
                        json_only=True,
                    )

    async def test_edgeql_ext_pgcrypto_crypt(self):
        CASES = [
            (
                "bf",
                "$2a$06$1qmwsi8lj0HKQnokkCkZSe",
                "$2a$06$1qmwsi8lj0HKQnokkCkZSenubuHv5CGJ2ICxcOPjOr6xOKDBY..Eu",
            ),
            (
                "md5",
                "$1$ePFh8A9K",
                "$1$ePFh8A9K$xN/KWq.qDWTW9HYvx8/VP/",
            ),
            (
                "xdes",
                "_J9..3OQ/",
                "_J9..3OQ/ylnZ6cP2muw",
            ),
            (
                "des",
                "JU",
                "JUJ5Ovy43JsfM",
            ),
        ]

        for hash_type, salt, result in CASES:
            with self.subTest(hash_type):
                await self.assert_query_result(
                    """
                        select ext::pgcrypto::crypt("foo", <str>$salt)
                    """,
                    [result],
                    variables={
                        "salt": salt,
                    },
                    json_only=True,
                )
