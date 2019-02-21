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


import base64
import unittest

from edb.common import scram


class TestSCRAM(unittest.TestCase):

    def test_scram_sha_256_rfc_example(self):
        # Test SCRAM-SHA-256 against an example in RFC 7677

        username = 'user'
        password = 'pencil'
        client_nonce = 'rOprNGfwEbeRWgbNEkqO'
        server_nonce = 'rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0'
        salt = 'W22ZaJ0SNY7soEsUEjb6gQ=='
        channel_binding = 'biws'
        iterations = 4096

        client_first = f'n={username},r={client_nonce}'
        server_first = f'r={server_nonce},s={salt},i={iterations}'
        client_final = f'c={channel_binding},r={server_nonce}'

        AuthMessage = f'{client_first},{server_first},{client_final}'.encode()
        SaltedPassword = scram.get_salted_password(
            scram.saslprep(password).encode('utf-8'),
            base64.b64decode(salt),
            iterations)
        ClientKey = scram.get_client_key(SaltedPassword)
        ServerKey = scram.get_server_key(SaltedPassword)
        StoredKey = scram.H(ClientKey)
        ClientSignature = scram.HMAC(StoredKey, AuthMessage)
        ClientProof = scram.XOR(ClientKey, ClientSignature)
        ServerProof = scram.HMAC(ServerKey, AuthMessage)

        self.assertEqual(scram.B64(ClientProof),
                         'dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=')

        self.assertEqual(scram.B64(ServerProof),
                         '6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=')

    def test_scram_sha_256_verifier(self):
        salt = 'W22ZaJ0SNY7soEsUEjb6gQ=='
        password = 'pencil'

        v = scram.build_verifier(
            password,
            salt=base64.b64decode(salt),
            iterations=4096,
        )

        stored_key = 'WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY='
        server_key = 'wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU='

        self.assertEqual(
            v, f'SCRAM-SHA-256$4096:{salt}${stored_key}:{server_key}')

        parsed = scram.parse_verifier(v)

        self.assertEqual(parsed.mechanism, 'SCRAM-SHA-256')
        self.assertEqual(parsed.iterations, 4096)
        self.assertEqual(parsed.salt, base64.b64decode(salt))
        self.assertEqual(parsed.stored_key, base64.b64decode(stored_key))
        self.assertEqual(parsed.server_key, base64.b64decode(server_key))

        self.assertTrue(scram.verify_password(password, v))
