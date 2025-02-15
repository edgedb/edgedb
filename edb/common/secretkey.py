#
# This source file is part of the EdgeDB open source project.
#
# Copyright EdgeDB Inc. and the EdgeDB authors.
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

from __future__ import annotations
from typing import Iterable

import pathlib
import uuid

from datetime import datetime, timedelta


def generate_tls_cert(
    tls_cert_file: pathlib.Path,
    tls_key_file: pathlib.Path,
    listen_hosts: Iterable[str]
) -> None:
    from cryptography import x509
    from cryptography.hazmat import backends
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509 import oid

    backend = backends.default_backend()
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=backend
    )
    subject = x509.Name(
        [x509.NameAttribute(oid.NameOID.COMMON_NAME, "Gel Server")]
    )
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .public_key(private_key.public_key())
        .serial_number(int(uuid.uuid4()))
        .issuer_name(subject)
        .not_valid_before(
            datetime.today() - timedelta(days=1)
        )
        .not_valid_after(
            datetime.today() + timedelta(weeks=1000)
        )
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName(name) for name in listen_hosts
                    if name not in {'0.0.0.0', '::'}
                ]
            ),
            critical=False,
        )
        .sign(
            private_key=private_key,
            algorithm=hashes.SHA256(),
            backend=backend,
        )
    )
    with tls_cert_file.open("wb") as f:
        f.write(certificate.public_bytes(encoding=serialization.Encoding.PEM))
    tls_cert_file.chmod(0o644)
    with tls_key_file.open("wb") as f:
        f.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
    tls_key_file.chmod(0o600)
