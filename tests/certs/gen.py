#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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

import datetime
import os

from cryptography import x509
from cryptography.hazmat import backends
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509 import oid


def _new_cert(issuer=None, is_issuer=False, serial_number=None, **subject):
    backend = backends.default_backend()
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=4096, backend=backend
    )
    public_key = private_key.public_key()
    subject = x509.Name(
        [
            x509.NameAttribute(getattr(oid.NameOID, key.upper()), value)
            for key, value in subject.items()
        ]
    )
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .public_key(public_key)
        .serial_number(serial_number or int.from_bytes(os.urandom(8), "big"))
    )
    if issuer:
        issuer_cert, signing_key = issuer
        builder = (
            builder.issuer_name(issuer_cert.subject)
            .not_valid_before(issuer_cert.not_valid_before)
            .not_valid_after(issuer_cert.not_valid_after)
        )
        aki_ext = x509.AuthorityKeyIdentifier(
            key_identifier=issuer_cert.extensions.get_extension_for_class(
                x509.SubjectKeyIdentifier
            ).value.digest,
            authority_cert_issuer=[x509.DirectoryName(issuer_cert.subject)],
            authority_cert_serial_number=issuer_cert.serial_number,
        )
    else:
        signing_key = private_key
        builder = (
            builder.issuer_name(subject)
            .not_valid_before(
                datetime.datetime.today() - datetime.timedelta(days=1)
            )
            .not_valid_after(
                datetime.datetime.today() + datetime.timedelta(weeks=1000)
            )
        )
        aki_ext = x509.AuthorityKeyIdentifier.from_issuer_public_key(
            public_key
        )
    if is_issuer:
        builder = (
            builder.add_extension(
                x509.BasicConstraints(ca=True, path_length=None),
                critical=False,
            )
            .add_extension(
                x509.SubjectKeyIdentifier.from_public_key(public_key),
                critical=False,
            )
            .add_extension(
                aki_ext,
                critical=False,
            )
        )
    else:
        builder = (
            builder.add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=True,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=False,
                    crl_sign=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=False,
            )
            .add_extension(
                x509.BasicConstraints(ca=False, path_length=None),
                critical=False,
            )
            .add_extension(
                x509.ExtendedKeyUsage([oid.ExtendedKeyUsageOID.SERVER_AUTH]),
                critical=False,
            )
            .add_extension(
                x509.SubjectAlternativeName([x509.DNSName("localhost")]),
                critical=False,
            )
            .add_extension(
                x509.SubjectKeyIdentifier.from_public_key(public_key),
                critical=False,
            )
            .add_extension(
                aki_ext,
                critical=False,
            )
        )
    certificate = builder.sign(
        private_key=signing_key,
        algorithm=hashes.SHA256(),
        backend=backend,
    )
    return certificate, private_key


def _write_cert(path, cert_key_pair, password=None):
    certificate, private_key = cert_key_pair
    if password:
        encryption = serialization.BestAvailableEncryption(password)
    else:
        encryption = serialization.NoEncryption()
    with open(path + ".key.pem", "wb") as f:
        f.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=encryption,
            )
        )
    with open(path + ".cert.pem", "wb") as f:
        f.write(
            certificate.public_bytes(
                encoding=serialization.Encoding.PEM,
            )
        )


def new_ca(path, **subject):
    cert_key_pair = _new_cert(is_issuer=True, **subject)
    _write_cert(path, cert_key_pair)
    return cert_key_pair


def new_cert(
    path, ca_cert_key_pair, password=None, is_issuer=False, **subject
):
    cert_key_pair = _new_cert(
        issuer=ca_cert_key_pair, is_issuer=is_issuer, **subject
    )
    _write_cert(path, cert_key_pair, password)
    return cert_key_pair


def new_crl(path, issuer, cert):
    issuer_cert, signing_key = issuer
    revoked_cert = (
        x509.RevokedCertificateBuilder()
        .serial_number(cert[0].serial_number)
        .revocation_date(datetime.datetime.today())
        .build()
    )
    builder = (
        x509.CertificateRevocationListBuilder()
        .issuer_name(issuer_cert.subject)
        .last_update(datetime.datetime.today())
        .next_update(datetime.datetime.today() + datetime.timedelta(days=1))
        .add_revoked_certificate(revoked_cert)
        .add_extension(x509.CRLNumber(1), critical=True)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(
            issuer_cert.public_key()), critical=True)
    )
    crl = builder.sign(private_key=signing_key, algorithm=hashes.SHA256())
    with open(path + ".crl.pem", "wb") as f:
        f.write(crl.public_bytes(encoding=serialization.Encoding.PEM))


def main():
    ca = new_ca(
        "ca",
        country_name="US",
        state_or_province_name="California",
        locality_name="San Francisco",
        organization_name="EdgeDB Inc.",
        organizational_unit_name="EdgeDB tests",
        common_name="EdgeDB test root ca",
        email_address="hello@edgedb.com",
    )
    server = new_cert(
        "server",
        ca,
        country_name="US",
        state_or_province_name="California",
        organization_name="EdgeDB Inc.",
        organizational_unit_name="EdgeDB tests",
        common_name="localhost",
        email_address="hello@edgedb.com",
        serial_number=4096,
    )
    new_crl('server', ca, server)


if __name__ == "__main__":
    main()
