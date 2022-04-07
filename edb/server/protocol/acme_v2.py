import binascii
import codecs
import functools
import http
import json
import os
import ssl
from typing import *

import acme.client
import josepy
from OpenSSL import crypto
from acme import challenges
from acme import errors
from acme import messages
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

EMAIL = 'wifiv35446@procowork.com'
USER_AGENT = 'edgedb'
DIRECTORY = 'https://acme-staging-v02.api.letsencrypt.org/directory'
DOMAIN = 'edgedb.fmoor.me'
KEY_SIZE = 4096


def take(data, length):
    if (length < 0) or (len(data) < length):
        raise IndexError('index out of range')

    return data[:length], data[length:]


def take_int8(data):
    return data[0], data[1:]


def take_int16(data):
    if len(data) < 2:
        raise IndexError('index out of range')

    return int.from_bytes(data[:2], 'big'), data[2:]


def parse_alpn_protocols(data):
    _, data = take(data, 2)  # data length
    protocols = []
    while len(data):
        length, data = take_int8(data)
        protocol, data = take(data, length)
        protocols.append(protocol.decode('utf-8'))

    return protocols


def is_tls_alpn_01_challenge(data):
    try:
        record_type, data = take_int8(data)
        if record_type != 0x16:  # handshake record
            return False

        _, data = take(data, 2)  # protocol version
        _, data = take(data, 2)  # data length

        message_type, data = take_int8(data)
        if message_type != 0x01:  # client hello message
            return False

        _, data = take(data, 3)  # data length

        _, data = take(data, 2)  # client version
        _, data = take(data, 32)  # client random
        length, data = take_int8(data)
        _, data = take(data, length)  # session id
        length, data = take_int16(data)
        _, data = take(data, length)  # cipher suits
        length, data = take_int8(data)
        _, data = take(data, length)  # compression methods
        _, data = take(data, 2)  # data length

        while len(data):
            ext_type, data = take_int16(data)
            length, data = take_int16(data)
            ext_bts, data = take(data, length)

            if ext_type == 0x10:
                return parse_alpn_protocols(ext_bts) == ['acme-tls/1']

        return False

    except IndexError:
        return False


def get_private_key(filename: str) -> rsa.RSAPrivateKey:
    try:
        with open(filename, 'rb') as f:
            key = serialization.load_pem_private_key(f.read(), password=None)
            assert isinstance(key, rsa.RSAPrivateKey)
    except FileNotFoundError:
        key = rsa.generate_private_key(public_exponent=65537, key_size=KEY_SIZE)
        pem = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())
        with open(filename, 'wb') as f:
            f.write(pem)

    return key


def get_account_key(domain: str) -> josepy.JWKRSA:
    filename = f'account.private.key.pem'
    return josepy.JWKRSA(key=get_private_key(filename))


def get_registration():
    try:
        with open('registration.json', 'rt') as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def get_domain_key():
    try:
        with open('domain.private.key.pem', 'rb') as f:
            key = crypto.load_privatekey(crypto.FILETYPE_PEM, f.read())
    except FileNotFoundError:
        key = crypto.PKey()
        key.generate_key(crypto.TYPE_RSA, KEY_SIZE)
        with open('domain.private.key.pem', 'wb') as f:
            f.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, key))

    return key


def new_csr(domain_key: crypto.PKey, domain: str) -> crypto.X509Req:
    csr = crypto.X509Req()
    csr.add_extensions([
        crypto.X509Extension(
            b'subjectAltName',
            critical=False,
            value=f'DNS:{domain}'.encode('ascii')
        ),
    ])
    csr.set_pubkey(domain_key)
    csr.set_version(2)
    csr.sign(domain_key, 'sha256')

    return csr


class ChallengeUnavailable(Exception):
    pass


def select_tlsalpn01(order: messages.OrderResource) -> messages.ChallengeBody:
    for auth in order.authorizations:
        for challenge in auth.body.challenges:
            if isinstance(challenge.chall, challenges.TLSALPN01):
                return challenge

    raise ChallengeUnavailable('TLS-ALPN-01 challenge was not offered by the CA server.')


def gen_self_signed_cert(
    response: challenges.TLSALPN01Response,
    domain_key: crypto.PKey
) -> crypto.X509:
    """Generate a self signed certificate for TLSALPN01 validation.

    Reference: https://datatracker.ietf.org/doc/html/rfc8737#section-3

    This functionality is currently broken in acme==1.25.0
    """
    cert = crypto.X509()
    cert.set_serial_number(int(binascii.hexlify(os.urandom(16)), 16))
    cert.set_version(2)
    cert.get_subject().CN = DOMAIN
    cert.set_issuer(cert.get_subject())
    cert.add_extensions([
        crypto.X509Extension(
            b'subjectAltName',
            critical=False,
            value=b'DNS:' + DOMAIN.encode('utf-8'),
        ),
        crypto.X509Extension(
            b'1.3.6.1.5.5.7.1.31',
            critical=True,
            value=b'DER:04:20:' + codecs.encode(response.h, 'hex'),
        )
    ])
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(7 * 24 * 60 * 60)
    cert.set_pubkey(domain_key)
    cert.sign(domain_key, "sha256")

    with open('challenge.cert.pem', 'wb') as f:
        f.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))

    return cert


def issue_certificate(domain: str, server):
    account_key = get_account_key(domain)
    print(account_key)
    registration = get_registration()
    network = acme.client.ClientNetwork(
        account_key,
        user_agent=USER_AGENT,
        account=registration,
    )
    client = acme.client.ClientV2(
        messages.Directory.from_json(network.get(DIRECTORY).json()),
        network
    )

    if registration is None:
        new_reg = messages.NewRegistration.from_data(
            email=EMAIL,
            terms_of_service_agreed=True,
        )
        registration = client.new_account(new_reg)
        with open('registration.json', 'wt') as f:
            f.write(registration.json_dumps_pretty())

    # Create domain private key
    domain_key = get_domain_key()

    csr = new_csr(domain_key, DOMAIN)
    csr_pem = crypto.dump_certificate_request(crypto.FILETYPE_PEM, csr)
    order = client.new_order(csr_pem)
    challb = select_tlsalpn01(order)

    resp = challb.response(client.net.key)
    cert = gen_self_signed_cert(resp, domain_key)
    return ACMEChallenge(client, order, challb, resp, domain_key, cert, server)


class ACMEChallenge:

    def __init__(self, client, order, challb, resp, key, cert, server):
        self._client = client
        self._order = order
        self._challb = challb
        self._resp = resp
        self._cert = cert
        self._key = key
        self._server = server
        self._finalized = False

    def sslctx(self):
        sslctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        sslctx.load_cert_chain(
            certfile='challenge.cert.pem',
            keyfile='domain.private.key.pem'
        )
        sslctx.set_alpn_protocols(['acme-tls/1'])
        return sslctx

    def start(self):
        self._client.answer_challenge(self._challb, self._resp)

    def finalize(self):
        if self._finalized:
            return

        self._finalized = True
        try:
            x = self._client.poll_and_finalize(self._order)
            with open('cert.pem', 'wt') as f:
                f.write(x.fullchain_pem)
            print('certificate saved!')
            self._server.init_tls('cert.pem', 'domain.private.key.pem', True)
            return x
        except errors.ValidationError as e:
            print(e.failed_authzrs)
            print(e)
