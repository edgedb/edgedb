import asyncio
import binascii
import codecs
import datetime
import functools
import http
import json
import os
import pathlib
import ssl
from typing import *

import acme.client
import aiohttp
import josepy
import yarl
from OpenSSL import crypto
from acme import challenges
from acme import errors
from acme import jws
from acme import messages
from cryptography import x509
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509 import oid

USER_AGENT = 'edgedb'
DIRECTORY = 'https://acme-staging-v02.api.letsencrypt.org/directory'
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


def get_private_key(filename: pathlib.Path) -> rsa.RSAPrivateKey:
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


class ChallengeUnavailable(Exception):
    pass


def select_tlsalpn01(order: messages.OrderResource) -> messages.ChallengeBody:
    for auth in order.authorizations:
        for challenge in auth.body.challenges:
            if isinstance(challenge.chall, challenges.TLSALPN01):
                return challenge

    raise ChallengeUnavailable('TLS-ALPN-01 challenge was not offered by the CA server.')


def gen_challenge_response_cert(
    response: challenges.TLSALPN01Response,
    domain_key: crypto.PKey,
    filename: str,
    domain: str,
) -> crypto.X509:
    """Generate a self signed certificate for TLSALPN01 validation.

    Reference: https://datatracker.ietf.org/doc/html/rfc8737#section-3

    This functionality is currently broken in acme==1.25.0
    """
    cert = crypto.X509()
    cert.set_serial_number(int(binascii.hexlify(os.urandom(16)), 16))
    cert.set_version(2)
    cert.get_subject().CN = domain
    cert.set_issuer(cert.get_subject())
    cert.add_extensions([
        crypto.X509Extension(
            b'subjectAltName',
            critical=False,
            value=b'DNS:' + domain.encode('utf-8'),
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

    with open(filename, 'wb') as f:
        f.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))

    return cert


class Client:
    USER_AGENT = 'edgedb'
    DIRECTORY = 'https://acme-staging-v02.api.letsencrypt.org/directory'

    def __init__(
        self,
        *,
        domain: str,
        email: str,
        acme_account_key_file: pathlib.Path,
        registration_filename: pathlib.Path,
        domain_key_filename: pathlib.Path,
        cert_filename: pathlib.Path,
        challenge_cert_filename: pathlib.Path,
    ):
        self._email = email
        self._domain = domain
        self._challenge_cert_filename = challenge_cert_filename
        self._cert_filename = cert_filename
        self._domain_key_filename = domain_key_filename
        self._acme_account_key_file = acme_account_key_file
        self._registration_filename = registration_filename

        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20),
            headers={'User-Agent':self.USER_AGENT},
        )

        self._reg = None
        self._dir = {}

    def __del__(self):
        try:
            asyncio.run(self._session.close())
        except Exception:
            pass

    async def _assert_status(self, resp: aiohttp.ClientResponse, status: int) -> None:
        if resp.status != status:
            msg = f'unexpected status {resp.status}: {await resp.text()}'
            raise NotImplementedError(msg)

    async def _get_directory(self) -> dict:
        if self._dir:
            return self._dir

        async with self._session.get(self.DIRECTORY) as resp:
            await self._assert_status(resp, 200)
            self._dir = await resp.json()

        return self._dir

    async def _get_nonce(self):
        directory = await self._get_directory()
        url = directory['newNonce']

        async with self._session.head(url) as resp:
            await self._assert_status(resp, 200)
            if 'Replay-Nonce' not in resp.headers:
                # todo
                raise NotImplementedError('missing Replay-Nonce header')
            nonce = resp.headers['Replay-Nonce']
            return josepy.decode_b64jose(nonce)

    async def _sign_payload(self, payload, url) -> str:
        kwargs = {}
        if not isinstance(payload, messages.NewRegistration):
            reg = await self._get_registration()
            kwargs['kid'] = reg['uri']

        if payload is None:
            # POST-as-GET
            # https://datatracker.ietf.org/doc/html/draft-ietf-acme-acme-15#section-6.3
            data = b''
        else:
            data = payload.json_dumps_pretty().encode('utf-8')

        return jws.JWS.sign(
            data,
            alg=josepy.RS256,
            nonce=await self._get_nonce(),
            url=url,
            key=self._account_key,
            **kwargs
        ).json_dumps_pretty()

    async def _post(self, url, payload):
        return self._session.post(
            url,
            data=await self._sign_payload(payload, url),
            headers={
                'Content-Type': 'application/jose+json',
                'User-Agent': self.USER_AGENT,
            },
        )

    async def _new_account(self) -> messages.RegistrationResource:
        directory = await self._get_directory()
        url = directory['newAccount']
        payload = messages.NewRegistration.from_data(
            email=self._email,
            terms_of_service_agreed=True,
        )

        async with await self._post(url, payload) as resp:
            await self._assert_status(resp, 201)
            if resp.status == 200 and 'Location' in resp.headers:
                # todo
                raise NotImplementedError('account already exists')
            tos = resp.links.get('terms-of-service', {}).get('url')
            if isinstance(tos, yarl.URL):
                tos = str(tos)
            reg = messages.RegistrationResource(
                body=messages.Registration.from_json(await resp.json()),
                uri=resp.headers.get('Location'),
                terms_of_service=tos,
            )

        return reg

    @functools.cached_property
    def _account_key(self):
        return josepy.JWKRSA(key=get_private_key(self._acme_account_key_file))

    @functools.cached_property
    def _domain_key(self):
        return get_private_key(self._domain_key_filename)

    async def _get_registration(self) -> messages.RegistrationResource:
        if self._reg:
            return self._reg

        try:
            with open(self._registration_filename, 'rt') as f:
                self._reg = messages.RegistrationResource.json_loads(f.read())
        except FileNotFoundError:
            self._reg = await self._new_account()
            with open(self._registration_filename, 'wt') as f:
                f.write(self._reg.json_dumps_pretty())

        return self._reg

    async def _new_order(self) -> messages.OrderResource:
        directory = await self._get_directory()
        url = directory['newOrder']
        payload = messages.NewOrder(identifiers=[messages.Identifier(
            typ=messages.IDENTIFIER_FQDN,
            value=self._domain,
        )])

        async with await self._post(url, payload) as resp:
            await self._assert_status(resp, 201)
            body = messages.Order.from_json(await resp.json())
            uri = resp.headers.get('Location')

        authorizations = []
        for url in body.authorizations:
            async with await self._post(url, None) as resp:
                await self._assert_status(resp, 200)
                authorizations.append(messages.AuthorizationResource(
                    body=messages.Authorization.from_json(await resp.json()),
                    uri=resp.headers.get('Location'),
                ))
            

        return messages.OrderResource(
            body=body,
            uri=uri,
            authorizations=authorizations,
        )


    def _gen_challenge_response_cert(
        self,
        response: challenges.TLSALPN01Response,
    ) -> None:
        """Generate a self signed certificate for TLSALPN01 validation.

        Reference: https://datatracker.ietf.org/doc/html/rfc8737#section-3

        This functionality is broken in acme==1.25.0
        """
        # subject = issuer = x509.Name([
        #     x509.NameAttribute(oid.NameOID.COMMON_NAME, self._domain)
        # ])

        # cert = (
        #     x509.CertificateBuilder()
        #     .subject_name(subject)
        #     .issuer_name(issuer)
        #     .public_key(self._domain_key.public_key())
        #     .serial_number(x509.random_serial_number())
        #     .not_valid_before(datetime.datetime.utcnow())
        #     .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=1))
        #     .add_extension(
        #         x509.SubjectAlternativeName([x509.DNSName(self._domain)]),
        #         critical=False,
        #     )
        #     .add_extension(
        #         x509.UnrecognizedExtension(
        #             x509.ObjectIdentifier('1.3.6.1.5.5.7.1.31'),
        #             value=b'. ' + response.h,
        #         ),
        #         critical=True,
        #     )
        #     .sign(self._domain_key, hashes.SHA256())
        # )

        # with open('challenge.cert.pem', 'wb') as f:
        #     f.write(cert.public_bytes(serialization.Encoding.PEM))
        gen_challenge_response_cert(
            response,
            crypto.PKey.from_cryptography_key(self._domain_key),
            self._challenge_cert_filename,
            self._domain,
        )

    async def _answer_challenge(
        self,
        challb: messages.ChallengeBody,
        response: challenges.ChallengeResponse,
    ) -> None:
        async with await self._post(challb.uri, response) as resp:
            await self._assert_status(resp, 200)

            try:
                uri = resp.links['up']['url']
            except KeyError as e:
                raise errors.ClientError('"up" link header missing') from e

            challr = messages.ChallengeResource(
                authzr_uri=str(uri),
                body=messages.ChallengeBody.from_json(await resp.json()),
            )

        if challr.uri != challb.uri:
            raise errors.UnexpectedUpdate(challr.uri)

    async def _poll_authorizations(
        self,
        order: messages.OrderResource,
        deadline: datetime.datetime,
    ) -> messages.OrderResource:
        responses = []
        for url in order.body.authorizations:
            while datetime.datetime.now() < deadline:
                async with await self._post(url, None) as resp:
                    await self._assert_status(resp, 200)
                    auth = messages.AuthorizationResource(
                        body=messages.Authorization.from_json(await resp.json()),
                        uri=resp.headers.get('Location', url),
                    )
                    if auth.body.status != messages.STATUS_PENDING:
                        responses.append(auth)
                        break
                    await asyncio.sleep(1)

        if len(responses) < len(order.body.authorizations):
            raise errors.TimeoutError()

        failed = []
        for auth in responses:
            if auth.body.status != messages.STATUS_VALID:
                for chall in auth.body.challenges:
                    if chall.error is not None:
                        failed.append(auth)

        if failed:
            raise errors.ValidationError(failed)

        return order.update(authorizations=responses)

    async def _finalize_order(
        self,
        order: messages.OrderResource,
        deadline:datetime.datetime,
    ) -> messages.OrderResource:
        csr = messages.CertificateRequest(csr=josepy.ComparableX509(
            crypto.X509Req.from_cryptography(
                x509.CertificateSigningRequestBuilder()
                .subject_name(x509.Name([x509.NameAttribute(
                    oid.NameOID.COMMON_NAME,
                    self._domain,
                )]))
                .add_extension(
                    x509.SubjectAlternativeName([x509.DNSName(self._domain)]),
                    critical=False,
                )
                .sign(self._domain_key, hashes.SHA256())
            )
        ))
        async with await self._post(order.body.finalize, csr) as resp:
            await self._assert_status(resp, 200)

        while datetime.datetime.now() < deadline:
            await asyncio.sleep(1)
            async with await self._post(order.uri, None) as resp:
                await self._assert_status(resp, 200)
                body = messages.Order.from_json(await resp.json())
                if body.error is not None:
                    raise errors.IssuanceError(body.error)
                if body.certificate is not None:
                    async with await self._post(body.certificate, None) as resp:
                        await self._assert_status(resp, 200)
                        order = order.update(
                            body=body,
                            fullchain_pem=await resp.text(),
                        )
                        return order

        raise errors.TimeoutError()

    async def _poll_and_finalize(
        self,
        order: messages.OrderResource,
    ) -> messages.OrderResource:
        deadline = datetime.datetime.now() + datetime.timedelta(seconds=90)
        order = await self._poll_authorizations(order, deadline)
        return await self._finalize_order(order, deadline)


    async def new_challenge(self):
        order = await self._new_order()
        challb = select_tlsalpn01(order)
        resp = challb.response(self._account_key)
        self._gen_challenge_response_cert(resp)
        return Challenge(
            client=self,
            order=order,
            challb=challb,
            response=resp
        )


class Challenge:
    def __init__(
        self,
        *,
        client: Client,
        order: messages.OrderResource,
        challb: messages.ChallengeBody,
        response: challenges.KeyAuthorizationChallengeResponse,
    ):
        self._client = client
        self._order = order
        self._challb = challb
        self._response = response
        self._finalized = False

    def sslctx(self):
        sslctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        sslctx.load_cert_chain(
            self._client._challenge_cert_filename,
            self._client._domain_key_filename,
        )
        sslctx.set_alpn_protocols(['acme-tls/1'])
        return sslctx

    async def start(self):
        await self._client._answer_challenge(self._challb, self._response)

    async def finalize(self) -> bool:
        if self._finalized:
            return False
        self._finalized = True
        
        try:
            order = await self._client._poll_and_finalize(self._order)
        except errors.ValidationError as e:
            print(e.failed_authzrs)
            raise
        with open(self._client._cert_filename, 'wt') as f:
            f.write(order.fullchain_pem)
        os.unlink(self._client._challenge_cert_filename)
        return True
