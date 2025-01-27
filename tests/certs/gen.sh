#!/bin/sh
set -e -x

# Save original directory
ORIG_DIR=$(pwd)

# Ensure we return to original directory even on error
trap 'cd "$ORIG_DIR"' EXIT

cd tests/certs

rm index.txt*
rm serial.txt
rm crlnumber.txt

# Root CA
openssl genrsa -out ca.key.pem 4096

# Create empty database files required by ca.conf
touch index.txt
echo "01" > serial.txt
echo "01" > crlnumber.txt

openssl req -new -x509 -key ca.key.pem -out ca.cert.pem -days 7300 -config ca.conf -batch -subj "/C=US/ST=California/L=San Francisco/O=EdgeDB Inc./OU=EdgeDB tests/CN=EdgeDB test root ca/emailAddress=hello@edgedb.com"

# Server cert
openssl genrsa -out server.key.pem 4096
openssl req -new -key server.key.pem -out server.csr.pem -subj "/C=US/ST=California/L=San Francisco/O=EdgeDB Inc./OU=EdgeDB tests/CN=localhost/emailAddress=hello@edgedb.com" -batch
openssl x509 -req -in server.csr.pem -CA ca.cert.pem -CAkey ca.key.pem -CAcreateserial -out server.cert.pem -days 7300 -extensions v3_req -extfile ca.conf

# Client CA
openssl genrsa -out client_ca.key.pem 4096
openssl req -new -x509 -key client_ca.key.pem -out client_ca.cert.pem -days 7300 -subj "/C=US/ST=California/L=San Francisco/O=EdgeDB Inc./OU=EdgeDB tests/CN=EdgeDB test client CA/emailAddress=hello@edgedb.com" -batch

# Client cert
openssl genrsa -out client.key.pem 4096
openssl req -new -key client.key.pem -out client.csr.pem -subj "/C=US/ST=California/L=San Francisco/O=EdgeDB Inc./OU=EdgeDB tests/CN=ssl_user/emailAddress=hello@edgedb.com" -batch
openssl x509 -req -in client.csr.pem -CA client_ca.cert.pem -CAkey client_ca.key.pem -CAcreateserial -out client.cert.pem -days 7300

# Password protected client key
openssl rsa -aes256 -in client.key.pem -out client.key.protected.pem -passout pass:secret1234

# Revoke server cert and generate CRL
openssl ca -config ca.conf -revoke server.cert.pem -keyfile ca.key.pem -cert ca.cert.pem -batch -md sha256
openssl ca -config ca.conf -gencrl -keyfile ca.key.pem -cert ca.cert.pem -out ca.crl.pem -batch -md sha256
