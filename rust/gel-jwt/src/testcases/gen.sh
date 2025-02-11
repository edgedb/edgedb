#!/bin/bash
OLD_PWD=$(pwd)
trap "cd $OLD_PWD" EXIT

cd $(dirname $0)

rm *.pem
rm *.asn1.txt
# Generate RSA 2048 private keys in PKCS8 and PKCS1 formats
openssl genpkey -algorithm RSA -out rsa2048-prv-pkcs8.pem -pkeyopt rsa_keygen_bits:2048
openssl rsa -in rsa2048-prv-pkcs8.pem -out rsa2048-prv-pkcs1.pem -outform PEM -traditional

# Generate RSA 2048 public keys in PKCS8 and PKCS1 formats
openssl rsa -in rsa2048-prv-pkcs8.pem -out rsa2048-pub-pkcs8.pem -outform PEM -pubout
openssl rsa -in rsa2048-prv-pkcs1.pem -out rsa2048-pub-pkcs1.pem -outform PEM -pubout -RSAPublicKey_out

# Generate prime256v1 private keys in SEC1 and PKCS8 formats
openssl ecparam -name prime256v1 -genkey -noout -out prime256v1-prv-sec1.pem
openssl pkcs8 -topk8 -inform PEM -outform PEM -in prime256v1-prv-sec1.pem -out prime256v1-prv-pkcs8.pem -nocrypt

# Generate prime256v1 public keys in various formats
# SPKI format (compressed)
openssl ec -in prime256v1-prv-sec1.pem -pubout -out prime256v1-pub-spki.pem -conv_form compressed

# SPKI format (uncompressed) 
openssl ec -in prime256v1-prv-sec1.pem -pubout -out prime256v1-pub-spki-uncompressed.pem -conv_form uncompressed

# Raw EC point formats (display only)
echo "Compressed public key point:"
openssl ec -in prime256v1-prv-sec1.pem -text -noout -conv_form compressed | grep 'pub:' -A 2

echo "Uncompressed public key point:"
openssl ec -in prime256v1-prv-sec1.pem -text -noout -conv_form uncompressed | grep 'pub:' -A 2

# For each file, run asn1parse and save the output to a file with the same name but -asn1.txt extension
for file in *.pem; do
    # First do basic asn1parse
    openssl asn1parse -dump -in $file > ${file%.pem}-asn1.txt
    
    # Look for any BITSTRING or OCTET STRING fields and parse them
    while read -r line; do
        if [[ $line =~ "BIT STRING" ]] || [[ $line =~ "OCTET STRING" ]]; then
            # Extract offset and length
            offset=$(echo $line | cut -d':' -f1 | tr -d ' ')
            # Parse the contents
            if [[ $line =~ "BIT STRING" ]]; then
                echo "-- BITSTRING at offset $offset --" >> ${file%.pem}-asn1-tmp.txt
            else
                echo "-- OCTET STRING at offset $offset --" >> ${file%.pem}-asn1-tmp.txt
            fi
            openssl asn1parse -dump -in $file -strparse $offset >> ${file%.pem}-asn1-tmp.txt 2>/dev/null
        fi
    done < ${file%.pem}-asn1.txt
    cat ${file%.pem}-asn1-tmp.txt >> ${file%.pem}-asn1.txt 2>/dev/null
    rm ${file%.pem}-asn1-tmp.txt 2>/dev/null
done
