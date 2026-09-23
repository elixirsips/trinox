# TLS test fixture

A throwaway CA (`ca.pem`) and the server certificate it signed (`cert.pem` /
`key.pem`, valid for `localhost` and `127.0.0.1`) used by the HTTPS slice of the
test suite: `Trinox.MockTrino` serves the certificate, clients trust `ca.pem`.

These are test fixtures with no secrets in them, and they expire in 2046. The CA
key is not kept — regenerate the whole set if you ever need to:

```sh
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca-key.pem -out ca.pem -days 7300 \
  -subj "/CN=Trinox Test CA/O=Trinox Test Fixture" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"

openssl req -newkey rsa:2048 -nodes -keyout key.pem -out csr.pem \
  -subj "/CN=localhost/O=Trinox Test Fixture"

printf "subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n" > ext.cnf

openssl x509 -req -in csr.pem -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -out cert.pem -days 7300 -extfile ext.cnf

rm -f csr.pem ext.cnf ca-key.pem ca.srl
```
