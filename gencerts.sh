#!/bin/bash
set -e -x
openssl genrsa -out server.key
openssl req -new -sha256 -key server.key -out server.csr -config cert_config.conf
openssl x509 -req -days 3650 \
	-in server.csr -out server.crt \
	-signkey server.key \
	-extensions ext -extfile cert_config.conf
