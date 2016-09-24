#!/bin/bash

WORKING="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

. ${WORKING}/deploy-env.sh

PASSPREFIX=pass:
KEYPASS=password
PASSWORD=$PASSPREFIX$KEYPASS

if [ ! -d ${SRC_DIR}/pki ]; then
	mkdir ${SRC_DIR}/pki
	pushd ${SRC_DIR}/pki

	#Create a private key

	openssl genrsa -des3 -out CA.key -passout ${PASSWORD} 4096

	#Create a certificate request using the private key

	openssl req -x509 -new -key CA.key -sha256 -nodes -days 365 -out CA.pem  -passin ${PASSWORD} -subj '/C=US/ST=Confusion/L=Here/O=Timely/OU=Server/CN=localhost/emailAddress=noreply@localhost/subjectAltName=DNS.1=127.0.0.1'
	
	#Create your SSL material for Grafana

	#Create the private key for the Grafana server

	openssl genrsa -out grafana.key -passout ${PASSWORD} 4096

	#Generate a certificate signing request (CSR) with our Grafana private key

	openssl req -new -key grafana.key -sha256 -nodes -out grafana.csr -subj '/C=US/ST=Confusion/L=Here/O=Timely/OU=Server/CN=localhost/emailAddress=noreply@localhost/subjectAltName=DNS.1=127.0.0.1'
	
	#Use the CSR and the CA to create a certificate for the server (a reply to the CSR)

	openssl x509 -req -in grafana.csr -CA CA.pem -CAkey CA.key -CAcreateserial -out grafana.crt -days 365 -passin ${PASSWORD}

	#Create your SSL material for Timely

	#Create the private key for the Timely server

	openssl genrsa -out timely.key 4096
	
	#Generate a certificate signing request (CSR) with our Timely private key

	openssl req -new -key timely.key -sha256 -nodes -subj '/C=US/ST=Confusion/L=Here/O=Timely/OU=Server/CN=localhost/emailAddress=noreply@localhost/subjectAltName=DNS.1=127.0.0.1' > timely.csr

	#Use the CSR and the CA to create a certificate for the server (a reply to the CSR)

	openssl x509 -req -in timely.csr -CA CA.pem -CAkey CA.key -CAcreateserial -out timely.crt -days 365 -passin ${PASSWORD}
	
	#Convert the private key to pkcs#8 format

	openssl pkcs8 -topk8 -inform PEM -outform PEM -in timely.key -out timely-pkcs8.key -passout ${PASSWORD}
	popd
fi

