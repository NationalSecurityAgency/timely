# Examples

## WebSocket Example

Timely will serve up files located in the `bin/webapp` directory. The source of the index.html file contains an example of how to use the [Subscription] (#subscription-api) Api with WebSockets. For this example to work with the standalone server, you first need to create some user certificates signed by the certificate authority that Timely is using. If you followed the instructions in the [SSL] (#ssl-setup) Setup section, then you just need to do the following:

> Note: When prompted for input parameters during the certificate request creation, enter the value `example.com` for the Common Name. In the default `conf/security.xml` file it is looking for this value in X509 login requests.

`
openssl genrsa -out timely-user.key 4096
`

`
openssl req -new -key timely-user.key -sha256 -nodes -out timely-user.csr
`

`
openssl x509 -req -in timely-user.csr -CA CA.pem -CAkey CA.key -CAcreateserial -out timely-user.crt -days 365
`

`
openssl pkcs12 -export -out timely-user.p12 -inkey timely-user.key -in timely-user.crt -certfile CA.pem
`

Next, load the timely-user.p12 file into your web browser. 
Then, start the standalone server.
Finally, navigate to `https://localhost:54322/webapp/index.html` in your web browser.
