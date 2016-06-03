# Standalone Quick Start
---

1. Install latest version of Grafana
2. Modify grafana configuration:
  1. protocol = https
  2. http_addr = localhost
  3. cert_file= <path to PEM encoded certificate file>
  4. key_file= <path to PEM encoded un-encrypted private key file>
3. Start Grafana
4. Build Timely
  1. set JAVA_HOME to JDK 8
  2. mvn clean package
5. Untar Timely server distribution
6. Modify timely-standalone.properties:
  1. Set server side SSL certification information
    1. timely.ssl.use.generated.keypair=true
    OR
    2. timely.ssl.use.generated.keypair=false
    3. timely.ssl.certificate.file=```<path to PEM encoded certificate file>```
    4. timely.ssl.key.file=```<path to PKCS8 PEM encoded private key file>```
    5. timely.ssl.key.pass=```<private key password>```
    6. timely.ssl.use.openssl=true if openssl installed locally, else false
    7. timely.ssl.use.ciphers=```<list of ciphers to use if you don not want to use the default>```
  2. Set Timely domain information, this is used for the HTTP Cookie
    1. timely.http.address=localhost
  3. Set Grafana login address, used for HTTP redirect after login
    1. grafana.http.address=https://localhost:3000/login
  4. Set Anonymous access for Timely
    1. timely.allow.anonymous.access=```<true or false>```
7. Start the Timely standalone server
  1. cd bin; ./timely-standalone.sh
8. Insert test data
  1. cd bin; ./insert-test-data.sh
9. Add the Timely datasource to Grafana
  1. Login to Grafana, go to DataSources
  2. Click Add data source
  3. Enter the following information:
    1. Name: Timely Standalone
    2. Type: OpenTSDB
    3. Url: https://localhost:54322
    4. Access: direct
    5. With Credentials: checked
    6. Version: 2.1
    7. Resolution: millisecond
    8. Click ```Save & Test```". Note that you will get an error, but it will save.
10. Import Standalone Test dashboard


# Notes on Security Options
---

1. If anonymous access is disabled, then users will only be able to see unlabeled data
2. If anonymous access is enabled, then users must login before Grafana will work.
  1. For client certificates users should perform a GET request to the /login endpoint
  2. For basic authentication users should perform a POST request to the /login endpoint
    1. This can be done using Poster or HttpRequester with the content-type of "application/json" and the following content:
    ```    
			{
			  "username": "<>",
			  "password": "<>"
			}
    ```
3. Access control is configured in conf/security.xml and supports basic auth and client certificates. Upon a successful login the response will include a HTTP Session Cookie. Once this is set, access to Timely from Grafana should work.
