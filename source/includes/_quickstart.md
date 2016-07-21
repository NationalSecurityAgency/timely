# Standalone Quick Start

1. Install latest version of Grafana (3.0.4 at time of writing)
2. Create a grafana configuration file (e.g: `conf/settings.ini`)
  1. `protocol = https`
  2. `http_addr = localhost`
  3. `cert_file= <path to PEM encoded certificate file>`
  4. `key_file= <path to PEM encoded un-encrypted private key file>`
3. Start Grafana (`./bin/grafana-server -c conf/settings.ini`)
  1. Visit `https://localhost:3000` to verify Grafana is working
4. Build Timely
  1. set `JAVA_HOME` to point to a JDK 8 installation
  2. cd into the server directory and run `mvn clean package`
5. Untar Timely server distribution (found in `target/timely-server-(VERSION)-SNAPSHOT-dist.tar.gz`)
6. Modify `timely-standalone.properties`:
  1. Use generated server side SSL certificates
    1. `timely.ssl.use.generated.keypair=true`
  2. or, use your own SSL certificates - see [SSL] (#ssl-setup) Setup
    1. `timely.ssl.use.generated.keypair=false`
    2. `timely.ssl.certificate.file=<path to PEM encoded certificate file>`
    3. `timely.ssl.key.file=<path to PKCS8 PEM encoded private key file>`
    4. `timely.ssl.key.pass=<private key password>`
    5. `timely.ssl.use.openssl=true` (if openssl installed locally, else false)
    6. `timely.ssl.use.ciphers=<list of ciphers>` (comma-delimited list of ciphers to use if you do not want to use the default)
  3. Set Timely domain information, this is used for the HTTP Cookie
    1. `timely.http.address=localhost`
  4. Set Grafana login address, used for HTTP redirect after login
    1. `grafana.http.address=https://localhost:3000/login`
  5. Set Anonymous access for Timely
    1. `timely.allow.anonymous.access=<true or false>`
7. Start the Timely standalone server
  1. `cd bin; ./timely-standalone.sh`
8. Insert test data
  1. `cd bin; ./insert-test-data.sh`
9. Add the Timely datasource to Grafana
  1. Go to `https://localhost:54322/api/metrics` in your browser and accept the certificate
  2. Login to Grafana, go to 'DataSources'
  3. Click 'Add data source'
  4. Enter the following information:
    1. Name: `Timely Standalone`
    2. Type: `OpenTSDB`
    3. Url: `https://localhost:54322`
    4. Access: `direct`
    5. With Credentials: `checked`
    6. Version: `<=2.1`
    7. Resolution: `millisecond`
    8. Click `Save & Test`. Note that you will get an error, but it will save.
10. Import Standalone Test dashboard into Grafana.


## Notes on Security Options

1. If anonymous access is disabled, then users will only be able to see unlabeled data.
2. If anonymous access is enabled, then users must login before Grafana will work.
3. Access control is configured in `conf/security.xml` and supports basic auth and SSL client certificates. Upon a successful login the response will include a HTTP Session Cookie. Once this is set, access to Timely from Grafana should work.
  1. For SSL client certificate auth, users should perform a GET request to the `/login` endpoint over HTTPS. The default `conf/security.xml` specifies a user `example.com` with authorizations `D`,`E`,`F`. The username will be extracted from the certificate's `CN` using the `x509PrincipalExtractor` bean.
  2. For basic password authentication users should perform a POST request to the `/login` endpoint.
    1. This can be done using Poster or HttpRequester with the content-type of "application/json" and the following content:
    ```    
			{
			  "username": "<>",
			  "password": "<>"
			}
    ```
    2. The default `conf/security.xml` specifies a user `test` with password `test1` that has the authorizations for `A`,`B`,`C`.
4. The user specified in `timely.user` must have authorizations compatible with the visibility values on your data to be able to return that data to a client. Use `setauths` in the Accumulo shell to configure this.
5. `insert-test-data.sh` will insert some data with visibilities, specifically `sys.eth0.rx.*` will have `A`,`B` or `C` and `sys.eth0.tx.*` will have `D`,`E` or `F`.

