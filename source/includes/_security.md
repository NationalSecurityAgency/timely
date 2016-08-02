# Security

## Securing the Transport

Timely only exposes two operations over a non-secure transport, one to the version of the server and the other to insert data. All other operations are protected by either HTTPS or WSS protocols. The server [configuration] (#configuration) allows the user to change many of the security settings such as: CORS origins, specific SSL/TLS ciphers, duration of the HTTP session cookie, and more.

### HTTP Strict Transport Security

Timely returns a [HTTP Strict Transport Security] (https://tools.ietf.org/html/rfc6797) header to callers that attempt to use HTTP. The max age for this header can be set in the configuration.

## Securing Data

Timely allows users to optionally label their data using Accumulo column visibility expressions. To enable this feature, users should put the expressions in a tag named `viz`. Timely will [flatten] (http://accumulo.apache.org/1.7/apidocs/org/apache/accumulo/core/security/ColumnVisibility.html#flatten%28%29) this expression and store it in the column visibility of the data point in the metrics table. Column visibilities are *not* stored in the meta table, so anyone can see metric names, tag names, and tag values. Additionally, column visibilities are not returned with the data.

> With anonymous access users that have not logged in will only see unlabeled data.

Timely uses Spring Security to configure user authentication and user role information. Users must call the `/login` endpoint for authentication and Timely will respond by setting a HTTP cookie with a session id. When anonymous access is disabled, a call to any operation that requires the session id will fail. When anonymous access is enabled, these calls will succeed but only unlabeled data will be returned. When using Grafana, it must be configured to use https also. For more information see the [Quick Start] (#standalone-quick-start) documentation.
