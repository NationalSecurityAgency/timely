# General API

## Version Operation

```plaintext
echo "version" | nc <host> <port>
```

```http
GET /version HTTP/1.1
```

```http
POST /version HTTP/1.1
```

```json
{
  "operation" : "version"
}
```

Get the version of the Timely server. Returns a [Version](#version-response) response.

## Version Response

```plaintext
0.0.2
```

```http
HTTP/1.1 200 OK
0.0.2
```

```json
0.0.2
```

Represents the version of the Timely server. Contents are a single string with the version.

## Login Operation

```http
GET /login HTTP/1.1
```

```http
POST /login HTTP/1.1
{
  "username" : "user",
  "password" : "pass"
}
```

Timely uses Spring Security for user authentication. When successful, a HTTP Set-Cookie header is returned in the response with the name `TSESSIONID`. If a user does not log in, and anonymous access is enabled, then the user will only see data that is not marked. If a user does not log in, and anonymous access is disabled, then any operation that requires authentication will return an error. HTTP GET and POST methods are supported, the GET request is used when client SSL authentication is configured. When using the HTTP protocol, the `TSESSIONID` cookie should be sent along with the request. For the WebSocket protocol the client will need to add the `TSESSIONID` cookie value to the request in the `sessionId` property. If using the WebSocket protocol with anonymous access, then use a unique `sessionId` value for the duration of the client session. Returns a [Login](#login-response) response.

## Login Response

```http
HTTP/1.1 200 OK
Set-Cookie TSESSIONID=e480176b-b0d4-4c96-8437-55eef3f1f6d8; Max-Age=86400; Expires=Thu, 14 Jul 2016 13:57:20 GMT; Domain=localhost; Secure; HTTPOnly
```

```http
HTTP/1.1 401 Unauthorized
```

```http
HTTP/1.1 500 Internal Server Error
```

Login response contains an error or a HTTP cookie for use in subsequent calls.
