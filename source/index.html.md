---

title: Timely API Reference

language_tabs:
  - plaintext: tcp
  - http: https
  - json: websocket

toc_footers:
  - <a href='https://github.com/tripit/slate'>Documentation Powered by Slate</a>

search: true

---

# Transports

The Timely server supports clients sending requests over different protocols. Not all operations are supported over all protocols. The table below summarizes the protocols supported for each operation. The API documention is organized into sections that describe the general, time series, and subscription operations. 

Operation | TCP | HTTPS | WebSocket
----------|-----|-------|----------
Version     | X | X | X
Login       |   | X | 
Put         | X | X | X
Suggest     |   | X | X
Lookup      |   | X | X
Query       |   | X | X
Aggregators |   | X | X
Metrics     |   | X | X
Create      |   |   | X
Add         |   |   | X
Remove      |   |   | X
Close       |   |   | X

# General Operations

## Version

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

Get the version of the Timely server.

## Login

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

Timely utilizes Spring Security for user authentication. When successful, a HTTP Set-Cookie header is returned in the response with the name `TSESSIONID`. If a user does not log in, and anonymous access is enabled, then the user will only see data that is not marked. If a user does not log in, and anonymous access is disabled, then any operation that requires authentication will return an error. HTTP GET and POST methods are supported, the GET request is used when client SSL authentication is configured.

Response codes:

Code | Reason
-----|-------
200 | Successful login
401 | User credentials incorrect or unknown
500 | Internal error condition

# Time Series Operations

## Put

```plaintext
```

```http
```

```json
```

The put operation is used for sending time series data to Timely.

## Suggest

## Lookup

## Query

## Aggregators

## Metrics

# Subscription Operations

## Create

## Add

## Remove

## Close

