---

title: Timely API Reference

language_tabs:
  - plaintext: TCP
  - http: HTTP
  - json-doc: WebSocket

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
Request:
echo "version" | nc <host> <port>

Response:
0.0.2
```

```http
GET /version HTTP/1.1
```

```http
POST /version HTTP/1.1
```

```http
HTTP/1.1 200 OK
0.0.2
```

```json-doc
// Request
{
  "operation" : "version"
}

// Response:
// TextWebSocketFrame containing a string (e.g. 0.0.2)
// or
// CloseWebSocketFrame on error
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

Timely utilizes Spring Security for user authentication. When successful, a HTTP Set-Cookie header is returned in the response with the name `TSESSIONID`. If a user does not log in, and anonymous access is enabled, then the user will only see data that is not marked. If a user does not log in, and anonymous access is disabled, then any operation that requires authentication will return an error. HTTP GET and POST methods are supported, the GET request is used when client SSL authentication is configured. When using the HTTP protocol, the `TSESSIONID` cookie should be sent along with the request. For the WebSocket protocol the client will need to add the `TSESSIONID` cookie value to the request in the `sessionId` property. If using the WebSocket protocol with anonymous access, then use a unique `sessionId` value for the duration of the client session.

# Time Series Operations

## Put

```plaintext
echo "put sys.cpu.user 1234567890 1.0 tag1=value1 tag2=value2" | nc <host> <port>
```

```http
POST /api/put HTTP/1.1
{
  "metric" : "sys.cpu.user",
  "timestamp" : 1234567890,
  "value" : 1.0,
  "tags" : [
    {
      "key" : "tag1",
      "value" : "value1"
    },
    {
      "key" : "tag2",
      "value" : "value2"
    }
  ]
}
```

```json
{
  "operation" : "put",
  "metric" : "sys.cpu.user",
  "timestamp" : 1234567890,
  "value" : 1.0,
  "tags" : [
    {
      "key" : "tag1",
      "value" : "value1"
    },
    {
      "key" : "tag2",
      "value" : "value2"
    }
  ]
}
```

The put operation is used for sending time series data to Timely. A time series metric is composed of the following items:

1. metric name
2. metric value
3. timestamp
4. optional set of tags

## Suggest

```http
GET /api/suggest?type=metrics&q=sys.cpu.u&max=30 HTTP/1.1
```

```http
POST /api/suggest HTTP/1.1
{
  "type" : "metrics",
  "q" : "sys.cpu",
  "max" : 30
}

HTTP/1.1 200 OK
[
  "sys.cpu.idle", 
  "sys.cpu.user", 
  "sys.cpu.wait"
]

HTTP/1.1 401 Unauthorized
-- Unknown or missing TSESSIONID cookie

HTTP/1.1 500 Internal Server Error
```

```json
{
  "operation" : "suggest",
  "sessionId" : "<value of TSESSIONID>",
  "type" : "metrics",
  "q" : "sys.cpu",
  "max" : 30
}

# TextWebSocketFrame Response:
[
  "sys.cpu.idle", 
  "sys.cpu.user", 
  "sys.cpu.wait"
]

# CloseWebSocketFrame when an error occurs
```

The suggest operation is used to find metrics, tag keys, or tag values that match the specified pattern. Input parameters:

1. type, one of metrics, tagk, tagv
2. q, the query string
3. max, the maximum number of results

## Lookup

## Query

## Aggregators

## Metrics

# Subscription Operations

## Create

## Add

## Remove

## Close

