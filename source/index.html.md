---

title: Timely API Reference

language_tabs:
  - plaintext: TCP
  - http: HTTP
  - json: WebSocket

toc_footers:
  - <a href='https://github.com/tripit/slate'>Documentation Powered by Slate</a>

includes:
  - security
  - server
  - developer

search: true

---

Timely is a time series database application that provides secure access to time series data. Timely is written in Java and designed to work with [Apache Accumulo] (http://accumulo.apache.org/) and [Grafana] (http://www.grafana.org).

# Responses

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

## Suggest Response

```http
HTTP/1.1 200 OK
[
  "sys.cpu.idle", 
  "sys.cpu.user", 
  "sys.cpu.wait"
]
```

```http
HTTP/1.1 401 Unauthorized
```

```http
HTTP/1.1 500 Internal Server Error
```

```json
[
  "sys.cpu.idle", 
  "sys.cpu.user", 
  "sys.cpu.wait"
]
```

The response to the suggest operation contains a list of suggestions based on the request parameters or an error. An HTTP 401 error will be returned on a missing or unknown `TSESSIONID` cookie. A TextWebSocketFrame will be returned on a successful WebSocket request, otherwise a CloseWebSocketFrame will be returned. 

## Lookup Response

```http
HTTP/1.1 200 OK
{
  "type":"LOOKUP",
  "metric":"sys.cpu.idle",
  "tags":{
    "tag3":"*"
  },
  "limit":25,
  "time":49,
  "totalResults":1,
  "results":[
    {
      "metric":null,
      "tags":{
        "tag3":"value3"
      },
      "tsuid":null
    }
  ]
}
```

```json
{
  "type":"LOOKUP",
  "metric":"sys.cpu.idle",
  "tags":{
    "tag3":"*"
  },
  "limit":25,
  "time":49,
  "totalResults":1,
  "results":[
    {
      "metric":null,
      "tags":{
        "tag3":"value3"
      },
      "tsuid":null
    }
  ]
}
```

The response to the lookup operation contains a set of metric names and tag set information that matches the request parameters. Response attributes are:

Attribute | Type | Description
----------|------|------------
type | string | constant value of LOOKUP
metric | string | copy of the metric input parameter
tags | map | copy of the tags input parameter
limit | int | copy of the limit input parameter
time | int | operation duration in ms
totalResults | int | number of results
results | array | array of result objects that contain matching metric names and tag sets

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

Note that websocket port is configurable and the websocket endpoint is `/websocket`. 

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

Get the version of the Timely server. Returns a [Version](#version-response) response.

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

Timely uses Spring Security for user authentication. When successful, a HTTP Set-Cookie header is returned in the response with the name `TSESSIONID`. If a user does not log in, and anonymous access is enabled, then the user will only see data that is not marked. If a user does not log in, and anonymous access is disabled, then any operation that requires authentication will return an error. HTTP GET and POST methods are supported, the GET request is used when client SSL authentication is configured. When using the HTTP protocol, the `TSESSIONID` cookie should be sent along with the request. For the WebSocket protocol the client will need to add the `TSESSIONID` cookie value to the request in the `sessionId` property. If using the WebSocket protocol with anonymous access, then use a unique `sessionId` value for the duration of the client session. Returns a [Login](#login-response) response.

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

Attribute | Type | Description
----------|------|------------
meric | string | The name of the metric
timestamp | long | The timestamp in ms
value | double | The value of this metric at this time
tags | map | (Optional) Pairs of K,V strings to associate with this metric

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
```

```json
{
  "operation" : "suggest",
  "sessionId" : "<value of TSESSIONID>",
  "type" : "metrics",
  "q" : "sys.cpu",
  "max" : 30
}
```

The suggest operation is used to find metrics, tag keys, or tag values that match the specified pattern. Returns a [Suggest](#suggest-response) response. Input parameters are:

Attribute | Type | Description
----------|------|------------
type | string | one of metrics, tagk, tagv
q | string | the query string
max | integer | the maximum number of results

## Lookup

```http
GET /api/search/lookup?m=sys.cpu.user{host=*}&limit=3000 HTTP/1.1 
```

```http
POST /api/search/lookup HTTP/1.1
{
  "metric": "sys.cpu.user",
  "limit": 3000,
  "tags":[
     {
      "key": "host",
      "value": "*"
    }
  ]
}
```

```json
{
  "operation" : "lookup",
  "sessionId" : "<value of TSESSIONID>",
  "metric": "sys.cpu.user",
  "limit": 3000,
  "tags":[
     {
      "key": "host",
      "value": "*"
    }
  ]
}
```

The lookup operation is used to find information in the meta table associated with the supplied metric or tag input parameters. Returns a [Lookup](#lookup-response) response.

Attribute | Type | Description
----------|------|------------
metric | string | metric name or prefix. Used in HTTP POST and WebSocket
m | string | metric name or prefix. Used in HTTP GET
limit | int | (Optional default:25) maximum number of results
tags | map | (Optional) Pairs of K,V strings to to match results against

## Query

## Aggregators

## Metrics

# Subscription Operations

## Create

## Add

## Remove

## Close

