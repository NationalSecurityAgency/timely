# Timeseries API

The Timeseries API contains operations for getting metadata about the time series, and retrieving the data points.

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

## Query Response

```http
HTTP/1.1 200 OK
{
  "metric" : "sys.cpu.user",
  "tags" :{
    "tag1" : "value1"
  },
  "aggregatedTags"  :[],
  "dps" : {
    "1468954529" : 1.0,
    "1468954530" : 3.0
  }
}
```

```json
[{
  "metric" : "sys.cpu.user",
  "tags" :{
    "tag1" : "value1"
  },
  "aggregatedTags"  :[],
  "dps" : {
    "1468954529" : 1.0,
    "1468954530" : 3.0
  }
}]
```

The response to the query operation contains a set of an array of time series data points. Each array element contains the metric and associated set of tags along with the metric values and associated timestamps. Reponse attributes are:

Attribute | Type | Description
----------|------|------------
metric | string | metric name for this time series
tags | map | tags associated with this time series
aggregatedTags | map | not used
dps | map | map of timestamp to metric value

## Aggregators Response

TODO

## Metrics Response

TODO

## Put Operation

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

## Suggest Operation

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

## Lookup Operation

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

## Query Operation

```http
GET /api/query?start=1356998400&end=1356998460&m=sum:rate{false,100,0}:sys.cpu.user{host=*}{rack=r1|r2}&tsuid=sum:000001000002000042,000001000002000043 HTTP/1.1
```

```http
POST /api/query HTTP/1.1
{
  "start": 1356998400,
  "end": 1356998460,
  "queries": [
    {
      "aggregator": "sum",
      "metric": "sys.cpu.user",
      "rate": "true",
      "rateOptions": {
          "counter":false,
          "counterMax":100,
          "resetValue":0
      },
      "downsample" : "1m-max",
      "tags": {
           "host": "*",
           "rack": "r1"
        },
      "filters": [
        {
           "type":"wildcard",
           "tagk":"host",
           "filter":"*",
           "groupBy":true
        },
        {
           "type":"literal_or",
           "tagk":"rack",
           "filter":"r1|r2",
           "groupBy":false
        }
      ]
    },
    {
      "aggregator": "sum",
      "tsuids": [
        "000001000002000042",
        "000001000002000043"
      ]
    }
  ]
}
```

```json
{
  "operation" : "query",
  "sessionId" : "<value of TSESSIONID>",
  "start" : 1356998400,
  "end" : 1356998460,
  "queries" : [
    {
      "aggregator" : "sum",
      "metric" : "sys.cpu.user",
      "rate" : "true",
      "rateOptions" : {
          "counter" : false,
          "counterMax" : 100,
          "resetValue" : 0
      },
      "downsample" : "1m-max",
      "tags" : {
           "host" : "*",
           "rack" : "r1"
        },
      "filters" : [
        {
           "type" : "wildcard",
           "tagk" : "host",
           "filter" : "*",
           "groupBy" : true
        },
        {
           "type" : "literal_or",
           "tagk" : "rack",
           "filter" : "r1|r2",
           "groupBy" : false
        }
      ]
    }
  ]
}
```

The query operation is used to find time series data that matches the submitted query parameters. Returns a [Query] (#query-response) response.

Attribute | Type | Description
----------|------|------------
start | long | start time in ms for this query
end | long | end time in ms for this query
queries | array | array of metric sub query types. Used in HTTP Post or WebSocket

### Metric SubQuery Type

TODO

## Aggregators Operation

TODO

## Metrics Operation

TODO
