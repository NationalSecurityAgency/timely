# Subscription API

The subscription API allows the user to subscribe to metrics data using WebSockets. The create, add, remove, and close operations are expected to be [Text] (https://tools.ietf.org/html/rfc6455#section-5.6) frames and do not return a response for successful invocation. They return a [Close] (https://tools.ietf.org/html/rfc6455#section-5.5.1) frame with a message upon error condition. Each subscription is identified by a subscription id, a unique string, that the client will use to manipulate the subscription. This allows a client to create multiple subscriptions, using different subscription ids, on the same WebSocket channel.

## Metric Response

```json
{
  "metric" : "sys.cpu.user",
  "timestamp":1469028728091,
  "value":1.0,
  "tags":
  [
    {
      "key":"rack",
      "value":"r1"
    },{
      "key":"tag3",
      "value":"value3"
    },{
      "key":"tag4",
      "value":"value4"
    }
  ],
  "subscriptionId":"<unique id>"
}
```

The Metric response contains the metric name, tag set, value for the metric, and the timestamp.

## Create Operation

```json
{
  "operation" : "create",
  "subscriptionId" : "<unique id>"
}
```

Initialize this WebSocket connection for subscription requests. This method does not return a response.


## Add Operation

```json
{
  "operation" : "add",
  "subscriptionId" : "<unique id>",
  "metric" : "sys.cpu.user",
  "tags" : null,
  "startTime" : null,
  "delayTime" : 1000
}
```

Subscribe to metrics that match the metric name and optional tag set. Only one subscription per `metric` is supported. The Timely server will return [Metric] (#metric-response) responses over the WebSocket channel that match the `metric` and `tags` in the request. The Timely server will return metrics that are older than the `startTime`, which can be zero to return all currently stored data. When all data has been returned, the Timely server will wait `delayTime` ms before looking for new data. The Timely server maintains a pointer to the last metric returned, so it will not return data that is newly inserted but older than the last returned metric. Use the `timely.ws.subscription.lag` server configuration property to determine how close to current time the subscriptions will be. This will depend on your deployment. For example, if the `timely.write.latency` configuration property is 30s, then you may want to set the lag to greater than 30s to ensure that all data has arrived.

Attribute | Type | Description
----------|------|------------
metric | string | metric name
tags | map | (Optional) Map of K,V strings to use in the query.
startTime | long | (Optional, default 0) Return metrics after this time (in ms)
delayTime | long | Wait time to look for the arrival of new data.

## Remove Operation

```json
{
  "operation" : "remove",
  "subscriptionId" : "<unique id>",
  "metric" : "sys.cpu.user"
}
```

Remove the subscription for this `metric`. Data for this subscription will no longer be sent back to the user.

Attribute | Type | Description
----------|------|------------
metric | string | metric name

## Close Operation

```json
{
  "operation" : "close",
  "subscriptionId" : "<unique id>"
}
```

Remove all subscriptions associated with this WebSocket channel.
