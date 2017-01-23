# API Overview

The Timely server supports clients sending requests over different protocols. Not all operations are supported over all protocols. The table below summarizes the protocols supported for each operation. The API documention is organized into sections that describe the general, time series, and subscription operations.

> The TCP, UDP, HTTPS, and WebSocket ports can be specified in the Timely configuration properties.


> The endpoint for the WebSocket protocol is `/websocket` 

Operation | UDP | TCP | HTTPS | WebSocket
----------|-----|-----|-------|----------
Version     |   | X | X | X
Login       |   |   | X |
Put         | X | X | X | X
Suggest     |   |   | X | X
Lookup      |   |   | X | X
Query       |   |   | X | X
Aggregators |   |   | X | X
Metrics     |   |   | X | X
Create      |   |   |   | X
Add         |   |   |   | X
Remove      |   |   |   | X
Close       |   |   |   | X

# Timely Client

Java Timely client library was started in 0.0.4, it is not complete, but what is done does work. The client library includes classes for communicating over all the protocols (UDP, TCP, HTTPS, WSS).

