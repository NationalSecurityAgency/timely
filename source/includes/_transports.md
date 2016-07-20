# Transports

The Timely server supports clients sending requests over different protocols. Not all operations are supported over all protocols. The table below summarizes the protocols supported for each operation. The API documention is organized into sections that describe the general, time series, and subscription operations.

> The TCP, HTTPS, and WebSocket ports can be specified in the Timely configuration properties.


> The endpoint for the WebSocket protocol is `/websocket` 

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

