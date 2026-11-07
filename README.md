![Timely](timely-readme-logo.png)

[![Apache License][li]][ll]

Timely is a time series database application that provides secure access to time series data. Timely is written in Java and designed to work with [Apache Accumulo](https://accumuloProperties.apache.org/) and [Grafana](https://www.grafana.com). Documentation is located [here](https://nationalsecurityagency.github.io/timely/).

[li]: https://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0

## Query value bounds

Each `/api/query` POST or WebSocket subquery accepts optional `minValue` and
`maxValue` numbers. Both bounds are inclusive. They filter the final values after
rate calculation, downsampling, aggregation, and combining cache and store results.
They reduce the response size; they do not reduce the underlying scan or change
which samples contribute to an aggregation. Omitting both bounds preserves the
existing behaviour.

For example, this POST returns only aggregated values between 10 and 90:

```json
{
  "start": 1700000000000,
  "end": 1700000060000,
  "queries": [
    {"aggregator": "avg", "metric": "sys.cpu.user", "minValue": 10, "maxValue": 90}
  ]
}
```

GET requests accept `minValue` and `maxValue` query parameters, applied to every
subquery: `/api/query?start=1700000000000&m=avg:sys.cpu.user&minValue=10&maxValue=90`.
Use POST or WebSocket queries when different subqueries need different bounds.
Bounds must be finite and `minValue` must not exceed `maxValue`. Equal bounds keep
only matching values. A bounded query excludes NaN values. When every value is
filtered out, the series retains its metric and tags with an empty `dps` object.
