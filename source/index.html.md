---

title: Timely Reference

language_tabs:
  - plaintext: TCP
  - http: HTTP
  - json: WebSocket

toc_footers:
  - <a href='https://github.com/NationalSecurityAgency/timely'>Timely on GitHub</a>
  - <a href='https://github.com/lord/slate'>Documentation Powered by Slate</a>

includes:
  - server
  - security
  - quickstart
  - grafana
  - collectd
  - api
  - general
  - timeseries
  - subscription
  - developer
  - documentation
  - examples

search: true

---

#Introduction

Timely is a time series database application that provides secure access to time series data. Timely is written in Java and designed to work with [Apache Accumulo] (http://accumulo.apache.org/) and [Grafana] (http://www.grafana.org).

# Getting Started

Getting started with Timely requires that you:

> Use the [standalone] (#standalone-quick-start) server to get a test environment up and running quickly

1. Install and configure a Timely [server] (#timely-server)

2. Install and configure the Timely [app] (#deployment)  within Grafana

> Timely should accept data from TCollector with no changes. A [plugin] (#plugins-for-collectd) exists for sending data from CollectD.

3. Send metrics to Timely using [CollectD] (http://collectd.org/), OpenTSDBs [TCollector] (https://github.com/OpenTSDB/tcollector), etc.

