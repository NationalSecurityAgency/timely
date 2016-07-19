# Timely-App

This is an app plugin for the [Grafana](http://grafana.org) time-series visualization tool.

## Installation

Copy the entire timely-app directory to the Grafana plugins directory, restart grafana.


## Installation

Unpack the contents of the timely-app directory into the grafana plugins directory.
By default, `/var/lib/grafana/plugins/`  Restart Grafana.


## Setup

Enable Timely App
  Timely should appear in Grafana menu

Create Datasource
  Browser Test - opens separate browser window to allow you to accept the cert manually if you don't have cert and ca installed in browser.

  Click Add. This test connection to Timely using the /version rest endpoint.

  Import dashboard. Currently displays fake ingest data and real timely ingest metrics. Dashboard should be visible in Timely-Status

Login to Timely

  Get user authorizations from Timely. At this point dashboard may not work if anonymous access is disabled in Timely. Must login to Timely
  to set session cookie so Timely can query data using your authorizations.

  View dashboard - this should now be working
