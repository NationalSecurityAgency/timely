# Release Notes

## 0.0.3

* The metric value is no longer encoded using a Lexicoder. This means that Timely will not read your data correctly after an upgrade. You will need to remove your existing Timely tables. The Timely server will create new tables when it is started.
* The UDP protocol is now supported for put requests
* Put requests can be sent in binary form using Google Flatbuffers encoding, supported in UDP and TCP only
* Commons-Collections4 jar is now required on the TabletServer
* Configuration file updated to YAML for use with Spring Boot
* Accumulo 1.8.0 compatibility
* Individual metrics can have their own age-off configuration

