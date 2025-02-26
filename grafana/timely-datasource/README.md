# Timely data source

Use this data source to fetch data from the Timely time-series database.

### Configuring the data source

Timely data source is configured with

1. Hostname or IP to Timely, Timely Balancer, or load balancer
2. Https port
3. Whether to forward the OAuth login token to Timely 
4. Whether to use the server PKI is the OAuth is missing for a user
5. PKI configuration
6. Whether to allow insecure SSL (e.g. self-signed certificates for testing)
7. Data source tags where the configured tag keys / tag values are added to queries that contain the corresponding data source tag key

### Constructing a query 

1. Autocomplete values (using substring matching) for the metric and tag keys/values are fetched via the suggest API from the Timely metadata table 
2. Select or type a metric name
3. Tag keys will populate based on the selected metric
4. Tag values will populate based on the selected metric and selected tag key
5. Data source tags - a method of adding a tag to the query if the data source is configured with a tag value for the requested tag key
5. Alias is a way of setting the series name.  Variable values can be used: 
   1. \${metric} will substitute the name of the metric
   2. \${tag_TAGKEY} will substitute the corresponding tag value for each series
6. Downsample - downsample period to collect all values with unique tag key / tag value for all requested tags
7. Downsample Aggregator - the operation to use on each set of collected values
8. Disable downsampling - return all values; do not downsample
9. All Results Aggregator - Combine all results per downsample period into a single value using the selected operation
10. Compute Rate - for continuously increasing metrics, compute the metric change across the downsample period and divide by the rate interval
11. Rate Interval - rate interval for computing rate - defaults to the downsample period
12. Counter - for continuously increasing metrics that reset at a maximum value
