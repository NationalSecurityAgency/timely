import TimelyMetric
import TimelyAnalytic

hostport = "127.0.0.1:4244"
timelyMetric = TimelyMetric.TimelyMetric(hostport, '', None, None, None, 48, None).fetch()
TimelyAnalytic.find_alerts(timelyMetric, TimelyAnalyticConfiguration({
    'includeColRegex' : None,
    'excludeColRegex' : None,
    'groupByColumn' : 'instance',
    'sample' : '10min',
    'how' : 'mean',
    'rolling_average' : 12,
    'min_threshold' : None,
    'max_threshold' : None,
    'alert_percentage' : 25,
    'boolean' : 'and'
    'display' : 'all',
    'output_dir' : '/path/to/output'
}))

if alert is not None:
    alert.graph()

