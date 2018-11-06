import timely.TimelyMetric
import timely.TimelyAnalytic
import timely.DataOperations
import os
from timely.TimelyAnalyticConfiguration import TimelyAnalyticConfiguration

hostport = "127.0.0.1:5244"
timelyMetric = timely.TimelyMetric.TimelyMetric(hostport, 'timely.keys.metric.inserted', None, None, None, '24 hours', None).fetch()

print(timelyMetric.getDataFrame())
exit()

analyticConfig = TimelyAnalyticConfiguration({
    'system_name' : 'TestSystem',
    'counter' : False,
    'includeColRegex' : None,
    'excludeColRegex' : None,
    'groupByColumn' : 'host',
    'sample' : '1 minute',
    'how' : 'mean',
    'rolling_average_period' : '12 hours',
    'min_threshold' : None,
    'average_min_threshold' : None,
    'max_threshold' : None,
    'average_max_threshold' : None,
    'min_threshold_percentage' : None,
    'max_threshold_percentage' : None,
    'boolean' : 'and',
    'min_alert_period' : None,
    'last_alert' : None,
    'display' : 'all',
    'output_dir' : '/path/to/output'
})

alert = timely.TimelyAnalytic.find_alerts(timelyMetric, analyticConfig)

if alert is not None:
    # write graph to file
    oldmask = os.umask(022)
    file = alert.graph(type='html')
    os.umask(oldmask)

    text = timely.DataOperations.getTitle(timelyMetric, analyticConfig)

    # send email with graph attached
    alert.email("", "", text, text, [file])

    # log to syslog
    alert.log(text)

