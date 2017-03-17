import TimelyMetric
import TimelyAnalytic
import DataOperations
import os
from TimelyAnalyticConfiguration import TimelyAnalyticConfiguration

hostport = "127.0.0.1:4244"
timelyMetric = TimelyMetric.TimelyMetric(hostport, '', None, None, None, 48, None).fetch()

analyticConfig = TimelyAnalyticConfiguration({
    'system_name' : 'TestSystem',
    'includeColRegex' : None,
    'excludeColRegex' : None,
    'groupByColumn' : 'instance',
    'sample' : '10 minutes',
    'how' : 'mean',
    'rolling_average_period' : '12 hours',
    'min_threshold' : None,
    'average_min_threshold' : None,
    'max_threshold' : None,
    'average_max_threshold' : None,
    'alert_percentage' : 25,
    'boolean' : 'and',
    'min_alert_period' : '5 minutes',
    'last_alert' : '1 hour',
    'display' : 'all',
    'output_dir' : '/path/to/output'
})

alert = TimelyAnalytic.find_alerts(timelyMetric, analyticConfig)

if alert is not None:
    # write graph to file
    oldmask = os.umask(022)
    file = alert.graph(type='html')
    os.umask(oldmask)

    text = DataOperations.getTitle(timelyMetric, analyticConfig)

    # send email with graph attached
    alert.email("", "", text, text, [file])

    # log to syslog
    alert.log(text)

