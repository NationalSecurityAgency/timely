import TimelyMetric
import TimelyAnalytic
import DataOperations
from TimelyAnalyticConfiguration import TimelyAnalyticConfiguration

hostport = "127.0.0.1:4244"
timelyMetric = TimelyMetric.TimelyMetric(hostport, '', None, None, None, 48, None).fetch()

analyticConfig = TimelyAnalyticConfiguration({
    'includeColRegex' : None,
    'excludeColRegex' : None,
    'groupByColumn' : 'instance',
    'sample' : '10min',
    'how' : 'mean',
    'rolling_average' : 12,
    'min_threshold' : None,
    'max_threshold' : None,
    'alert_percentage' : 25,
    'boolean' : 'and',
    'display' : 'all',
    'output_dir' : '/path/to/output'
})

alert = TimelyAnalytic.find_alerts(timelyMetric, analyticConfig)

if alert is not None:
    # write graph to file
    file = alert.graph()

    text = DataOperations.getTitle(timelyMetric.metric, analyticConfig)

    # send email with graph attached
    alert.email("", "", text, text, [file])

    # log to syslog
    alert.log(text)

