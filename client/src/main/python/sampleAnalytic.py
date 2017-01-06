import Timely
import Analytics

hostport = "127.0.0.1:4244"
timelyMetric = Timely.TimelyMetric(hostport, '', None, None, None, 48, None).fetch()
Analytics.analytics(timelyMetric, alertConfig={
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
})

