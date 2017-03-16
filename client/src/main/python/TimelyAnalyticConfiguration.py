import pandas


class TimelyAnalyticConfiguration():


    def __init__(self, analyticConfig):

        if isinstance(analyticConfig, dict):
            self.groupByColumn = analyticConfig.get('groupByColumn', None)
            self.includeColRegex = analyticConfig.get('includeColRegex', None)
            self.excludeColRegex = analyticConfig.get('excludeColRegex', None)
            self.sample_period = analyticConfig.get('sample', None)
            self.how = analyticConfig.get('how', 'mean')
            self.rolling_average_period = analyticConfig.get('rolling_average_period', None)
            self.min_threshold = analyticConfig.get('min_threshold', None)
            self.max_threshold = analyticConfig.get('max_threshold', None)
            self.alert_percentage = analyticConfig.get('alert_percentage', None)
            self.min_alert_period = analyticConfig.get('min_alert_period', None)
            boolean = analyticConfig.get('boolean', 'or')
            self.orCondition = boolean == 'or' or boolean == 'OR'
            # alerts or all
            self.display = analyticConfig.get('display', 'alerts')
            self.send_alerts_to = analyticConfig.get('send_alerts_to', [])
            self.output_dir = analyticConfig.get('output_dir', '/tmp')
            self.last_alert = analyticConfig.get('last_alert', None)
            self.system_name = analyticConfig.get('system_name', None)

            self.sample = None
            self.sample_minutes = None
            if self.sample_period is not None:
                td = pandas.to_timedelta(self.sample_period)
                self.sample_minutes = int(td.total_seconds() / 60)
                self.sample = str(self.sample_minutes) + 'min'

            self.rolling_average_samples = None
            self.rolling_average_minutes = None
            if (self.rolling_average_period is not None) and (self.sample_minutes is not None):
                td = pandas.to_timedelta(self.rolling_average_period)
                self.rolling_average_minutes = int(td.total_seconds() / 60)
                self.rolling_average_samples = int(self.rolling_average_minutes / self.sample_minutes)

            self.min_alert_minutes = None
            if self.min_alert_period is not None:
                td = pandas.to_timedelta(self.min_alert_period)
                self.min_alert_minutes = int(td.total_seconds() / 60)

            self.last_alert_minutes = None
            if self.last_alert is not None:
                td = pandas.to_timedelta(self.last_alert)
                self.last_alert_minutes = int(td.total_seconds() / 60)

        elif isinstance(analyticConfig, TimelyAnalyticConfiguration):
            self.groupByColumn = analyticConfig.groupByColumn
            self.includeColRegex = analyticConfig.includeColRegex
            self.excludeColRegex = analyticConfig.excludeColRegex
            self.sample_period = analyticConfig.sample_period
            self.sample_minutes = analyticConfig.sample_minutes
            self.sample = analyticConfig.sample
            self.how = analyticConfig.how
            self.rolling_average_period = analyticConfig.rolling_average_period
            self.rolling_average_samples = analyticConfig.rolling_average_samples
            self.rolling_average_minutes = analyticConfig.rolling_average_minutes
            self.min_threshold = analyticConfig.min_threshold
            self.max_threshold = analyticConfig.max_threshold
            self.alert_percentage = analyticConfig.alert_percentage
            self.min_alert_period = analyticConfig.min_alert_period
            self.min_alert_minutes = analyticConfig.min_alert_minutes
            self.orCondition = analyticConfig.orCondition
            # alerts or all
            self.display = analyticConfig.display
            self.send_alerts_to = analyticConfig.send_alerts_to
            self.output_dir = analyticConfig.output_dir
            self.last_alert = analyticConfig.last_alert
            self.last_alert_minutes = analyticConfig.last_alert_minutes
            self.system_name = analyticConfig.system_name
