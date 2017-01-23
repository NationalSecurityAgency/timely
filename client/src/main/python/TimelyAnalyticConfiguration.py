


class TimelyAnalyticConfiguration():


    def __init__(self, analyticConfig):

        self.groupByColumn = analyticConfig.get('groupByColumn', None)
        self.includeColRegex = analyticConfig.get('includeColRegex', None)
        self.excludeColRegex = analyticConfig.get('excludeColRegex', None)
        self.sample = analyticConfig.get('sample', None)
        self.how = analyticConfig.get('how', 'mean')
        self.rolling_average = analyticConfig.get('rolling_average', None)
        self.min_threshold = analyticConfig.get('min_threshold', None)
        self.max_threshold = analyticConfig.get('max_threshold', None)
        self.alert_percentage = analyticConfig.get('alert_percentage', None)
        boolean = analyticConfig.get('boolean', 'or')
        self.orCondition = boolean == 'or' or boolean == 'OR'
        # alerts or all
        self.display = analyticConfig.get('display', 'alerts')
        self.send_alerts_to = analyticConfig.get('send_alerts_to', [])
        self.output_dir = analyticConfig.get('output_dir', '/tmp')


