import TimelyMetric
from TimeDateRange import TimeDateRange
from TimeDateRange import UTC
from TimelyAnalyticConfiguration import TimelyAnalyticConfiguration
import DataOperations
import pandas
import re
from pandas.tslib import Timestamp

from TimelyAlert import TimelyAlert
import numpy as np

utc = UTC()

def logTiming(timelyMetric, startTime, stopTime):
    df = timelyMetric.getDataFrame()
    print("start: " + str(startTime))
    print("stop: " + str(stopTime))
    print("num records: " + str(len(df)))
    elapsedSec = (TimeDateRange.unix_time_millis(stopTime) - TimeDateRange.unix_time_millis(startTime)) / 1000
    print("elapsed seconds: " + str(elapsedSec))
    recPerSec = len(df) / elapsedSec
    print("records per second: " + recPerSec)

def addCondition(orCondition, currResult, condition):
    if orCondition:
        currResult = currResult | condition
    else:
        currResult = currResult & condition
    return currResult

def find_alerts(timelyMetric, analyticConfig, notebook=False):


    df = timelyMetric.getDataFrame()

    graphDF = TimelyMetric.pivot(df, timelyMetric.metric, groupByColumn=analyticConfig.groupByColumn)

    if analyticConfig.excludeColRegex is not None:
        graphDF = graphDF.select(lambda x : not (re.search(analyticConfig.excludeColRegex, x)), axis=1)
    if analyticConfig.includeColRegex is not None:
        graphDF = graphDF.select(lambda x : re.search(analyticConfig.includeColRegex, x), axis=1)

    if analyticConfig.sample is not None:
        graphDF = TimelyMetric.resample(graphDF, analyticConfig.sample, how=analyticConfig.how)

    graphDF_avg = pandas.DataFrame(graphDF, copy=True)

    combined = pandas.DataFrame()

    now = Timestamp.now()

    seriesConfig = {}
    for i in graphDF_avg.columns:
        col = str(i)

        anyConditions = False
        result = np.ones(graphDF[col].shape, bool)
        if analyticConfig.orCondition:
            result = np.zeros(graphDF[col].shape, bool)

        if analyticConfig.min_threshold is not None:
            currCondition = graphDF[col].astype(float) < analyticConfig.min_threshold
            result = addCondition(analyticConfig.orCondition, result, currCondition)
            anyConditions = True

        if analyticConfig.max_threshold is not None:
            currCondition = graphDF[col].astype(float) > analyticConfig.max_threshold
            result = addCondition(analyticConfig.orCondition, result, currCondition)
            anyConditions = True

        if analyticConfig.rolling_average_samples is not None:
            graphDF_avg = TimelyMetric.rolling_average(graphDF_avg, col, rolling_average=analyticConfig.rolling_average_samples)
            if (analyticConfig.alert_percentage is not None) and (analyticConfig.rolling_average is not None):
                if analyticConfig.alert_percentage > 0:
                    multiple = 1.0 + (float(abs(analyticConfig.alert_percentage)) / float(100))
                    currCondition = graphDF[col].astype(float) > (graphDF_avg[col].astype(float) * multiple)
                    result = addCondition(analyticConfig.orCondition, result, currCondition)
                    anyConditions = True
                if analyticConfig.alert_percentage < 0:
                    multiple = 1.0 - (float(abs(analyticConfig.alert_percentage)) / float(100))
                    if multiple > 0:
                        currCondition = graphDF[col].astype(float) < (graphDF_avg[col].astype(float) * multiple)
                        result = addCondition(analyticConfig.orCondition, result, currCondition)
                        anyConditions = True

        if anyConditions == False:
            result = np.zeros(graphDF[col].shape, bool)

        exceptional = graphDF.loc[result, col]

        lastAlertRecentEnough = True
        minAlertPeriodMet = True
        if (exceptional.size > 0):
            if (analyticConfig.min_alert_minutes is not None) or (analyticConfig.last_alert_minues is not None):
                if analyticConfig.last_alert_minutes is not None:
                    lastAlertRecentEnough = False
                currentFirst = None
                currentLast = None
                currentSpan = None
                longestSpan = None
                for index, row in graphDF.iterrows():
                    if index in exceptional.index:
                        currentLast = index
                        if analyticConfig.last_alert_minutes is not None:
                            if lastAlertRecentEnough == False:
                                howLongAgo = int((now - currentLast).total_seconds() / 60)
                                if howLongAgo < analyticConfig.last_alert_minutes:
                                    lastAlertRecentEnough = True

                        if currentFirst is None:
                            currentFirst = index
                        currentSpan = currentLast - currentFirst
                        if (longestSpan is None) or (currentSpan > longestSpan):
                            longestSpan = currentSpan
                    else:
                        currentFirst = None
                        currentLast = None
                        currentSpan = None

                if longestSpan is None:
                    longestSpan = 0
                else:
                    longestSpan = int(longestSpan.total_seconds() / 60)

                if (analyticConfig.min_alert_minutes is not None) and (longestSpan < int(analyticConfig.min_alert_minutes)):
                    minAlertPeriodMet = False


        if (analyticConfig.display.lower() == "all") or (analyticConfig.display.lower() == "alerts" and exceptional.size > 0 and minAlertPeriodMet and lastAlertRecentEnough):
            combined[col] = graphDF[col]

        if ((analyticConfig.rolling_average_samples is not None) and
                ((analyticConfig.display.lower() == "all") or (analyticConfig.display.lower() == "alerts" and exceptional.size > 0 and analyticConfig.alert_percentage is not None and minAlertPeriodMet and lastAlertRecentEnough))):
            combined[col+'_avg'] = graphDF_avg[col]

        if (exceptional.size > 0) and ((analyticConfig.display.lower() == 'all') or (minAlertPeriodMet and lastAlertRecentEnough)):
            combined[col+'_warn'] = exceptional.dropna()

            seriesConfig[col+'_warn'] = {
                "mode" : "markers",
                "marker" : {
                    "symbol" : "hash-open",
                    "color" : "red"
                }
            }

    timelyAlert = None
    if not combined.empty:
        alertAnalyticConfig = TimelyAnalyticConfiguration(analyticConfig)
        if alertAnalyticConfig.groupByColumn is None:
            alertAnalyticConfig.groupByColumn = timelyMetric.metric + "_obs"
        combined = TimelyMetric.unpivot(combined, timelyMetric.metric, groupByColumn=alertAnalyticConfig.groupByColumn)
        combined = combined.sort_index()
        combined['date'] = combined.index.values
        combined = combined.sort_values(['date', analyticConfig.groupByColumn])
        combined = combined.drop(['date'], 1)
        combined = combined.dropna()
        combined = DataOperations.ensureMinSeriesLength(combined, alertAnalyticConfig.groupByColumn)

        message = DataOperations.getTitle(timelyMetric.metric, analyticConfig, separator=', ')

        timelyAlert = TimelyAlert(timelyMetric, combined, message, seriesConfig, alertAnalyticConfig, notebook)

    return timelyAlert