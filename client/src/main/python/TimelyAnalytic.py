import TimelyMetric
from TimeDateRange import TimeDateRange
from TimeDateRange import UTC
from datetime import datetime
import pandas
import re
import plotly
from TimelyAlert import TimelyAlert

utc = UTC()
plotly.offline.init_notebook_mode()

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

    if analyticConfig.groupByColumn is None:
        return

    df = timelyMetric.getDataFrame()

    graphDF = TimelyMetric.pivot(df, timelyMetric.metric, groupByColumn=analyticConfig.groupByColumn)

    if analyticConfig.sample is not None:
        graphDF = TimelyMetric.resample(graphDF, analyticConfig.sample, how=analyticConfig.how)

    if analyticConfig.excludeColRegex is not None:
        graphDF = graphDF.select(lambda x : not (re.search(analyticConfig.excludeColRegex, x)), axis=1)
    if analyticConfig.includeColRegex is not None:
        graphDF = graphDF.select(lambda x : re.search(analyticConfig.includeColRegex, x), axis=1)

    graphDF_avg = pandas.DataFrame(graphDF, copy=True)

    combined = pandas.DataFrame()

    seriesConfig = {}
    for i in graphDF_avg.columns:
        col = str(i)

        result = True
        if analyticConfig.orCondition:
            result = False

        if analyticConfig.min_threshold is not None:
            currCondition = graphDF[col].astype(float) < analyticConfig.min_threshold
            result = addCondition(analyticConfig.orCondition, result, currCondition)

        if analyticConfig.max_threshold is not None:
            currCondition = graphDF[col].astype(float) > analyticConfig.max_threshold
            result = addCondition(analyticConfig.orCondition, result, currCondition)

        graphDF_avg = TimelyMetric.rolling_average(graphDF_avg, str(i), rolling_average=analyticConfig.rolling_average)
        if (analyticConfig.alert_percentage is not None) and (analyticConfig.rolling_average is not None):
            if analyticConfig.alert_percentage > 0:
                multiple = 1.0 + (float(abs(analyticConfig.alert_percentage)) / float(100))
                currCondition = graphDF[col].astype(float) > (graphDF_avg[col].astype(float) * multiple)
                result = addCondition(analyticConfig.orCondition, result, currCondition)
            if analyticConfig.alert_percentage < 0:
                multiple = 1.0 - (float(abs(analyticConfig.alert_percentage)) / float(100))
                if multiple > 0:
                    currCondition = graphDF[col].astype(float) < (graphDF_avg[col].astype(float) * multiple)
                    result = addCondition(analyticConfig.orCondition, result, currCondition)

        exceptional = graphDF.loc[result, col]
        if (analyticConfig.display.lower() == "all") or (analyticConfig.display.lower() == "alerts" and exceptional.size > 0):
            combined[col] = graphDF[col]

        if (analyticConfig.display.lower() == "all") or (analyticConfig.display.lower() == "alerts" and exceptional.size > 0 and analyticConfig.alert_percentage is not None and analyticConfig.rolling_average is not None):
            combined[col+'_avg'] = graphDF_avg[col]

        if (exceptional.size > 0):
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
        combined = TimelyMetric.unpivot(combined, timelyMetric.metric, groupByColumn=analyticConfig.groupByColumn)
        combined = combined.sort_index()
        combined = combined.sort_values(['date', analyticConfig.groupByColumn])
        timelyAlert = TimelyAlert(timelyMetric, combined, seriesConfig, analyticConfig, notebook)

    return timelyAlert