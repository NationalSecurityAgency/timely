import Timely
from TimeDateRange import TimeDateRange
from TimeDateRange import UTC
from datetime import datetime
import pandas
import re
import plotly

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

def createTitle(metric, sample, how, rolling_average, orCondition, min, max, alert_pct):

    if orCondition:
        condition = "OR"
    else:
        condition = "AND"

    metricTitle = metric
    alertTitle = ""
    if sample is not None:
        sampleTitle = sample + " " + how + " samples"

    if min is not None:
        if alertTitle != "":
            alertTitle += " " + condition + " "
        alertTitle += "(y < " + str(min) + ")"

    if max is not None:
        if alertTitle != "":
            alertTitle += " " + condition + " "
        alertTitle += "(y > " + str(max) + ")"

    if alert_pct is not None:
        if alertTitle != "":
            alertTitle += " " + condition + " "
        if alert_pct > 0:
            alertTitle += "y > " + str(alert_pct) + "% above " + str(rolling_average) + " sample average"
        else:
            alertTitle += "y < " + str(-alert_pct) + "% below " + str(rolling_average) + " sample average"

    title = metricTitle
    if sampleTitle != "":
        title += "\r" + sampleTitle

    if alertTitle != "":
        title += "\r" + alertTitle

    return title

def analytics(timelyMetric, notebook=False, alertConfig={}):

    groupByColumn = alertConfig.get('groupByColumn', None)
    if groupByColumn is None:
        return

    includeColRegex = alertConfig.get('includeColRegex', None)
    excludeColRegex = alertConfig.get('excludeColRegex', None)
    sample = alertConfig.get('sample', None)
    how = alertConfig.get('how', 'mean')
    rolling_average = alertConfig.get('rolling_average', None)
    min_threshold = alertConfig.get('min_threshold', None)
    max_threshold = alertConfig.get('max_threshold', None)
    alert_percentage = alertConfig.get('alert_percentage', None)
    boolean = alertConfig.get('boolean', 'or')
    # alerts or all
    display = alertConfig.get('display', 'alerts')

    df = timelyMetric.getDataFrame()

    graphDF = Timely.pivot(df, timelyMetric.metric, groupByColumn=groupByColumn)

    if sample is not None:
        graphDF = Timely.resample(graphDF, sample, how=how)

    if excludeColRegex is not None:
        graphDF = graphDF.select(lambda x : not (re.search(excludeColRegex, x)), axis=1)
    if includeColRegex is not None:
        graphDF = graphDF.select(lambda x : re.search(includeColRegex, x), axis=1)

    graphDF_avg = pandas.DataFrame(graphDF, copy=True)

    combined = pandas.DataFrame()

    seriesConfig = {}
    for i in graphDF_avg.columns:
        col = str(i)

        orCondition = boolean=='or' or boolean=='OR'

        result = True
        if orCondition:
            result = False

        if min_threshold is not None:
            currCondition = graphDF[col].astype(float) < min_threshold
            result = addCondition(orCondition, result, currCondition)

        if max_threshold is not None:
            currCondition = graphDF[col].astype(float) > max_threshold
            result = addCondition(orCondition, result, currCondition)

        graphDF_avg = Timely.rolling_average(graphDF_avg, str(i), rolling_average=rolling_average)
        if (alert_percentage is not None) and (rolling_average is not None):
            if alert_percentage > 0:
                multiple = 1.0 + (float(abs(alert_percentage)) / float(100))
                currCondition = graphDF[col].astype(float) > (graphDF_avg[col].astype(float) * multiple)
                result = addCondition(orCondition, result, currCondition)
            if alert_percentage < 0:
                multiple = 1.0 - (float(abs(alert_percentage)) / float(100))
                if multiple > 0:
                    currCondition = graphDF[col].astype(float) < (graphDF_avg[col].astype(float) * multiple)
                    result = addCondition(orCondition, result, currCondition)

        exceptional = graphDF.loc[result, col]
        if (display.lower() == "all") or (display.lower() == "alerts" and exceptional.size > 0):
            combined[col] = graphDF[col]

        if (display.lower() == "all") or (display.lower() == "alerts" and exceptional.size > 0 and alert_percentage is not None and rolling_average is not None):
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

    graphConfig = {}
    graphConfig["title"] = createTitle(timelyMetric.metric, sample, how, rolling_average, orCondition, min_threshold, max_threshold, alert_percentage)

    if not combined.empty:
        combined = Timely.unpivot(combined, timelyMetric.metric, groupByColumn=groupByColumn)
        combined = combined.sort_index()
        combined = combined.sort_values(['date', groupByColumn])
        Timely.graph(combined, timelyMetric.metric, sample=sample, groupByColumn=groupByColumn, seriesConfig=seriesConfig, graphConfig=graphConfig, notebook=notebook)