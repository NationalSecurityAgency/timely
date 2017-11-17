import time
from datetime import timedelta
from datetime import datetime
from TimeDateRange import UTC
import pandas
import matplotlib.dates as mdates
import plotly.graph_objs as go
import plotly.offline as py
import seaborn
import matplotlib.ticker as tkr
import numpy as np
import os

utc = UTC()

def pivot(df, metric, groupByColumn=None):

    dataFrame = pandas.DataFrame(df, copy=True)
    if dataFrame is not None:
        if groupByColumn is not None:
            dataFrame["date"] = dataFrame.index.to_pydatetime()
            dataFrame = dataFrame.pivot_table(index="date", columns=groupByColumn, values=metric)
    return dataFrame


def resample(df, sample, how='mean'):

    dataFrame = pandas.DataFrame(df, copy=True)
    if dataFrame is not None:
        if sample is not None:
            dataFrame = dataFrame.resample(sample, how=how).interpolate()
    return dataFrame


def unpivot(df, metric, groupByColumn=None):
    dataFrame = pandas.DataFrame(df, copy=True)
    if dataFrame is not None:
        dataFrame['date'] = dataFrame.index.values
        dataFrame = pandas.melt(dataFrame, id_vars=['date'], value_name=metric, var_name=groupByColumn)
        dataFrame = dataFrame.set_index('date')
    return dataFrame


def rolling_average(df, metric, rolling_average=None):

    dataFrame = pandas.DataFrame(df, copy=True)
    if dataFrame is not None:
        if rolling_average is not None:
            dataFrame[metric] = pandas.rolling_mean(dataFrame[metric], rolling_average)
    return dataFrame


def graph(analyticConfig, df, timelyMetric, seriesConfig={}, graphConfig={}, notebook=False, type='png'):
    if type == 'png':
        return graphSeaborn(analyticConfig, df, timelyMetric, seriesConfig=seriesConfig, graphConfig=graphConfig, notebook=notebook)
    else:
        return graphPlotly(analyticConfig, df, timelyMetric, seriesConfig=seriesConfig, graphConfig=graphConfig, notebook=notebook)


def graphSeaborn(analyticConfig, df, timelyMetric, seriesConfig={}, graphConfig={}, notebook=False):

    dataFrame = pandas.DataFrame(df, copy=True)
    if dataFrame is not None:
        dataFrame = ensureMinSeriesLength(dataFrame, analyticConfig.groupByColumn)
        dataFrame['date'] = dataFrame.index.values
        dataFrame['dateFloat'] = dataFrame['date'].apply(lambda x: datetime2graphdays(x))
        dataFrame['index'] = dataFrame['dateFloat']
        groupByColumn = analyticConfig.groupByColumn

        if groupByColumn is not None:
            # sort on date and the groupByColumn so that the series legend is sorted
            dataFrame = dataFrame.sort_values(['index', analyticConfig.groupByColumn])
        else:
            dataFrame = dataFrame.sort_values(['index'])

        #dataFrame = dataFrame.set_index('index', drop=True)
        dataFrame = dataFrame.dropna()
        seaborn.set(style="darkgrid", palette="Set2")
        fig, ax = seaborn.plt.subplots()

        colors = np.random.random((len(dataFrame), 3))
        i = 0
        for col in df[analyticConfig.groupByColumn].unique():
            tempDataFrame = dataFrame.loc[dataFrame[analyticConfig.groupByColumn] == col]
            addlColConfig = seriesConfig.get(col, seriesConfig.get('default', dict()))
            markerDict = addlColConfig.get("marker", dict())
            color = markerDict.get("color", "")
            if color == "red":
                seaborn.tsplot(tempDataFrame, time="dateFloat", unit=None, interpolate=False, color="red", marker="d",
                               estimator=np.nanmean, condition=groupByColumn, value=timelyMetric.metric, ax=ax)
            else:
                seaborn.tsplot(tempDataFrame, time="dateFloat", unit=None, interpolate=True, color=colors[i],
                               estimator=np.nanmean, condition=groupByColumn, value=timelyMetric.metric, ax=ax)
            i = i + 1

        ax.xaxis_date()
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y/%m/%d %H:%M:%S'))
        ax.set_xlim(xmin=dataFrame['dateFloat'].min(), xmax=dataFrame['dateFloat'].max())

        formatter = tkr.ScalarFormatter(useMathText=False)
        formatter.set_scientific(False)
        ax.yaxis.set_major_formatter(formatter)

        graphTitle = getTitle(timelyMetric, analyticConfig)
        seaborn.plt.title(graphTitle, fontsize='large')
        seaborn.plt.xlabel('date/time')
        seaborn.plt.ylabel(timelyMetric.metric)

        legendTitle = "Legend"
        if groupByColumn is not None:
            legendTitle = groupByColumn

        seaborn.plt.legend(loc='upper center', prop={'size':8}, ncol=10, title=legendTitle, bbox_to_anchor=(0.5,-0.4))
        fig.autofmt_xdate(rotation=45)

        seaborn.plt.autoscale(tight=False)

        if notebook == True:
            seaborn.plt.show()
            return None
        else:
            directory = analyticConfig.output_dir
            try:
                os.makedirs(directory, mode=0755)
            except OSError, e:
                # be happy if someone already created the path
                pass
            baseFilename = getFilename(timelyMetric.metric, analyticConfig)
            filename = directory+'/'+baseFilename
            fig.savefig(filename + '.png')
            return baseFilename + '.png'


def graphPlotly(analyticConfig, df, timelyMetric, seriesConfig={}, graphConfig={}, notebook=False):

    dataFrame = pandas.DataFrame(df, copy=True)
    if dataFrame is not None:
        dataFrame['colFromIndex'] = dataFrame.index
        if analyticConfig.groupByColumn is not None:
            # sort on date and the groupByColumn so that the series legend is sorted
            dataFrame = dataFrame.sort_values(['colFromIndex', analyticConfig.groupByColumn])
        else:
            dataFrame = dataFrame.sort_values(['colFromIndex'])

        title = getTitle(timelyMetric, analyticConfig)

        dataFrame['date'] = dataFrame.index.values

        layoutConfig = dict(
            title=title,
            autosize=False,
            width=1000,
            height=700,
            showlegend=True,
            xaxis=dict(
                autorange=True,
                tickangle=-45
            ),
            yaxis=dict(
                autorange=True,
                title=timelyMetric.metric
            )
        )

        layoutConfig.update(graphConfig)

        layout = go.Layout(layoutConfig)

        data = None
        groupByValues = sorted(dataFrame[analyticConfig.groupByColumn].unique())
        for col in groupByValues:
            tempDataFrame = dataFrame.loc[dataFrame[analyticConfig.groupByColumn] == col]

            addlColConfig = seriesConfig.get(col, seriesConfig.get('default', dict()))

            config = dict(
                name=col,
                x=tempDataFrame.index,
                y=tempDataFrame[timelyMetric.metric],
                hoverinfo='x+y+text',
                text=col
            )

            config.update(addlColConfig)

            s = go.Scatter(config)

            if data is None:
                data = [s]
            else:
                data.append(s)

        directory = analyticConfig.output_dir
        try:
            os.makedirs(directory, mode=0755)
        except OSError, e:
            # be happy if someone already created the path
            pass
        baseFilename = getFilename(timelyMetric.metric, analyticConfig)
        filename = directory+'/'+baseFilename

        fig = go.Figure(data=data, layout=layout)
        if notebook:
            py.iplot(fig, filename=filename, show_link=False)
            return None
        else:
            py.plot(fig, output_type='file', filename=filename+'.html', image_filename=filename, image=None, auto_open=False, show_link=False)
            return baseFilename+'.html'

def ensureMinSeriesLength(df, groupByColumn):

    dataFrame = df
    if groupByColumn is not None:
        dataFrame = pandas.DataFrame(df, copy=True)
        dataFrame = dataFrame.dropna()

        extra = pandas.DataFrame()
        for c, (cond, df_c) in enumerate(dataFrame.groupby(groupByColumn, sort=False)):
            if len(df_c) <= 1:
                extra = extra.append(df_c)

        extra.index = extra.index + pandas.DateOffset(seconds=1)
        dataFrame = dataFrame.append(extra)

    return dataFrame

def datetime2graphdays(dt):

    return mdates.epoch2num((time.mktime(dt.timetuple()) / 86400) * mdates.SEC_PER_DAY)

def graphdays2datetime(x):
    epoch = mdates.num2epoch(x)
    return datetime.fromtimestamp(int(epoch), utc)


def getFilename(metric, analyticConfig):

    now = datetime.now(tz=utc)
    format = "%Y%m%d%H%M%S"
    nowStr = time.strftime(format, now.timetuple())
    fileNameBase = metric
    if analyticConfig.sample is not None:
        fileNameBase += "-" + analyticConfig.sample
    if analyticConfig.how is not None:
        fileNameBase += "-" + analyticConfig.how

    return fileNameBase + '-' + nowStr

def getDirectoryPathForDate(format='%Y/%m/%d'):
    now = datetime.now(tz=utc)
    return time.strftime(format, now.timetuple())

def getTitle(timelyMetric, config, separator='\n'):
    if config.orCondition:
        condition = "OR"
    else:
        condition = "AND"

    metricTitle = timelyMetric.metric
    if timelyMetric.tags is not None and len(timelyMetric.tags) > 0:
        metricTitle += ' (' + timelyMetric.tags + ')'

    sampleTitle = ""
    alertTitle = ""
    if config.sample_period is not None:
        sampleTitle = config.sample_period + " " + config.how + " samples"

    if config.rolling_average_period is not None:
        sampleTitle += ', ' + config.rolling_average_period + ' rolling average'

    if config.min_threshold is not None:
        if alertTitle != "":
            alertTitle += " " + condition + " "
        alertTitle += "(y < " + str(config.min_threshold) + ")"

    if config.average_min_threshold is not None:
        if alertTitle != "":
            alertTitle += " " + condition + " "
        alertTitle += "(y-avg < " + str(config.average_min_threshold) + ")"

    if config.max_threshold is not None:
        if alertTitle != "":
            alertTitle += " " + condition + " "
        alertTitle += "(y > " + str(config.max_threshold) + ")"

    if config.average_max_threshold is not None:
        if alertTitle != "":
            alertTitle += " " + condition + " "
        alertTitle += "(y-avg > " + str(config.average_max_threshold) + ")"

    if config.min_threshold_percentage is not None:
        if alertTitle != "":
            alertTitle += " " + condition + " "
        if config.min_threshold_percentage >= 0:
            descriptivePercentage = 100 + config.min_threshold_percentage
        else:
            descriptivePercentage = abs(config.min_threshold_percentage)
        alertTitle += "(y < " + str(descriptivePercentage) + "% of " + str(
            config.rolling_average_period) + " sample average)"

    if config.max_threshold_percentage is not None:
        if alertTitle != "":
            alertTitle += " " + condition + " "
        if config.max_threshold_percentage >= 0:
            descriptivePercentage = 100 + config.max_threshold_percentage
        else:
            descriptivePercentage = abs(config.max_threshold_percentage)
        alertTitle += "(y > " + str(descriptivePercentage) + "% of " + str(
            config.rolling_average_period) + " sample average)"

    if config.system_name is None:
        title = metricTitle
    else:
        title = config.system_name + ' - ' + metricTitle

    if sampleTitle != "":
        title += separator + sampleTitle

    if alertTitle != "":
        if config.min_alert_period is not None:
            alertTitle = '(' + alertTitle + ') for at least ' + config.min_alert_period
        if config.last_alert is not None:
            alertTitle = alertTitle + ' in last ' + config.last_alert
        title += separator + alertTitle


    return title