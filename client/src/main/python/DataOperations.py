import time
from datetime import datetime
from TimeDateRange import UTC
import pandas
import matplotlib.dates as mdates
import plotly.graph_objs as go
import plotly.offline as py
import seaborn
import matplotlib.dates as dates
import matplotlib.ticker as tkr

utc = UTC()

def pivot(df, metric, groupByColumn=None):

    dataFrame = pandas.DataFrame(df, copy=True)
    if dataFrame is not None:
        if groupByColumn is not None:
            dataFrame = dataFrame.pivot_table(index="date", columns=groupByColumn, values=metric)
    return dataFrame


def resample(df, sample, how='mean', fill_method='ffill'):

    dataFrame = pandas.DataFrame(df, copy=True)
    if dataFrame is not None:
        if sample is not None:
            dataFrame = dataFrame.resample(sample, how=how, fill_method=fill_method)
    return dataFrame


def unpivot(df, metric, groupByColumn=None):
    dataFrame = pandas.DataFrame(df, copy=True)
    if dataFrame is not None:
        dataFrame['date'] = dataFrame.index.values
        dataFrame = pandas.melt(dataFrame, id_vars=['date'], value_name=metric, var_name=groupByColumn)
        dataFrame = dataFrame.set_index(dataFrame['date'])
    return dataFrame


def rolling_average(df, metric, rolling_average=None):

    dataFrame = pandas.DataFrame(df, copy=True)
    if dataFrame is not None:
        if rolling_average is not None:
            dataFrame[metric] = pandas.rolling_mean(dataFrame[metric], rolling_average)
    return dataFrame


def graphSeaborn(analyticConfig, df, metric, seriesConfig={}, graphConfig={}, notebook=False):

    dataFrame = pandas.DataFrame(df, copy=True)
    if dataFrame is not None:
        dataFrame = dataFrame.set_index(dataFrame['date'].apply(lambda x: datetime2graphdays(x)))

        print(dataFrame)


        seaborn.set(style="darkgrid", palette="Set2")
        fix, ax = seaborn.plt.subplots()
        seaborn.tsplot(dataFrame[metric], time=dataFrame.index, unit=None, interpolate=True, ax=ax, scalex=False, scaley=False, err_style="ci_bars", n_boot=1)
        ax.xaxis_date()
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y/%m/%d %H:%M:%S'))

        formatter = tkr.ScalarFormatter(useMathText=False)
        formatter.set_scientific(False)
        ax.yaxis.set_major_formatter(formatter)

        seaborn.plt.title(graphConfig["title"], fontsize='large')
        seaborn.plt.xlabel('date/time')
        seaborn.plt.ylabel(metric)

        fig.autofmt_xdate(rotation=45)

        now = datetime.now(tz=utc)
        format = "%Y%m%d%H%M%S"
        nowStr = time.strftime(format, now.timetuple())

        fileNameBase = analyticConfig.output_dir + "/" + metric
        if analyticConfig.sample is not None:
            fileNameBase += "-" + analyticConfig.sample
        if analyticConfig.how is not None:
            fileNameBase += "-" + analyticConfig.how
        fileName = fileNameBase + '-' + nowStr + '.png'

        fig.savefig(fileName)


def graph(analyticConfig, df, metric, seriesConfig={}, graphConfig={}, notebook=False):

    dataFrame = pandas.DataFrame(df, copy=True)
    if dataFrame is not None:
        dataFrame['colFromIndex'] = dataFrame.index
        if analyticConfig.groupByColumn is not None:
            # sort on date and the groupByColumn so that the series legend is sorted
            dataFrame = dataFrame.sort_values(['colFromIndex', analyticConfig.groupByColumn])
        else:
            dataFrame = dataFrame.sort_values(['colFromIndex'])

        title = None
        if analyticConfig.sample is None:
            title = metric
        else:
            title = metric + '\rsample ' + analyticConfig.sample

        dataFrame['date'] = dataFrame.index

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
                title=title
            )
        )

        layoutConfig.update(graphConfig)

        layout = go.Layout(layoutConfig)

        data = None
        for col in df[analyticConfig.groupByColumn].unique():
            tempDataFrame = dataFrame.loc[dataFrame[analyticConfig.groupByColumn] == col]

            addlColConfig = graphConfig.get(col, graphConfig.get('default', dict()))

            config = dict(
                name=col,
                x=tempDataFrame.index,
                y=tempDataFrame[metric],
                hoverinfo='x+y+text',
                text=col
            )

            config.update(addlColConfig)

            s = go.Scatter(config)

            if data is None:
                data = [s]
            else:
                data.append(s)

        now = datetime.now(tz=utc)
        format = "%Y%m%d%H%M%S"
        nowStr = time.strftime(format, now.timetuple())

        fileNameBase = analyticConfig.output_dir + "/" + metric
        if analyticConfig.sample is not None:
            fileNameBase += "-" + analyticConfig.sample
        if analyticConfig.how is not None:
            fileNameBase += "-" + analyticConfig.how

        fig = go.Figure(data=data, layout=layout)
        if notebook:
            py.iplot(fig, filename=fileNameBase+'-'+nowStr, show_link=False)
        else:
            py.plot(fig, output_type='file', filename=fileNameBase+'-'+nowStr+'.html', image='png', auto_open=False, show_link=False)


def datetime2graphdays(dt):

    return mdates.epoch2num((time.mktime(dt.timetuple()) / 86400) * mdates.SEC_PER_DAY)
