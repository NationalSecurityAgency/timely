import time
from datetime import datetime
from TimeDateRange import TimeDateRange
from TimeDateRange import TimeDateError
from TimeDateRange import UTC
import json
import pandas
import getopt
import sys
import matplotlib.dates as mdates
import random
import plotly.graph_objs as go
import plotly.offline as py

from WebSocketClient import WebSocketClient
from tornado import ioloop

utc = UTC()

class TimelyWebSocketClient(WebSocketClient):

    def __init__(self, hostport, metric, tags, startTime, endTime, connect_timeout=WebSocketClient.DEFAULT_CONNECT_TIMEOUT,
                 request_timeout=WebSocketClient.DEFAULT_REQUEST_TIMEOUT):

        self.metric = metric
        self.tags = tags
        self.startTime = startTime
        self.endTime = endTime
        self.subscriptionId = random.randint(1000000000, 9999999999)
        WebSocketClient.__init__(self, hostport, connect_timeout, request_timeout)

    def _on_message(self, msg):
        # implement in subclass
        pass

    def _on_connection_success(self):
        print('Connected.')

        create = {
            "operation": "create",
            "subscriptionId": self.subscriptionId,
        }
        self.send(create)
        self.send(create)

        m1 = {
            "operation": "add",
            "subscriptionId": self.subscriptionId,
            "metric": self.metric,
            "startTime": self.startTime,
            "endTime" : self.endTime
        }

        if self.tags is not None:
            t = self.tags.strip().split(',')
            tagDict = {}
            for pair in t:
                k = pair.split('=')[0].strip()
                v = pair.split('=')[1].strip()
                tagDict = dict(tagDict.items() + dict({k : v}).items())
            m1["tags"] = tagDict

        self.send(m1)

    def _on_connection_close(self):
        print('Connection closed!')
        exit()

    def _on_connection_error(self, exception):
        print('Connection error: %s', exception)




class TimelyMetric(TimelyWebSocketClient):

    client = None
    endtime = None

    def __init__(self, hostport, metric, tags, beginStr, endStr, hours, sample):
        self.series = pandas.Series()
        self.metric = metric
        self.tags = tags
        self.sample = sample
        self.debug = False
        self.dataFrame = None
        self.data = []

        timeDateRange = TimeDateRange(beginStr, endStr, hours)

        TimelyWebSocketClient.__init__(self, hostport, metric, tags, timeDateRange.getBeginMs(), timeDateRange.getEndMs())

    def debugOn(self):
        self.debug = True

    def debugOff(self):
        self.debug = False

    def fetch(self):

        ioloop.IOLoop().make_current()
        self.connect(ioloop.IOLoop.current(instance=False))
        ioloop.IOLoop.current(instance=False).start()
        return self


    def _on_message(self, msg):

        global df
        global endtime

        obj = json.loads(msg)
        responses = responsesObj.get("responses")

        for obj in responses:
            complete = bool(obj.get("complete"));
            if complete:
                self._on_connection_close()
                return
            else:
                date = int(obj.get("timestamp")/1000);
                dt = pandas.datetime.utcfromtimestamp(date)
                metricName = str(obj.get("metric"))
                metricValue = obj.get("value")

                newData = {}
                newData["date"] = dt
                newData[metricName] = metricValue

                tags = obj.get("tags")
                for d in tags:
                    for k,v in d.items():
                        newData[k] = v

                self.data.append(newData)


    def _on_connection_close(self):
        global client

        self.dataFrame = pandas.DataFrame(self.data)
        self.dataFrame = self.dataFrame.set_index(self.dataFrame['date'].apply(lambda x: pandas.DatetimeIndex([x])))


        self.close()
        ioloop.IOLoop.current(instance=False).stop()
        print("Exiting.")

    def getDataFrame(self):

        return self.dataFrame;

    def print_debug(self):
        if self.debug:
            print(self.dataFrame)

    def graph(self, groupByColumn=None):
        graph(self.dataFrame, self.metric, sample=self.sample, how=self.how, groupByColumn=groupByColumn)

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


def graph(df, metric, sample=None, groupByColumn=None, seriesConfig={}, graphConfig={}, notebook=False):

    dataFrame = pandas.DataFrame(df, copy=True)
    if dataFrame is not None:
        dataFrame['colFromIndex'] = dataFrame.index
        if groupByColumn is not None:
            # sort on date and the groupByColumn so that the series legend is sorted
            dataFrame = dataFrame.sort_values(['colFromIndex', groupByColumn])
        else:
            dataFrame = dataFrame.sort_values(['colFromIndex'])

        title = None
        if sample is None:
            title = metric
        else:
            title = metric + '\rsample ' + sample

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
        for col in df[groupByColumn].unique():
            tempDataFrame = dataFrame.loc[dataFrame[groupByColumn] == col]

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

        fig = go.Figure(data=data, layout=layout)
        if notebook:
            py.iplot(fig, filename=title+'-'+nowStr, show_link=False)
        else:
            py.plot(fig, output_type='file', filename=title+'-'+nowStr+'.html', auto_open=False, show_link=False)


def datetime2graphdays(dt):

    return mdates.epoch2num((time.mktime(dt.timetuple()) / 86400) * mdates.SEC_PER_DAY)


def main():

    hours = None
    beginStr = None
    endStr = None

    metric = "timely.metrics.received"
    sample = None
    how = None
    tags = None
    groupByColumn = None
    try:
        argv = sys.argv
        options, remainder = getopt.getopt(argv[1:], 'h', ['metric=', 'tags=', 'hours=', 'groupByColumn=', 'begin=', 'end=', 'sample=', 'how=', 'hostport='])
    except getopt.GetoptError:
        print 'TimelyMetric.py ---hostport=<host:port> --metric=<metric> --tags=<tag1=val1,tag2=val2> --hours=<hours>, --groupByColumn=<groupByColumn>, --sample=<sample_period> --how=<mean|min|max>'
        sys.exit(2)
    for opt, arg in options:
        if opt == '-h':
            print 'TimelyMetric.py --hostport=<host:port> --metric=<metric> --tags=<tag1=val1,tag2=val2> --hours=<hours>, --groupByColumn=<groupByColumn>, --sample=<sample_period> --how=<mean|min|max>'
            sys.exit()
        elif opt in ("--metric"):
            metric = arg
        elif opt in ("--hours"):
            hours = int(arg)
        elif opt in ("--begin"):
            beginStr = arg
        elif opt in ("--end"):
            endStr = arg
        elif opt in ("--groupByColumn"):
            groupByColumn = arg
        elif opt in ("--sample"):
            sample = arg
        elif opt in ("--how"):
            how = arg
        elif opt in ("--hostport"):
            hostport = arg
    try:

        timelyMetric = TimelyMetric(hostport, metric, tags, beginStr, endStr, hours, sample).fetch()
        dataFrame = timelyMetric.getDataFrame()

        if dataFrame is not None:
            dataFrame = resample(dataFrame, metric, sample, how)
            dataFrame = pivot(dataFrame, metric, groupByColumn=groupByColumn)
            graph(dataFrame, metric, sample=sample, groupByColumn=groupByColumn, graphConfig={}, notebook=False)

    except TimeDateError as e:
        print(e.message)

if __name__ == '__main__':
    main()



