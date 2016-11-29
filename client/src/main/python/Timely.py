import time
import pytz
from datetime import datetime
from TimeDateRange import TimeDateRange
from TimeDateRange import TimeDateError
import json
import pandas
import getopt
import sys
import seaborn
import matplotlib.dates as mdates
import matplotlib.ticker as tkr
import random

from WebSocketClient import WebSocketClient
from tornado import ioloop


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

        if self.tags != None:
            t = self.tags.strip().split(',')
            for pair in t:
                k = pair.split('=')[0].strip()
                v = pair.split('=')[1].strip()
                m1 = dict(m1.items + dict({k : v}).items())

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
        self.timestamps = []
        self.debug = False
        self.dataFrame = None

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
        complete = bool(obj.get("complete"));
        if complete:
            self._on_connection_close()

        else:

            date = int(obj.get("timestamp")/1000);
            if not date in self.timestamps:

                self.timestamps += [date];
                dt = pandas.datetime.utcfromtimestamp(date)
                metricName = str(obj.get("metric"))
                metricValue = obj.get("value")

                df = pandas.DataFrame({'date': dt, metricName : [metricValue]}, columns=['date', metricName], index=[pandas.DatetimeIndex([dt])])

                tags = obj.get("tags")[0]
                for t in tags:
                    df[str(t)] = [tags[t]]

                if self.dataFrame is None:
                    self.dataFrame = df
                else:
                    self.dataFrame = self.dataFrame.append(df)

    def _on_connection_close(self):

        global client
        self.close()
        ioloop.IOLoop.current(instance=False).stop()
        print("Exiting.")

    def getDataFrame(self):

        return self.dataFrame;

    def print_debug(self):
        if self.debug:
            print(self.dataFrame)

    def graph(self):

        graph(self.dataFrame, self.metric, self.sample, self.how)




def resample(dataFrame, metric, sample, how):

    newDataFrame = dataFrame
    if not sample is None:
        newDataFrame = newDataFrame.resample(sample, how=how)
        newDataFrame['date'] = newDataFrame.index.values
    return newDataFrame


def graph(dataFrame, metric, sample):

    if dataFrame is not None:

        plotDataFrame = dataFrame.set_index(dataFrame['date'].apply(lambda x: datetime2graphdays(x)))

        seaborn.set(style="darkgrid", palette="Set2")

        # build the figure
        fig, ax = seaborn.plt.subplots()

        seaborn.tsplot(plotDataFrame[metric], time=plotDataFrame.index, unit=None, interpolate=True, ax=ax, scalex=False,
                       scaley=False, err_style="ci_bars", n_boot=1)

        # assign locator and formatter for the xaxis ticks.
        ax.xaxis_date();

        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y/%m/%d %H:%M:%S'))

        formatter = tkr.ScalarFormatter(useMathText=False)
        formatter.set_scientific(False)
        ax.yaxis.set_major_formatter(formatter)

        if sample is None:
            seaborn.plt.title(metric, fontsize='large')
        else:
            seaborn.plt.title(metric + ' sample ' + sample, fontsize='large')

        seaborn.plt.xlabel('date/time')
        seaborn.plt.ylabel(metric)

        # rotate the x-labels since they tend to be too long
        fig.autofmt_xdate(rotation=45)

        seaborn.plt.show()


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
    try:
        argv = sys.argv
        options, remainder = getopt.getopt(argv[1:], 'h', ['metric=', 'tags=', 'hours=', 'begin=', 'end=', 'sample=', 'how=', 'hostport='])
    except getopt.GetoptError:
        print 'TimelyMetric.py ---hostport=<host:port> --metric=<metric> --tags=<tag1=val1,tag2=val2> --hours=<hours>, --sample=<sample_period> --how=<mean|min|max>'
        sys.exit(2)
    for opt, arg in options:
        if opt == '-h':
            print 'TimelyMetric.py --hostport=<host:port> --metric=<metric> --tags=<tag1=val1,tag2=val2> --hours=<hours>, --sample=<sample_period> --how=<mean|min|max>'
            sys.exit()
        elif opt in ("--metric"):
            metric = arg
        elif opt in ("--hours"):
            hours = int(arg)
        elif opt in ("--begin"):
            beginStr = arg
        elif opt in ("--end"):
            endStr = arg
        elif opt in ("--sample"):
            sample = arg
        elif opt in ("--how"):
            how = arg
        elif opt in ("--hostport"):
            hostport = arg
    try:

        metric = TimelyMetric(hostport, metric, tags, beginStr, endStr, hours, sample).fetch()
        dataFrame = metric.getDataFrame()

        if (dataFrame != None):
            dataFrame = resample(dataFrame, metric, sample, how)
            print(dataFrame)
            graph(dataFrame, metric, sample)

    except TimeDateError as e:
        print(e.message)

if __name__ == '__main__':
    main()



