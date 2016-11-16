import time
import json
import pandas
import getopt
import sys
import seaborn
import matplotlib.dates as mdates
import matplotlib.ticker as tkr

from WebSocketClient import WebSocketClient
from tornado import ioloop


class TimelyWebSocketClient(WebSocketClient):

    def __init__(self, hostport, subscriptionId, metric, startTime, endTime, connect_timeout=WebSocketClient.DEFAULT_CONNECT_TIMEOUT,
                 request_timeout=WebSocketClient.DEFAULT_REQUEST_TIMEOUT):

        self.metric = metric
        self.startTime = startTime
        self.endTime = endTime
        self.subscriptionId = subscriptionId
        WebSocketClient.__init__(self, hostport, connect_timeout, request_timeout)

    def _on_message(self, msg):
        # self._on_message_callback(self, msg)

        obj = json.loads(msg)
        print(str(obj.get("timestamp")) + " -- " + str(self.endTime))

        timestamp = int(obj.get("timestamp"));

        if (timestamp >= self.endTime or (self.endTime - timestamp) < 60000):
            print("exiting")
            self._on_connection_close()

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
        self.send(m1)

    def _on_connection_close(self):
        print('Connection closed!')
        exit()

    def _on_connection_error(self, exception):
        print('Connection error: %s', exception)




class TimelyMetric(TimelyWebSocketClient):

    client = None
    endtime = None

    def __init__(self, hostport, subscriptionId, metric, startTime, endTime, sample):
        self.series = pandas.Series()
        self.metric = metric
        self.sample = sample
        self.timestamps = []
        self.debug = False
        self.dataFrame = None
        TimelyWebSocketClient.__init__(self, hostport, subscriptionId, metric, startTime, endTime)

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
                dt = pandas.datetime.fromtimestamp(date)
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

        if self.dataFrame is not None:

            self.print_debug()

            if not self.sample is None:
                try:
                    self.dataFrame = self.dataFrame.resample(self.sample).mean()
                    self.dataFrame['date'] = self.dataFrame.index.values
                except IOError as e:
                    print "I/O error({0}): {1}".format(e.errno, e.strerror)
                except:
                    print "Unexpected error:", sys.exc_info()[0]
                    raise

            self.print_debug()

            plotDataFrame = self.dataFrame.set_index(self.dataFrame['date'].apply(lambda x: datetime2graphdays(x)))

            self.print_debug()

            seaborn.set(style="darkgrid", palette="Set2")

            # build the figure
            fig, ax = seaborn.plt.subplots()

            seaborn.tsplot(plotDataFrame[self.metric], time=plotDataFrame.index, unit=None, interpolate=True, ax=ax, scalex=False,
                           scaley=False, err_style="ci_bars", n_boot=1)

            # assign locator and formatter for the xaxis ticks.
            ax.xaxis_date();

            ax.xaxis.set_major_locator(mdates.AutoDateLocator())
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y/%m/%d %H:%M:%S'))

            formatter = tkr.ScalarFormatter(useMathText=False)
            formatter.set_scientific(False)
            ax.yaxis.set_major_formatter(formatter)

            if self.sample is None:
                seaborn.plt.title(self.metric, fontsize='large')
            else:
                seaborn.plt.title(self.metric + ' sample ' + self.sample, fontsize='large')

            seaborn.plt.xlabel('date/time')
            seaborn.plt.ylabel(self.metric)

            # rotate the x-labels since they tend to be too long
            fig.autofmt_xdate(rotation=45)

            seaborn.plt.show()


def datetime2graphdays(dt):

    return mdates.epoch2num((time.mktime(dt.timetuple()) / 86400) * mdates.SEC_PER_DAY)


def main():

    hours = 1
    metric = "timely.metrics.received"
    sample = None
    try:
        argv = sys.argv
        options, remainder = getopt.getopt(argv[1:], 'h', ['metric=', 'time=', 'sample=', 'hostport='])
    except getopt.GetoptError:
        print 'TimelyMetric.py ---hostport=<host:port> --metric=<metric> --time=<hours>, --sample=<sample_period>'
        sys.exit(2)
    for opt, arg in options:
        if opt == '-h':
            print 'TimelyMetric.py --hostport=<host:port> --metric=<metric> --time=<hours>, --sample=<sample_period>'
            sys.exit()
        elif opt in ("--metric"):
            metric = arg
        elif opt in ("--time"):
            hours = int(arg)
        elif opt in ("--sample"):
            sample = arg
        elif opt in ("--hostport"):
            hostport = arg

    # print("Metric " + metric)
    # print("Hours " + str(hours))
    # if not sample is None:
    #     print("Sample " + sample)

    now = int(time.time() * 1000)
    rangeInSec = hours * 3600

    startTime = int(now - (rangeInSec * 1000))
    endTime = now

    metric = TimelyMetric(hostport, "12345", metric, startTime, endTime, sample).fetch()
    print(metric.getDataFrame())
    metric.graph()


if __name__ == '__main__':
    main()



