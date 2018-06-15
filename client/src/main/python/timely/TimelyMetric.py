from TimeDateRange import TimeDateRange
from TimeDateRange import TimeDateError
from TimeDateRange import UTC
from DataOperations import *
import json
import pandas
import getopt
import sys
import random

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
        WebSocketClient.__init__(self, hostport, '/websocket', {'metric' : metric}, connect_timeout, request_timeout)

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

    def __init__(self, hostport, metric, tags, beginStr, endStr, period, sample):
        self.series = pandas.Series()
        self.metric = metric
        self.tags = tags
        self.sample = sample
        self.debug = False
        self.dataFrame = None
        self.data = []
        self.timeDateRange = TimeDateRange(beginStr, endStr, period)

        TimelyWebSocketClient.__init__(self, hostport, metric, tags, self.timeDateRange.getBeginMs(), self.timeDateRange.getEndMs())

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

        responsesObj = json.loads(msg)
        responses = responsesObj.get("responses")

        for obj in responses:
            complete = bool(obj.get("complete"))
            if complete:
                self._on_connection_close()
                return
            else:
                date = int(obj.get("timestamp")/1000)
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
        if len(self.dataFrame) > 0:
            self.dataFrame['date'] = pandas.DatetimeIndex(self.dataFrame['date'])
            self.dataFrame = self.dataFrame.set_index('date')
            j = pandas.to_timedelta(self.dataFrame.groupby(self.dataFrame.index).cumcount(), unit='us').values
            self.dataFrame.index = self.dataFrame.index + j

        self.close()
        ioloop.IOLoop.current(instance=False).stop()
        print("Exiting.")

    def getDataFrame(self):

        return self.dataFrame;

    def setDataFrame(self, dataFrame):

        self.dataFrame = dataFrame

    def print_debug(self):
        if self.debug:
            print(self.dataFrame)

    def graph(self, groupByColumn=None):
        return graph(self.dataFrame, self, sample=self.sample, how=self.how, groupByColumn=groupByColumn)



def main():

    period = None
    beginStr = None
    endStr = None

    metric = "timely.metrics.received"
    sample = None
    how = None
    tags = None
    groupByColumn = None
    try:
        argv = sys.argv
        options, remainder = getopt.getopt(argv[1:], 'h', ['metric=', 'tags=', 'period=', 'groupByColumn=', 'begin=', 'end=', 'sample=', 'how=', 'hostport='])
    except getopt.GetoptError:
        print('TimelyMetric.py ---hostport=<host:port> --metric=<metric> --tags=<tag1=val1,tag2=val2> --period=<period>, --groupByColumn=<groupByColumn>, --sample=<sample_period> --how=<mean|min|max>')
        sys.exit(2)
    for opt, arg in options:
        if opt == '-h':
            print('TimelyMetric.py --hostport=<host:port> --metric=<metric> --tags=<tag1=val1,tag2=val2> --period=<period>, --groupByColumn=<groupByColumn>, --sample=<sample_period> --how=<mean|min|max>')
            sys.exit()
        elif opt in ("--metric"):
            metric = arg
        elif opt in ("--period"):
            period = arg
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

        timelyMetric = TimelyMetric(hostport, metric, tags, beginStr, endStr, period, sample).fetch()
        dataFrame = timelyMetric.getDataFrame()

        if dataFrame is not None:
            dataFrame = resample(dataFrame, metric, sample, how)
            dataFrame = pivot(dataFrame, metric, groupByColumn=groupByColumn)
            graph(dataFrame, timelyMetric, sample=sample, groupByColumn=groupByColumn, graphConfig={}, notebook=False)

    except TimeDateError as e:
        print(e.message)

if __name__ == '__main__':
    main()



