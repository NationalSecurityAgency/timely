import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import TimelyMetric
import syslog
import DataOperations

syslog.openlog("TimelyNotifications")


class TimelyAlert():

    def __init__(self, timelyMetric, dataFrame, message, seriesConfig, analyticConfig, notebook):
        self.timelyMetric = timelyMetric
        self.dataFrame = dataFrame
        self.seriesConfig = seriesConfig
        self.analyticConfig = analyticConfig
        self.notebook = notebook
        self.message = message

    def email(self, send_from, send_to, subject, text, files=None, server="127.0.0.1"):
        assert isinstance(send_to, list)

        msg = MIMEMultipart()
        msg['From'] = send_from
        msg['To'] = COMMASPACE.join(send_to)
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = subject

        msg.attach(MIMEText(text))

        for f in files or []:
            with open(f, "rb") as fil:
                part = MIMEApplication(
                    fil.read(),
                    Name=basename(f)
                )
                part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
                msg.attach(part)


        smtp = smtplib.SMTP(server)
        smtp.sendmail(send_from, send_to, msg.as_string())
        smtp.close()

    def log(self, message):

        syslog.systlog(syslog.LOG_ALERT, message)

    def graph(self, type="png"):

        graphConfig = {}
        graphConfig["title"] = DataOperations.getTitle(self.timelyMetric.metric, self.analyticConfig)
        return TimelyMetric.graph(self.analyticConfig, self.dataFrame, self.timelyMetric.metric, seriesConfig=self.seriesConfig, graphConfig=graphConfig, notebook=self.notebook, type=type)

    def getDataFrame(self):
        return self.dataFrame

    def getAnalyticConfig(self):
        return self.analyticConfig

    def getSeriesConfig(self):
        return self.seriesConfig

    def getTimelyMetric(self):
        return self.timelyMetric

    def getMessage(self):
        return self.message