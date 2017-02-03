import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from TimelyMetric import graphSeaborn
import syslog

syslog.openlog("TimelyNotifications")

#def __init__(self, file, send_to, subject, text, analyticConfig, notebook):


class TimelyAlert():

    def __init__(self, timelyMetric, dataFrame, seriesConfig, analyticConfig, notebook):
        self.timelyMetric = timelyMetric
        self.dataFrame = dataFrame
        self.seriesConfig = seriesConfig
        self.analyticConfig = analyticConfig
        self.notebook = notebook

    def createFilename(self, metric, config):

        metricTitle = metric
        if config.sample is not None:
            sampleTitle = config.sample + "-" + config.how

        filename = metricTitle
        if sampleTitle != "":
            filename += "-" + sampleTitle

        return filename

    def createTitle(self, metric, config):

        if config.orCondition:
            condition = "OR"
        else:
            condition = "AND"

        metricTitle = metric
        sampleTitle = ""
        alertTitle = ""
        if config.sample is not None:
            sampleTitle = config.sample + " " + config.how + " samples"

        if config.min_threshold is not None:
            if alertTitle != "":
                alertTitle += " " + condition + " "
            alertTitle += "(y < " + str(config.min_threshold) + ")"

        if config.max_threshold is not None:
            if alertTitle != "":
                alertTitle += " " + condition + " "
            alertTitle += "(y > " + str(config.max_threshold) + ")"

        if config.alert_percentage is not None:
            if alertTitle != "":
                alertTitle += " " + condition + " "
            if config.alert_percentage > 0:
                alertTitle += "(y > " + str(config.alert_percentage) + "% above " + str(config.rolling_average) + " sample average)"
            else:
                alertTitle += "(y < " + str(-config.alert_percentage) + "% below " + str(config.rolling_average) + " sample average)"

        title = metricTitle
        if sampleTitle != "":
            title += "\n" + sampleTitle

        if alertTitle != "":
            title += "\n" + alertTitle

        return title

    def notify(self, send_from, send_to, server="127.0.0.1"):

        self.graph()

        return

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

    def graph(self):

        graphConfig = {}
        graphConfig["title"] = self.createTitle(self.timelyMetric.metric, self.analyticConfig)
        # graph(self.analyticConfig, self.dataFrame, self.timelyMetric.metric, seriesConfig=self.seriesConfig, graphConfig=graphConfig, notebook=self.notebook)

        graphSeaborn(self.analyticConfig, self.dataFrame, self.timelyMetric.metric, seriesConfig=self.seriesConfig, graphConfig=graphConfig, notebook=self.notebook)



def main():
    #syslog.syslog(syslog.LOG_ALERT, "Example message")
    a = TimelyAlert(None, None, None, None, False)

    a.email_alert("wwoley@localhost", ["wwoley@localhost"], "Test1", "This is a sendmail test")



if __name__ == '__main__':
    main()