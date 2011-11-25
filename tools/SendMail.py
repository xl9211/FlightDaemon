import traceback
import LogUtil
import smtplib
from email.mime.text import MIMEText


class SendMail:
    def __init__(self, to_list):
        self.logger = LogUtil.Logging.getLogger()
        self.me = "TourBoxMonitor<monitor@tourbox.me>"
        self.to_list = to_list
        

    def send(self, subject, content):
        msg = MIMEText(content)
        msg['Subject'] = subject
        msg['From'] = self.me
        msg['To'] = self.to_list
        try:
            s = smtplib.SMTP("smtp.exmail.qq.com")
            s.login("monitor@tourbox.me", "tourbox.me")
            s.sendmail(self.me, self.to_list.split(','), msg.as_string())
            s.close()
            
            return True
        except Exception, e: #@UnusedVariable
            msg = traceback.format_exc()
            self.logger.error(msg)
            return False
        

if __name__ == '__main__':
    mail = SendMail('xulin@tourbox.me')
    print "Start"
    if mail.send("Build Error", "content"):
        print 'Succ'
    else:
        print 'Error'
        
        
