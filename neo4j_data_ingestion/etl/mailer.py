import os
import smtplib
from logger import Logger
from parse_config import ParseConfig
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart

__author__ = "Jagjeet Goraya"
__version__ = "0.1"
__status__ = "development"
__updatedDate__ = "2018-07-19"

log_file = 'mailer.py'
sep = os.sep

class Mailer(Logger):
    """."""

    def __init__(self):
        """."""

        # Logger.__init__(self)
        self.configfile = sep.join(os.path.abspath(os.path.join(os.path.dirname(__file__))).split(sep)[:-1])+sep+"resources"+sep+"config.cfg"
        self.parse_dict = ParseConfig.get_config_dict(self.configfile)
        self.path_section = self.parse_dict.get('emailCredentials')
        self.host_name = self.path_section.get('host_name')
        self.port_no = self.path_section.get('port_no')
        self.username = self.path_section.get('username')
        self.password = self.path_section.get('password')
        self.to_emails = self.path_section.get('to_emails')
        self.cc_emails = self.path_section.get('cc_emails')
        self.receiver_name = self.path_section.get('receiver_name')

    def create_content(self, to_addr, cc_addrs, subject, html_content):
        """."""

        html_content = html_content.format(receiver=self.receiver_name)        
        content = MIMEText(html_content, 'html')
        
        msg = MIMEMultipart()
        msg['To'] = to_addr
        msg['Cc']= cc_addrs
        msg['From'] = self.username
        msg['Subject'] = subject
        msg.attach(content)
        
        all_addrs = cc_addrs.split(',')+[to_addr]
        
        return msg

    def send_email(self, all_addrs, msg):
        """."""

        status = False
        
        try:
            smtp_connection = smtplib.SMTP(self.host_name,self.port_no)
            smtp_connection.ehlo()
            smtp_connection.starttls()
            smtp_connection.ehlo()
            smtp_connection.login(self.username , self.password)
            smtp_connection.sendmail(self.username, all_addrs, msg.as_string())
            status = True

        except Exception as e:
            self.logg(debug_msg = '', 
                info_msg = 'Function = send_email()', 
                warning_msg = 'Unable to send email.', 
                error_msg = 'Module = '+log_file, 
                critical_msg = str(e))

        else:
            return status

    def compose_mail(self, subject, html_content):
        """."""

        try:
            all_addrs = self.cc_emails.split(',')+[self.to_emails]
            msg = self.create_content(self.to_emails, self.cc_emails, subject, html_content)
            status = self.send_email(all_addrs, msg)

        except Exception as e:
            self.logg(debug_msg = '', 
                info_msg = 'Function = compose_mail()', 
                warning_msg = 'Unable to compose email.', 
                error_msg = 'Module = '+log_file, 
                critical_msg = str(e))


if __name__ == '__main__':
    """."""

    html_content = """\
            <body style="font-family:arial;">
            Hi <span style="color:#1F497D;">{receiver}</span>,<br><br>
                This is a test mail.<br><br>
                <br><br>Regards,
                <br><span style="color:#0073A5"></span>
                <br><span style="font-size:10.0pt; color:#0073A5">BA Mail tester.<br></span>
        </html>
        """\

    mailer = Mailer()
    mailer.compose_mail('Test mail',html_content)

