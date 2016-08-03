#!/usr/bin/env python
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


COMMASPACE = ', '

# me == my email address
# you == recipient's email address
me = "admin@hadoop"
#you = "ravi.shekhar@bseindia.com"
you=['ravi.shekhar@bseindia.com','k.mohanan@bseindia.com','bhalchandra.pawar@bseindia.com','dulal.mali@bseindia.com','sachin.bhor@bseindia.com','amit.ranade@bseindia.com']
#you = ['santhiya.thevar@bseindia.com']

# Create message container - the correct MIME type is multipart/alternative.
msg = MIMEMultipart('alternative')
msg['Subject'] = "Alert | DFS Usage"
msg['From'] = me
msg['To'] = COMMASPACE.join(you)

# Create the body of the message (a plain-text and an HTML version).


def send_alert(html):
    part2 = MIMEText(html, 'html')
    msg.attach(part2)
    s = smtplib.SMTP('192.168.246.248')
    print "Sending Email Alert"
    s.sendmail(me, you, msg.as_string())
    s.quit()

