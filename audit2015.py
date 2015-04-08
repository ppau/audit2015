import sys
import pymongo
import logging
import json
import re

from tornado.template import Template
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate
from subprocess import Popen, PIPE

logging.getLogger().setLevel(logging.INFO)

def sendmail(mail):
    p = Popen(["sendmail", "-t", "-i"], stdin=PIPE)
    return p.communicate(mail.as_string().encode())

def create_email(frm=None, to=None, cc=None, bcc=None, subject=None, text=None, html=None, date=None, reply_to=None, attachments=[]):
    msg = MIMEMultipart('alternative')
    if frm:
        msg["From"] = frm
    if to:
        msg["To"] = to
    if cc:
        msg["Cc"] = cc
    if bcc:
        msg["Bcc"] = bcc
    if subject:
        msg["Subject"] = subject
    if text:
        textmsg = MIMEText(text, 'plain', _charset='utf-8')
        msg.attach(textmsg)
    if html:
        htmlmsg = MIMEText(html, 'html', _charset='utf-8')
        msg.attach(htmlmsg)
    if date is None:
        msg["Date"] = formatdate(localtime=True)
    else:
        msg["Date"] = formatdate(date, localtime=True)
    if reply_to:
        msg["Reply-To"] = reply_to
    for attachment in attachments:
        msg.attach(attachment)
    return msg

def mask(number):
    x = re.sub(r'[0-9]', '*', number[:-3])
    return x + number[-3:]

if len(sys.argv) < 3:
    print("Usage: %s <conf> <txt-template> [html-template]" % sys.argv[0])
    sys.exit()

with open(sys.argv[1]) as f:
    conf = json.load(f)

has_html = len(sys.argv) > 3

coll = pymongo.Connection().ppau.members

query = {
    "details.membership_level": {
        "$in": ["full", "founder"]
    },
    "details.last_audit_confirmation": {
        "$exists": False
    }
}

targets = coll.find(query)
count = targets.count()

with open(sys.argv[2]) as f:
    txt_tmpl = Template(f.read())
if has_html:
    with open(sys.argv[3]) as f:
        html_tmpl = Template(f.read())
else:
    html_tmpl = None

print("Email will be sent to %s recipients." % count)
if input("Are you ready? [y/N]> ") != 'y':
    sys.exit()

for n, member in enumerate(targets):
    details = member.get('details', None)
    details['member_id'] = member['_id'].hex

    if details is None:
        logging.warning("[%s/%s] Details none for record:" % (n+1, count))
        logging.warning("%r" % member)
        continue

    email = details.get('email', None)
    if email is None:
        logging.warning("[%s/%s] Missing email for record:" % (n+1, count))
        logging.warning("%r" % member)
        continue

    try:
        text = txt_tmpl.generate(mask=mask, tmpl_subject=conf['subject'], **details)
        if html_tmpl:
            html = html_tmpl.generate(mask=mask, tmpl_subject=conf['subject'], **details)
        else:
            html = None

        logging.debug(text)

        msg = create_email(frm=conf['from'],
                           to=email,
                           subject=conf['subject'],
                           text=text,
                           html=html)

    except NameError as e:
        logging.error("[%s/%s] Failed to format template for: %s" % (n+1, count, email))
        logging.error(e)
        continue


    try:
        sendmail(msg)
        logging.info("[%s/%s] %s" % (n+1, count, email))
    except Exception as e:
        logging.error("[%s/%s] %s - error of some kind" % (n+1, count, email))
        logging.error(e)
        continue

