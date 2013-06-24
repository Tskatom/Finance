#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


import smtplib
import os
from bs4 import BeautifulSoup
import shelve
import argparse
import urllib2
from operator import itemgetter


SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587


def sent_mail(sender, pwd, recipient, subject, body):
    header = ["From:" + sender,
              "Subject:" + subject,
              "To:" + recipient,
              "MIME-Version: 1.0",
              "Content-Type: text/html"]
    header = "\r\n".join(header)

    session = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    session.ehlo()
    session.starttls()
    session.ehlo()
    session.login(sender, pwd)

    session.sendmail(sender, recipient, header + "\r\n\r\n" + body)
    session.quit()


def query_post(seen_it):
    url = "http://washingtondc.craigslist.org/search/cto?hasPic=1&maxAsk=7000&minAsk=4000&query=scion%20tc&sort=pricedsc&srchType=T&format=rss"
    new_posts = []
    soup = BeautifulSoup(urllib2.urlopen(url))
    #get all the posts
    posts = soup.find_all('item')
    for item in posts:
        #get title
        title = item.title.text.encode('ascii', 'ignore')
        #get link
        link = item.link.text.encode('ascii', 'ignore')
        #get description
        desc = item.description.text.encode('ascii', 'ignore')
        #get post time
        p_time = item.find("dc:date").text.encode('ascii', 'ignore')

        if link in seen_it:
            continue
        else:
            seen_it[link] = 1
        new_posts.append([title, p_time, desc, link])

    if len(new_posts) >= 2:
        new_posts.sort(key=itemgetter(1), reverse=True)
    return new_posts


def send_notif(seen_it, sender, pwd, recipient):
    #get latest posts
    posts = query_post(seen_it)
    if len(posts) > 0:
        #construct mail body
        body = "Dear Ying:<br><br> You may be interested in following posts filter by rules that <br> keywords: sicon tc <br>price: 4000-7000<br><br>"
        for p in posts:
            body += "title: " + p[0] + "<br>" + "postTime: " + p[1] + "<br>" + "description:" + "<br>" + p[2] + "<br>" + "link: " + p[3] + "<br><br>"

        body += "This email is automatically sent from Wei's Server, Please do not reply!<br>"
        subject = "Notification For car search From Craigslist"
        sent_mail(sender, pwd, recipient, subject, body)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--sender', default=os.environ['NOTIFIER'], help="sender")
    ap.add_argument('--pwd', default=os.environ['NOTIFIER_PWD'], help="password for sender")
    ap.add_argument('--reci', default='niying1206@gmail.com', help="recipient")
    ap.add_argument('--db', default='/home/vic/work/Finance/Util/posts.db', help="db file")
    arg = ap.parse_args()

    seen_it = shelve.open(arg.db)
    send_notif(seen_it, arg.sender, arg.pwd, arg.reci)


if __name__ == "__main__":
    main()
