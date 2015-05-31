# it is neccessary to install requests and MySQLdb

import requests
import json
from optparse import OptionParser
import xml.etree.ElementTree as ET
import time
import socket
import sys
import MySQLdb
import os

# I'm using optparse module to parse input variables
# creating string that will popup when -h option is specified
# or when is not enough arguments
usage = "How to use: %prog PATH_TO_FILE.XML"
# creating OptionParser instance with usage as parameter
parser = OptionParser(usage)
# calling parse_args() method to fill args array
(options, args) = parser.parse_args()
# Checking for number of arguments
if len(args) != 1:
    parser.error("incorrect number of arguments")
# Checking if path to xml is correct
if not os.path.exists(args[0]):
    print "Invalid path to xml file"
    sys.exit()
# using ElementTree module to parse xml
try:
    tree = ET.parse(args[0])
except Exception:
    print "It seems that it's not an xml file"
    sys.exit()

root = tree.getroot()

# unix-socket /var/run/mysqld/mysqld.sock
#     username    u1
#     password    984hfwGTw2Fqs22q
#     database    sender
#     table       results

server_address = '/var/run/mysqld/mysqld.sock'
# Make sure the socket exists
if not os.path.exists(server_address):
    print "there is no socket, please start mysql server"
    sys.exit()

print "Trying to connect to database"
try:
    db = MySQLdb.connect(host="localhost",
         user="u1",
         passwd="984hfwGTw2Fqs22q",
         db="sender",
         charset='utf8')
except Exception:
    print "can't connect, something gone wrong - check mysql db parameters"
    sys.exit()
# To perform a query, you first need a cursor, and then you can execute queries on it:
c = db.cursor()
# Checking if table results exists
c.execute("SHOW TABLES FROM sender LIKE 'results';")
data = c.fetchone()
if data == None:
    print "There is no table results inside sender db, will create one"
    c.execute("""CREATE TABLE results (
    `id`                INT(11) UNSIGNED    NOT NULL,
    `ts`                TIMESTAMP           NULL        DEFAULT NULL,
    `delivery_status`   TINYINT(1) UNSIGNED NULL        DEFAULT NULL,
    PRIMARY KEY (`id`)
    ) ENGINE=InnoDB;""")
    # Checking if table was created
    c.execute("SHOW TABLES FROM sender LIKE 'results';")
    data = c.fetchone()
    if data != None:
        print "Successfully created table results"
    else:
        print "Something went wrong, table hasn't been created"
        sys.exit()

# iterating through all xml, parsing fields, skipping dublicates fetching db
# I will use this list of id's to check for dublicates:
ids = []
for email in root.findall('email'):
    to = email.find('to').text
    subject = email.find('subject').text
    _id = email.get('id')
    if _id in ids:
        print "Dubplicate id=%s in xml while pasring, skipping it" % _id
        continue
    ids.append(_id)
    # preparing data for request
    message = json.dumps({"to":to,"subject":subject})
    payload = { "id": _id,
                "message": message
              }
    url = 'http://test.webjet.pro/api.php'
    # Server's response:
    r = requests.post(url, data=payload)
    # printing logs:
    print "Response of server (email id %s) %s with code %s" % (_id, r, r.text)
    if "200" in str(r):
        if int(r.text) == 1:
            print "Email with id %s to %s with subject %s has been delivered"\
                  %(_id, to, subject)
        else:
            print "Email with id %s to %s with subject %s hasn't been delivered"\
                  %(_id, to, subject)
    elif "503" in str(r):
        print "Email with id %s to %s with subject %s hasn't been delivered: "\
              "server is busy, will try again" % (_id, to, subject)
        time.sleep(2)
        r = requests.post(url, data=payload)
        print "Response to email with id %s is %s with code %s after another try"\
              % (_id, r, r.text)
    # And now inputting data in table:
    cur_time = time.strftime("%Y-%m-%d %H:%M:%S")
    # try, cause it is possible that table is full with this entries
    try:
        with db:
            c = db.cursor()
            c.execute("""INSERT INTO results values
            (%s, %s, %s);""", (_id, cur_time, r.text))
    except Exception:
        print "Trying to add record in db, but there is dublicate"
        pass

db.close()
