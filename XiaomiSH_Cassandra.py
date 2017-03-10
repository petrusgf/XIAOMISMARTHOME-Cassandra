#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 24 02:28:25 2016

@author: psilva
"""

import socket
import binascii
import struct
import json
import datetime
from cassandra.cluster import Cluster


cluster = Cluster(['192.168.1.81'])
session = cluster.connect()
session.set_keyspace('xsh')

def gateway(x) :
   y = json.loads(x)
   
   session.execute(""" INSERT INTO data (sid, datestamp, event_type, sensor)
    VALUES (%s, %s, %s, %s)""", 
   (y['sid'], datetime.datetime.now(), y['cmd'],"Gateway"))
   
   print (y,"gateway") 

def door(x) :
   y = json.loads(x)
   w = json.loads(y['data'])
   session.execute(""" INSERT INTO data (sid, datestamp, event_type, sensor)
    VALUES (%s, %s, %s, %s)""", 
   (y['sid'], datetime.datetime.now(), w['status'],"Door"))
   
   print (y,"door")
   
def button(x) :
   y = json.loads(x)
   print (y)
   if ('status') in y:
       w = json.loads(y['data'])
       session.execute(""" INSERT INTO data (sid, datestamp, event_type, sensor)
       VALUES (%s, %s, %s, %s)""", 
       (y['sid'], datetime.datetime.now(), w['status'],"Button"))       
   else:
       session.execute(""" INSERT INTO data (sid, datestamp, event_type, sensor)
       VALUES (%s, %s, %s, %s)""", 
       (y['sid'], datetime.datetime.now(), 'heartbeat',"Button"))   

   print (y,"button") 

def motion(x) :
   y = json.loads(x)
   w = json.loads(y['data'])
   if ('no_motion') in w:
       session.execute(""" INSERT INTO data (sid, datestamp, event_type, sensor)
       VALUES (%s, %s, %s, %s)""", 
       (y['sid'], datetime.datetime.now(), "no_motion","Motion"))
   else:
       session.execute(""" INSERT INTO data (sid, datestamp, event_type, sensor)
       VALUES (%s, %s, %s, %s)""", 
       (y['sid'], datetime.datetime.now(), w['status'],"Motion"))
       print (y)
   print (y,"motion")
   
   
UDP_IP = "192.168.1.72"
UDP_PORT_FROM = 54322
UDP_PORT = 54321
 
MULTICAST_PORT = 9898
SERVER_PORT = 4321
 
MULTICAST_ADDRESS = '224.0.0.50'
SOCKET_BUFSIZE = 1024
MESSAGE = binascii.unhexlify('21310020ffffffffffffffffffffffffffffffffffffffffffffffffffffffff') 
 
sock = socket.socket(socket.AF_INET, # Internet
                     socket.SOCK_DGRAM) # UDP
 
sock.bind(("0.0.0.0", MULTICAST_PORT))
 
mreq = struct.pack("=4sl", socket.inet_aton(MULTICAST_ADDRESS), socket.INADDR_ANY)
sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 32)
sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUFSIZE)
sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
 
while True:
    data, addr = sock.recvfrom(SOCKET_BUFSIZE) # buffer size is 1024 bytes
    data1=data.decode("utf-8")
    if 'f0b429b3ce83' in (data1):
        gateway(data1)
    elif '158d00010ef090' in (data1):
        motion(data1)
    elif '158d000111a5a2' in (data1):
        door(data1)
    elif '158d0001157a0b' in (data1):
        button(data1)
    #print (data1)    
    
                